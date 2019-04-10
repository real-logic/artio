/*
 * Copyright 2015-2018 Real Logic Ltd, Adaptive Financial Consulting Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.co.real_logic.artio.system_tests;

import io.aeron.archive.ArchivingMediaDriver;
import org.agrona.CloseHelper;
import org.junit.After;
import uk.co.real_logic.artio.Constants;
import uk.co.real_logic.artio.Reply;
import uk.co.real_logic.artio.Reply.State;
import uk.co.real_logic.artio.builder.ResendRequestEncoder;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.engine.SessionInfo;
import uk.co.real_logic.artio.library.FixLibrary;
import uk.co.real_logic.artio.messages.SessionReplyStatus;
import uk.co.real_logic.artio.messages.SessionState;
import uk.co.real_logic.artio.session.Session;

import java.util.List;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static uk.co.real_logic.artio.Constants.*;
import static uk.co.real_logic.artio.FixMatchers.*;
import static uk.co.real_logic.artio.TestFixtures.cleanupMediaDriver;
import static uk.co.real_logic.artio.TestFixtures.unusedPort;
import static uk.co.real_logic.artio.Timing.assertEventuallyTrue;
import static uk.co.real_logic.artio.messages.SessionReplyStatus.OK;
import static uk.co.real_logic.artio.messages.SessionState.DISCONNECTED;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.*;

public class AbstractGatewayToGatewaySystemTest
{
    protected int port = unusedPort();
    protected int libraryAeronPort = unusedPort();
    protected ArchivingMediaDriver mediaDriver;
    protected TestSystem testSystem;

    FixEngine acceptingEngine;
    FixEngine initiatingEngine;
    FixLibrary acceptingLibrary;
    FixLibrary initiatingLibrary;
    Session initiatingSession;
    Session acceptingSession;

    FakeOtfAcceptor acceptingOtfAcceptor = new FakeOtfAcceptor();
    FakeHandler acceptingHandler = new FakeHandler(acceptingOtfAcceptor);

    FakeOtfAcceptor initiatingOtfAcceptor = new FakeOtfAcceptor();
    FakeHandler initiatingHandler = new FakeHandler(initiatingOtfAcceptor);

    @After
    public void close()
    {
        CloseHelper.close(initiatingLibrary);
        CloseHelper.close(acceptingLibrary);

        CloseHelper.close(initiatingEngine);
        CloseHelper.close(acceptingEngine);

        cleanupMediaDriver(mediaDriver);
    }

    void assertOriginalLibraryDoesNotReceiveMessages(final int initiatorMessageCount)
    {
        initiatingLibrary.poll(LIBRARY_LIMIT);
        assertThat("Messages received by wrong initiator",
            initiatingOtfAcceptor.messages(), hasSize(initiatorMessageCount));
    }

    void assertSequenceFromInitToAcceptAt(
        final int expectedInitToAccSeqNum, final int expectedAccToInitSeqNum)
    {
        assertEquals(expectedInitToAccSeqNum, initiatingSession.lastSentMsgSeqNum());
        assertEquals(expectedInitToAccSeqNum, acceptingSession.lastReceivedMsgSeqNum());

        awaitMessage(expectedAccToInitSeqNum, initiatingSession);

        assertEquals(expectedAccToInitSeqNum, initiatingSession.lastReceivedMsgSeqNum());
        assertEquals(expectedAccToInitSeqNum, acceptingSession.lastSentMsgSeqNum());
    }

    private void awaitMessage(final int sequenceNumber, final Session session)
    {
        assertEventuallyTrue(
            "Library Never reaches " + sequenceNumber,
            () ->
            {
                testSystem.poll();
                return session.lastReceivedMsgSeqNum() >= sequenceNumber;
            });
    }

    void disconnectSessions()
    {
        logoutAcceptingSession();

        assertSessionsDisconnected();

        acceptingSession.close();
        initiatingSession.close();
    }

    void logoutAcceptingSession()
    {
        assertThat(acceptingSession.startLogout(), greaterThan(0L));
    }

    void assertSessionsDisconnected()
    {
        assertSessionDisconnected(initiatingSession);
        assertSessionDisconnected(acceptingSession);

        assertEventuallyTrue("libraries receive disconnect messages",
            () ->
            {
                testSystem.poll();
                assertNotSession(acceptingHandler, acceptingSession);
                assertNotSession(initiatingHandler, initiatingSession);
            });
    }

    protected void assertSessionDisconnected(final Session session)
    {
        assertEventuallyTrue("Session is still connected",
            () ->
            {
                testSystem.poll();
                return session.state() == DISCONNECTED;
            });
    }

    void assertNotSession(final FakeHandler sessionHandler, final Session session)
    {
        assertThat(sessionHandler.sessions(), not(hasItem(session)));
    }

    void wireSessions()
    {
        connectSessions();
        acquireAcceptingSession();
    }

    void acquireAcceptingSession()
    {
        final long sessionId = acceptingHandler.awaitSessionId(testSystem::poll);

        acceptingSession = acquireSession(acceptingHandler, acceptingLibrary, sessionId, testSystem);
        assertEquals(INITIATOR_ID, acceptingHandler.lastInitiatorCompId());
        assertEquals(ACCEPTOR_ID, acceptingHandler.lastAcceptorCompId());
        assertNotNull("unable to acquire accepting session", acceptingSession);
    }

    void connectSessions()
    {
        final Reply<Session> reply = initiate(initiatingLibrary, port, INITIATOR_ID, ACCEPTOR_ID);
        completeConnectSessions(reply);
    }

    void completeConnectSessions(final Reply<Session> reply)
    {
        testSystem.awaitReply(reply);

        initiatingSession = reply.resultIfPresent();

        assertEquals(State.COMPLETED, reply.state());
        assertConnected(initiatingSession);
    }

    FixMessage assertMessageResent(final int sequenceNumber, final String msgType, final boolean isGapFill)
    {
        assertThat(acceptingOtfAcceptor.messages(), hasSize(0));
        assertEventuallyTrue("Failed to receive the reply",
            () ->
            {
                testSystem.poll();

                final FixMessage message = acceptingOtfAcceptor.lastReceivedMessage();
                assertEquals(msgType, message.msgType());
                if (isGapFill)
                {
                    assertEquals("Y", message.get(GAP_FILL_FLAG));
                }
                else
                {
                    assertNotNull(message.get(ORIG_SENDING_TIME));
                }
                assertEquals("Y", message.possDup());
                assertEquals(String.valueOf(sequenceNumber), message.get(MSG_SEQ_NUM));
                assertEquals(INITIATOR_ID, message.get(Constants.SENDER_COMP_ID));
                assertNull("Detected Error", acceptingOtfAcceptor.lastError());
                assertTrue("Failed to complete parsing", acceptingOtfAcceptor.isCompleted());
            });

        return acceptingOtfAcceptor.lastReceivedMessage();
    }

    int acceptorSendsResendRequest()
    {
        final int seqNum = acceptingSession.lastReceivedMsgSeqNum();
        return acceptorSendsResendRequest(seqNum);
    }

    int acceptorSendsResendRequest(final int seqNum)
    {
        final ResendRequestEncoder resendRequest = new ResendRequestEncoder()
            .beginSeqNo(seqNum)
            .endSeqNo(seqNum);

        acceptingOtfAcceptor.messages().clear();

        acceptingSession.send(resendRequest);

        return seqNum;
    }

    void messagesCanBeExchanged()
    {
        final long position = messagesCanBeExchanged(initiatingSession, initiatingOtfAcceptor);

        assertEventuallyTrue("position never catches up",
            () ->
            {
                testSystem.poll();

                return initiatingHandler.sentPosition() >= position;
            });
    }

    long messagesCanBeExchanged(final Session sendingSession, final FakeOtfAcceptor receivingAcceptor)
    {
        final String testReqID = testReqId();
        final long position = sendTestRequest(sendingSession, testReqID);

        assertReceivedSingleHeartbeat(testSystem, receivingAcceptor, testReqID);

        return position;
    }

    void clearMessages()
    {
        initiatingOtfAcceptor.messages().clear();
        acceptingOtfAcceptor.messages().clear();
    }

    void launchAcceptingEngine()
    {
        acceptingEngine = FixEngine.launch(
            acceptingConfig(port, ACCEPTOR_ID, INITIATOR_ID));
    }

    void assertSequenceIndicesAre(final int sequenceIndex)
    {
        assertAcceptingSessionHasSequenceIndex(sequenceIndex);
        assertInitiatingSequenceIndexIs(sequenceIndex);
        assertAllMessagesHaveSequenceIndex(sequenceIndex);
    }

    void assertAcceptingSessionHasSequenceIndex(final int sequenceIndex)
    {
        if (acceptingSession != null)
        {
            assertThat(acceptingSession, hasSequenceIndex(sequenceIndex));
        }
    }

    void assertInitiatingSequenceIndexIs(final int sequenceIndex)
    {
        assertThat(initiatingSession, hasSequenceIndex(sequenceIndex));

    }

    void assertAllMessagesHaveSequenceIndex(final int sequenceIndex)
    {
        acceptingOtfAcceptor.allMessagesHaveSequenceIndex(sequenceIndex);
        initiatingOtfAcceptor.allMessagesHaveSequenceIndex(sequenceIndex);
    }

    void sessionsCanReconnect()
    {
        acquireAcceptingSession();

        acceptingSession.startLogout();
        assertSessionsDisconnected();

        assertAllMessagesHaveSequenceIndex(0);
        clearMessages();

        wireSessions();

        messagesCanBeExchanged();

        assertSequenceIndicesAre(1);
    }

    void releaseSessionToEngine(final Session session, final FixLibrary library, final FixEngine engine)
    {
        final long connectionId = session.connectionId();
        final long sessionId = session.id();

        final SessionReplyStatus status = releaseToGateway(library, session, testSystem);

        assertEquals(OK, status);
        assertEquals(SessionState.DISABLED, session.state());
        assertThat(library.sessions(), hasSize(0));

        final List<SessionInfo> sessions = gatewayLibraryInfo(engine).sessions();
        assertThat(sessions, contains(allOf(
            hasConnectionId(connectionId),
            hasSessionId(sessionId))));
    }

    void engineShouldManageSession(
        final Session session,
        final FixLibrary library,
        final FakeOtfAcceptor otfAcceptor,
        final Session otherSession,
        final FakeOtfAcceptor otherAcceptor)
    {
        final int lastReceivedMsgSeqNum = engineShouldManageSession(session, library, otherSession, otherAcceptor, OK);

        // Callbacks for the missing messages whilst the gateway managed them
        final List<FixMessage> messages = otfAcceptor.messages();
        final String expectedSeqNum = String.valueOf(lastReceivedMsgSeqNum + 1);
        final long messageCount = messages
            .stream()
            .filter((m) -> m.msgType().equals(TEST_REQUEST_MESSAGE_AS_STR) &&
            m.get(MSG_SEQ_NUM).equals(expectedSeqNum))
            .count();

        assertEquals("Expected a single test request" + messages.toString(), 1, messageCount);

        messagesCanBeExchanged(otherSession, otherAcceptor);
    }

    int engineShouldManageSession(
        final Session session,
        final FixLibrary library,
        final Session otherSession,
        final FakeOtfAcceptor otherAcceptor,
        final SessionReplyStatus expectedStatus)
    {
        final long sessionId = session.id();
        final int lastReceivedMsgSeqNum = session.lastReceivedMsgSeqNum();
        final int sequenceIndex = session.sequenceIndex();

        releaseToGateway(library, session, testSystem);

        messagesCanBeExchanged(otherSession, otherAcceptor);

        final SessionReplyStatus status = requestSession(
            library, sessionId, lastReceivedMsgSeqNum, sequenceIndex, testSystem);
        assertEquals(expectedStatus, status);

        final List<Session> sessions = library.sessions();
        assertThat(sessions, hasSize(1));

        final Session newSession = sessions.get(0);
        assertNotSame(session, newSession);
        return lastReceivedMsgSeqNum;
    }
}
