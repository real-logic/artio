/*
 * Copyright 2015-2017 Real Logic Ltd.
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

import io.aeron.driver.MediaDriver;
import org.agrona.CloseHelper;
import org.junit.After;
import uk.co.real_logic.artio.Reply;
import uk.co.real_logic.artio.Reply.State;
import uk.co.real_logic.artio.builder.ResendRequestEncoder;
import uk.co.real_logic.artio.Constants;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.library.FixLibrary;
import uk.co.real_logic.artio.session.Session;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static uk.co.real_logic.artio.FixMatchers.hasSequenceIndex;
import static uk.co.real_logic.artio.TestFixtures.cleanupMediaDriver;
import static uk.co.real_logic.artio.TestFixtures.unusedPort;
import static uk.co.real_logic.artio.Timing.DEFAULT_TIMEOUT_IN_MS;
import static uk.co.real_logic.artio.Timing.assertEventuallyTrue;
import static uk.co.real_logic.artio.Constants.*;
import static uk.co.real_logic.artio.messages.SessionState.DISCONNECTED;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.*;

public class AbstractGatewayToGatewaySystemTest
{
    protected int port = unusedPort();
    protected int libraryAeronPort = unusedPort();
    protected MediaDriver mediaDriver;
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

    private void assertSessionDisconnected(final Session session)
    {
        assertEventuallyTrue("Session is still connected",
            () ->
            {
                testSystem.poll();
                return session.state() == DISCONNECTED;
            });
    }

    private void assertNotSession(final FakeHandler sessionHandler, final Session session)
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
        final Reply<Session> reply = testSystem.awaitReply(
            initiate(initiatingLibrary, port, INITIATOR_ID, ACCEPTOR_ID));

        initiatingSession = reply.resultIfPresent();

        assertEquals(State.COMPLETED, reply.state());
        assertConnected(initiatingSession);
        sessionLogsOn(testSystem, initiatingSession, DEFAULT_TIMEOUT_IN_MS);
    }

    FixMessage assertMessageResent(final int sequenceNumber, final String msgType, final boolean isGapFill)
    {
        assertThat(acceptingOtfAcceptor.messages(), hasSize(0));
        assertEventuallyTrue("Failed to receive the reply",
            () ->
            {
                testSystem.poll();

                final FixMessage message = acceptingOtfAcceptor.lastMessage();
                assertEquals(msgType, message.getMsgType());
                if (isGapFill)
                {
                    assertEquals("Y", message.get(GAP_FILL_FLAG));
                }
                else
                {
                    assertNotNull(message.get(ORIG_SENDING_TIME));
                }
                assertEquals("Y", message.getPossDup());
                assertEquals(String.valueOf(sequenceNumber), message.get(MSG_SEQ_NUM));
                assertEquals(INITIATOR_ID, message.get(Constants.SENDER_COMP_ID));
                assertNull("Detected Error", acceptingOtfAcceptor.lastError());
                assertTrue("Failed to complete parsing", acceptingOtfAcceptor.isCompleted());
            });

        return acceptingOtfAcceptor.lastMessage();
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
}
