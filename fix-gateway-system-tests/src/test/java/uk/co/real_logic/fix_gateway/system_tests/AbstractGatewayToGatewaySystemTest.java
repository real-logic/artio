/*
 * Copyright 2015-2016 Real Logic Ltd.
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
package uk.co.real_logic.fix_gateway.system_tests;

import io.aeron.driver.MediaDriver;
import org.agrona.CloseHelper;
import org.junit.After;
import uk.co.real_logic.fix_gateway.Reply;
import uk.co.real_logic.fix_gateway.Reply.State;
import uk.co.real_logic.fix_gateway.builder.ResendRequestEncoder;
import uk.co.real_logic.fix_gateway.engine.FixEngine;
import uk.co.real_logic.fix_gateway.engine.framer.LibraryInfo;
import uk.co.real_logic.fix_gateway.library.FixLibrary;
import uk.co.real_logic.fix_gateway.session.Session;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static uk.co.real_logic.fix_gateway.FixMatchers.hasConnectionId;
import static uk.co.real_logic.fix_gateway.FixMatchers.hasSequenceIndex;
import static uk.co.real_logic.fix_gateway.TestFixtures.cleanupMediaDriver;
import static uk.co.real_logic.fix_gateway.TestFixtures.unusedPort;
import static uk.co.real_logic.fix_gateway.Timing.assertEventuallyTrue;
import static uk.co.real_logic.fix_gateway.decoder.Constants.MSG_SEQ_NUM;
import static uk.co.real_logic.fix_gateway.engine.FixEngine.ENGINE_LIBRARY_ID;
import static uk.co.real_logic.fix_gateway.messages.SessionState.DISCONNECTED;
import static uk.co.real_logic.fix_gateway.system_tests.SystemTestUtil.*;

public class AbstractGatewayToGatewaySystemTest
{
    protected int port = unusedPort();
    protected int libraryAeronPort = unusedPort();

    protected MediaDriver mediaDriver;
    protected FixEngine acceptingEngine;
    protected FixEngine initiatingEngine;
    protected FixLibrary acceptingLibrary;
    protected FixLibrary initiatingLibrary;
    protected Session initiatingSession;
    protected Session acceptingSession;

    protected FakeOtfAcceptor acceptingOtfAcceptor = new FakeOtfAcceptor();
    protected FakeHandler acceptingHandler = new FakeHandler(acceptingOtfAcceptor);

    protected FakeOtfAcceptor initiatingOtfAcceptor = new FakeOtfAcceptor();
    protected FakeHandler initiatingHandler = new FakeHandler(initiatingOtfAcceptor);

    protected TestSystem testSystem;

    @After
    public void close()
    {
        CloseHelper.close(initiatingLibrary);
        CloseHelper.close(acceptingLibrary);

        CloseHelper.close(initiatingEngine);
        CloseHelper.close(acceptingEngine);

        cleanupMediaDriver(mediaDriver);
    }

    protected void assertOriginalLibraryDoesNotReceiveMessages(final int initiatorMessageCount)
    {
        initiatingLibrary.poll(LIBRARY_LIMIT);
        assertThat("Messages received by wrong initiator",
            initiatingOtfAcceptor.messages(), hasSize(initiatorMessageCount));
    }

    protected void assertSequenceFromInitToAcceptAt(
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

    protected void assertSessionsDisconnected()
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

    protected void assertNotSession(final FakeHandler sessionHandler, final Session session)
    {
        assertThat(sessionHandler.sessions(), not(hasItem(session)));
    }

    protected void wireSessions()
    {
        connectSessions();
        acquireAcceptingSession();
    }

    protected void acquireAcceptingSession()
    {
        acceptingSession = acquireSession(acceptingHandler, acceptingLibrary);
        assertEquals(INITIATOR_ID, acceptingHandler.lastInitiatorCompId());
        assertEquals(ACCEPTOR_ID, acceptingHandler.lastAcceptorCompId());
        assertNotNull("unable to acquire accepting session", acceptingSession);
        assertEquals(USERNAME, acceptingSession.username());
        assertEquals(PASSWORD, acceptingSession.password());
    }

    protected void connectSessions()
    {
        final Reply<Session> reply = initiate(initiatingLibrary, port, INITIATOR_ID, ACCEPTOR_ID);

        awaitLibraryReply(testSystem, reply);
        initiatingSession = reply.resultIfPresent();

        assertEquals(State.COMPLETED, reply.state());
        assertConnected(initiatingSession);
        sessionLogsOn(testSystem, initiatingSession);
    }

    protected void assertMessageResent(final int sequenceNumber)
    {
        assertThat(acceptingOtfAcceptor.messages(), hasSize(0));
        assertEventuallyTrue("Failed to receive the reply",
            () ->
            {
                testSystem.poll();

                final FixMessage message = acceptingOtfAcceptor.lastMessage();
                final String messageType = message.getMsgType();
                assertEquals("1", messageType);
                assertEquals("Y", message.getPossDup());
                assertEquals(String.valueOf(sequenceNumber), message.get(MSG_SEQ_NUM));
                assertEquals(INITIATOR_ID, acceptingOtfAcceptor.lastSenderCompId());
                assertNull("Detected Error", acceptingOtfAcceptor.lastError());
                assertTrue("Failed to complete parsing", acceptingOtfAcceptor.isCompleted());
            });
    }

    protected int sendResendRequest()
    {
        final int seqNum = acceptingSession.lastReceivedMsgSeqNum();
        final ResendRequestEncoder resendRequest = new ResendRequestEncoder()
            .beginSeqNo(seqNum)
            .endSeqNo(seqNum);

        acceptingOtfAcceptor.messages().clear();

        acceptingSession.send(resendRequest);

        return seqNum;
    }

    protected void messagesCanBeExchanged()
    {
        final long position = messagesCanBeExchanged(initiatingSession, initiatingOtfAcceptor);

        assertEventuallyTrue("position never catches up",
            () ->
            {
                testSystem.poll();

                return initiatingHandler.sentPosition() >= position;
            });
    }

    protected long messagesCanBeExchanged(
        final Session sendingSession,
        final FakeOtfAcceptor receivingAcceptor)
    {
        final long position = sendTestRequest(sendingSession);

        assertReceivedHeartbeat(testSystem, receivingAcceptor);

        return position;
    }

    protected void clearMessages()
    {
        initiatingOtfAcceptor.messages().clear();
        acceptingOtfAcceptor.messages().clear();
    }

    protected void launchAcceptingEngine()
    {
        acceptingEngine = FixEngine.launch(acceptingConfig(port, "engineCounters", ACCEPTOR_ID, INITIATOR_ID));
    }

    protected void acceptingEngineHasSessionAndLibraryIsNotified()
    {
        try (LibraryDriver library2 = new LibraryDriver())
        {
            library2.becomeOnlyLibraryConnectedTo(acceptingEngine);

            final LibraryInfo engine = engineLibrary(libraries(acceptingEngine));

            assertEquals(ENGINE_LIBRARY_ID, engine.libraryId());
            assertThat(engine.sessions(), contains(hasConnectionId(acceptingSession.connectionId())));

            final long sessionId = library2.awaitSessionId();
            assertEquals(sessionId, acceptingSession.id());
        }
    }

    protected void assertSequenceIndicesAre(final int sequenceIndex)
    {
        if (acceptingSession != null)
        {
            assertThat(acceptingSession, hasSequenceIndex(sequenceIndex));
        }
        assertInitiatingSequenceIndexIs(sequenceIndex);
        assertAllMessagesHaveSequenceIndex(sequenceIndex);
    }

    protected void assertInitiatingSequenceIndexIs(final int sequenceIndex)
    {
        assertThat(initiatingSession, hasSequenceIndex(sequenceIndex));

    }

    protected void assertAllMessagesHaveSequenceIndex(final int sequenceIndex)
    {
        acceptingOtfAcceptor.allMessagesHaveSequenceIndex(sequenceIndex);
        initiatingOtfAcceptor.allMessagesHaveSequenceIndex(sequenceIndex);
    }
}
