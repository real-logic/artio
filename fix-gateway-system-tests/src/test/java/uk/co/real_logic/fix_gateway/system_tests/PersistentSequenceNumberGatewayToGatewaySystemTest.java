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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.fix_gateway.Reply;
import uk.co.real_logic.fix_gateway.builder.ResendRequestEncoder;
import uk.co.real_logic.fix_gateway.decoder.Constants;
import uk.co.real_logic.fix_gateway.engine.EngineConfiguration;
import uk.co.real_logic.fix_gateway.engine.FixEngine;
import uk.co.real_logic.fix_gateway.library.SessionConfiguration;
import uk.co.real_logic.fix_gateway.session.Session;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.*;
import static uk.co.real_logic.fix_gateway.Reply.State.COMPLETED;
import static uk.co.real_logic.fix_gateway.TestFixtures.launchMediaDriver;
import static uk.co.real_logic.fix_gateway.Timing.assertEventuallyTrue;
import static uk.co.real_logic.fix_gateway.Timing.withTimeout;
import static uk.co.real_logic.fix_gateway.library.FixLibrary.NO_MESSAGE_REPLAY;
import static uk.co.real_logic.fix_gateway.library.SessionConfiguration.AUTOMATIC_INITIAL_SEQUENCE_NUMBER;
import static uk.co.real_logic.fix_gateway.system_tests.FixMessage.hasMessageSequenceNumber;
import static uk.co.real_logic.fix_gateway.system_tests.FixMessage.hasSequenceIndex;
import static uk.co.real_logic.fix_gateway.system_tests.SystemTestUtil.*;
import static uk.co.real_logic.fix_gateway.validation.PersistenceLevel.REPLICATED;

public class PersistentSequenceNumberGatewayToGatewaySystemTest extends AbstractGatewayToGatewaySystemTest
{
    private static final long TEST_TIMEOUT = 10_000L;
    private File backupLocation = null;

    private Runnable acquireSessionTask = () ->
    {
        final long sessionId = getAcceptingSessionId();

        acquireSession(sessionId, NO_MESSAGE_REPLAY, NO_MESSAGE_REPLAY);
    };

    @Before
    public void setUp() throws IOException
    {
        delete(ACCEPTOR_LOGS);
        delete(CLIENT_LOGS);
        backupLocation = File.createTempFile("backup", "tmp");
    }

    @After
    public void cleanupBackup()
    {
        if (null != backupLocation)
        {
            assertTrue("Failed to delete: " + backupLocation, backupLocation.delete());
        }
    }

    @Test(timeout = TEST_TIMEOUT)
    public void sequenceNumbersCanPersistOverRestarts()
    {
        sequenceNumbersCanPersistOverRestarts(AUTOMATIC_INITIAL_SEQUENCE_NUMBER);

        assertSequenceIndicesAre(0);
    }

    @Test(timeout = TEST_TIMEOUT)
    public void previousMessagesAreReplayed()
    {
        acquireSessionTask = () ->
        {
            final long sessionId = getAcceptingSessionId();

            if (acceptingSession != null)
            {
                final int lastReceivedMsgSeqNum = acceptingSession.lastReceivedMsgSeqNum();
                final int sequenceIndex = acceptingSession.sequenceIndex();
                acquireSession(sessionId, lastReceivedMsgSeqNum, sequenceIndex);

                final FixMessage firstReplayedMessage = acceptingOtfAcceptor.messages().get(0);
                assertThat(firstReplayedMessage, hasMessageSequenceNumber(lastReceivedMsgSeqNum + 1));
                assertThat(firstReplayedMessage, hasSequenceIndex(sequenceIndex));
            }
            else
            {
                acquireSession(sessionId, NO_MESSAGE_REPLAY, NO_MESSAGE_REPLAY);
            }
        };

        sequenceNumbersCanPersistOverRestarts(AUTOMATIC_INITIAL_SEQUENCE_NUMBER);

        assertSequenceIndicesAre(0);
    }

    @Test(timeout = TEST_TIMEOUT)
    public void messagesCanBeReplayedOverRestart()
    {
        sequenceNumbersCanPersistOverRestarts(AUTOMATIC_INITIAL_SEQUENCE_NUMBER);

        final ResendRequestEncoder resendRequest = new ResendRequestEncoder();
        resendRequest.beginSeqNo(1).endSeqNo(1);

        initiatingOtfAcceptor.messages().clear();

        assertEventuallyTrue(
            "Unable to send resend request",
            () ->
            {
                pollLibraries();
                return initiatingSession.send(resendRequest) > 0;
            });

        final FixMessage message = withTimeout(
            "Failed to receive reply",
            () ->
            {
                pollLibraries();
                return initiatingOtfAcceptor.hasReceivedMessage("A");
            },
            2_000);

        assertEquals(message.get(Constants.MSG_SEQ_NUM), "1");
        assertEquals(message.get(Constants.SENDER_COMP_ID), ACCEPTOR_ID);
        assertEquals(message.get(Constants.TARGET_COMP_ID), INITIATOR_ID);

        assertSequenceIndicesAre(0);
    }

    @Test(timeout = TEST_TIMEOUT)
    public void customInitialSequenceNumbersCanBeSet()
    {
        sequenceNumbersCanPersistOverRestarts(4);

        assertSequenceIndicesAre(0);
    }

    @Test(timeout = TEST_TIMEOUT)
    public void sessionsCanBeReset()
    {
        exchangeMessagesAroundARestart(
            AUTOMATIC_INITIAL_SEQUENCE_NUMBER, 1, this::resetSessions, false);

        // Different sessions themselves, so we start again at 0
        assertSequenceIndicesAre(0);
    }

    private void resetSessions()
    {
        acceptingEngine.resetSessionIds(backupLocation, ADMIN_IDLE_STRATEGY);
        initiatingEngine.resetSessionIds(backupLocation, ADMIN_IDLE_STRATEGY);
    }

    @Test(timeout = TEST_TIMEOUT)
    public void sequenceNumbersCanBeReset()
    {
        exchangeMessagesAroundARestart(
            AUTOMATIC_INITIAL_SEQUENCE_NUMBER, 2, this::resetSequenceNumbers, false);

        assertSequenceIndicesAre(1);
    }

    private void resetSequenceNumbers()
    {
        final Reply<?> initiatingReply =
            initiatingEngine.resetSequenceNumber(initiatingSession.id());
        final Reply<?> acceptingReply =
            acceptingEngine.resetSequenceNumber(acceptingSession.id());

        assertNotNull(initiatingReply);
        assertNotNull(acceptingReply);

        assertEventuallyTrue(
            "Failed to reset sequence numbers",
            () ->
            {
                initiatingLibrary.poll(LIBRARY_LIMIT);
                acceptingLibrary.poll(LIBRARY_LIMIT);
                return (!initiatingReply.isExecuting() && !acceptingReply.isExecuting());
            });

        assertEquals(COMPLETED, initiatingReply.state());
        assertEquals(COMPLETED, acceptingReply.state());
    }

    @Test(timeout = TEST_TIMEOUT)
    public void sequenceNumbersCanBeResetOnLogon()
    {
        exchangeMessagesAroundARestart(
            AUTOMATIC_INITIAL_SEQUENCE_NUMBER, 1, this::nothing, true);

        acceptingOtfAcceptor.logonMessagesHaveSequenceNumbers(1);
        initiatingOtfAcceptor.logonMessagesHaveSequenceNumbers(1);
        assertSequenceIndicesAre(1);
    }

    private void launch(
        final int initialSequenceNumber,
        final Runnable beforeConnect,
        final boolean resetSequenceNumbersOnLogon)
    {
        mediaDriver = launchMediaDriver();

        final EngineConfiguration config =
            acceptingConfig(port, "engineCounters", ACCEPTOR_ID, INITIATOR_ID);
        config.sessionPersistenceStrategy(logon -> REPLICATED);
        acceptingEngine = FixEngine.launch(config);
        initiatingEngine = launchInitiatingEngineWithSameLogs(libraryAeronPort);

        acceptingLibrary = newAcceptingLibrary(acceptingHandler);
        initiatingLibrary = newInitiatingLibrary(libraryAeronPort, initiatingHandler);
        testSystem = new TestSystem(acceptingLibrary, initiatingLibrary);

        beforeConnect.run();

        connectPersistingSessions(initialSequenceNumber, resetSequenceNumbersOnLogon);
    }

    private void connectPersistingSessions(final int initialSequenceNumber, final boolean resetSeqNum)
    {
        final SessionConfiguration config = SessionConfiguration.builder()
            .address("localhost", port)
            .credentials("bob", "Uv1aegoh")
            .senderCompId(INITIATOR_ID)
            .targetCompId(ACCEPTOR_ID)
            .sequenceNumbersPersistent(true)
            .initialSequenceNumber(initialSequenceNumber)
            .resetSeqNum(resetSeqNum)
            .build();

        final Reply<Session> reply = initiatingLibrary.initiate(config);
        awaitLibraryReply(initiatingLibrary, reply);
        initiatingSession = reply.resultIfPresent();

        assertConnected(initiatingSession);
        sessionLogsOn(initiatingLibrary, acceptingLibrary, initiatingSession);

        acquireSessionTask.run();
    }

    private void sequenceNumbersCanPersistOverRestarts(final int initialSequenceNumber)
    {
        exchangeMessagesAroundARestart(initialSequenceNumber, 4, this::nothing, false);
    }

    private void exchangeMessagesAroundARestart(
        final int initialSequenceNumber,
        final int sequNumAfter,
        final Runnable beforeConnect,
        final boolean resetSequenceNumbersOnLogon)
    {
        launch(AUTOMATIC_INITIAL_SEQUENCE_NUMBER, this::nothing, resetSequenceNumbersOnLogon);

        assertSequenceIndicesAre(0);

        sendTestRequest(initiatingSession);
        assertReceivedTestRequest(initiatingLibrary, acceptingLibrary, acceptingOtfAcceptor);
        assertSequenceFromInitToAcceptAt(2, 2);

        final long initiatedSessionId = initiatingSession.id();
        final long acceptingSessionId = acceptingSession.id();

        initiatingSession.startLogout();
        assertSessionsDisconnected();

        assertInitiatingSequenceIndexIs(0);
        clearMessages();
        close();

        launch(initialSequenceNumber, beforeConnect, resetSequenceNumbersOnLogon);

        assertEquals("initiatedSessionId not stable over restarts", initiatedSessionId, initiatingSession.id());
        assertEquals("acceptingSessionId not stable over restarts", acceptingSessionId, acceptingSession.id());
        assertSequenceFromInitToAcceptAt(sequNumAfter, sequNumAfter);

        sendTestRequest(initiatingSession);
        assertReceivedTestRequest(initiatingLibrary, acceptingLibrary, acceptingOtfAcceptor);
    }

    private void nothing()
    {

    }

    private long getAcceptingSessionId()
    {
        return acceptingHandler.awaitSessionId(() -> acceptingLibrary.poll(LIBRARY_LIMIT));
    }

    private void acquireSession(final long sessionId, final int lastReceivedMsgSeqNum, final int sequenceIndex)
    {
        acceptingSession = SystemTestUtil.acquireSession(
            acceptingHandler, acceptingLibrary, sessionId, lastReceivedMsgSeqNum, sequenceIndex);
    }
}
