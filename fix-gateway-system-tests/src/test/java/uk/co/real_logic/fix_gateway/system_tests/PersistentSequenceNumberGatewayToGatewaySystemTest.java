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
import uk.co.real_logic.fix_gateway.library.FixLibrary;
import uk.co.real_logic.fix_gateway.library.LibraryConfiguration;
import uk.co.real_logic.fix_gateway.library.SessionConfiguration;
import uk.co.real_logic.fix_gateway.session.Session;

import java.io.File;
import java.io.IOException;

import static io.aeron.CommonContext.IPC_CHANNEL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static uk.co.real_logic.fix_gateway.Reply.State.COMPLETED;
import static uk.co.real_logic.fix_gateway.TestFixtures.launchMediaDriver;
import static uk.co.real_logic.fix_gateway.Timing.assertEventuallyTrue;
import static uk.co.real_logic.fix_gateway.Timing.withTimeout;
import static uk.co.real_logic.fix_gateway.library.SessionConfiguration.AUTOMATIC_INITIAL_SEQUENCE_NUMBER;
import static uk.co.real_logic.fix_gateway.system_tests.SystemTestUtil.*;
import static uk.co.real_logic.fix_gateway.validation.PersistenceLevel.REPLICATED;

public class PersistentSequenceNumberGatewayToGatewaySystemTest extends AbstractGatewayToGatewaySystemTest
{
    private static final long TEST_TIMEOUT = 10_000L;
    private File backupLocation = null;

    @Before
    public void setUp() throws IOException
    {
        delete(ACCEPTOR_LOGS);
        delete(CLIENT_LOGS);
        backupLocation = File.createTempFile("backup", "tmp");

        launch(AUTOMATIC_INITIAL_SEQUENCE_NUMBER, false, false);

        assertSequenceIndicesAre(0);
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
    }

    private void pollLibraries()
    {
        poll(initiatingLibrary, acceptingLibrary);
    }

    @Test(timeout = TEST_TIMEOUT)
    public void customInitialSequenceNumbersCanBeSet()
    {
        sequenceNumbersCanPersistOverRestarts(4);
    }

    @Test(timeout = TEST_TIMEOUT)
    public void sessionsCanBeReset()
    {
        exchangeMessagesAroundARestart(AUTOMATIC_INITIAL_SEQUENCE_NUMBER, 1, true, false);
    }

    @Test(timeout = TEST_TIMEOUT)
    public void sequenceNumbersCanBeReset()
    {
        exchangeMessagesAroundARestart(AUTOMATIC_INITIAL_SEQUENCE_NUMBER, 2, false, true);
    }

    private void launch(final int initialSequenceNumber, final boolean resetAll, final boolean resetSequenceNumbers)
    {
        mediaDriver = launchMediaDriver();

        final EngineConfiguration config =
            acceptingConfig(port, "engineCounters", ACCEPTOR_ID, INITIATOR_ID);
        config.sessionPersistenceStrategy(logon -> REPLICATED);
        acceptingEngine = FixEngine.launch(config);
        initiatingEngine = launchInitiatingGatewayWithSameLogs(libraryAeronPort);

        final LibraryConfiguration acceptingLibraryConfig =
            acceptingLibraryConfig(acceptingHandler, ACCEPTOR_ID, INITIATOR_ID, IPC_CHANNEL);
        acceptingLibrary = FixLibrary.connect(acceptingLibraryConfig);
        initiatingLibrary = newInitiatingLibrary(libraryAeronPort, initiatingHandler);

        if (resetAll)
        {
            acceptingEngine.resetSessionIds(backupLocation, ADMIN_IDLE_STRATEGY);
            initiatingEngine.resetSessionIds(backupLocation, ADMIN_IDLE_STRATEGY);
        }

        if (resetSequenceNumbers)
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

        connectPersistingSessions(initialSequenceNumber);
    }

    private void connectPersistingSessions(final int initialSequenceNumber)
    {
        final SessionConfiguration config = SessionConfiguration.builder()
            .address("localhost", port)
            .credentials("bob", "Uv1aegoh")
            .senderCompId(INITIATOR_ID)
            .targetCompId(ACCEPTOR_ID)
            .sequenceNumbersPersistent(true)
            .initialSequenceNumber(initialSequenceNumber)
            .build();

        final Reply<Session> reply = initiatingLibrary.initiate(config);
        awaitLibraryReply(initiatingLibrary, reply);
        initiatingSession = reply.resultIfPresent();

        assertConnected(initiatingSession);
        sessionLogsOn(initiatingLibrary, acceptingLibrary, initiatingSession);
        acceptingSession = acquireSession(acceptingHandler, acceptingLibrary);
    }

    private void sequenceNumbersCanPersistOverRestarts(final int initialSequenceNumber)
    {
        exchangeMessagesAroundARestart(initialSequenceNumber, 4, false, false);
    }

    private void exchangeMessagesAroundARestart(
        final int initialSequenceNumber,
        final int sequNumAfter,
        final boolean resetAll,
        final boolean resetSequenceNumbers)
    {
        sendTestRequest(initiatingSession);
        assertReceivedTestRequest(initiatingLibrary, acceptingLibrary, acceptingOtfAcceptor);
        assertSequenceFromInitToAcceptAt(2, 2);

        final long initiatedSessionId = initiatingSession.id();
        final long acceptingSessionId = acceptingSession.id();

        initiatingSession.startLogout();
        assertSessionsDisconnected();

        close();

        launch(initialSequenceNumber, resetAll, resetSequenceNumbers);

        assertEquals("initiatedSessionId not stable over restarts", initiatedSessionId, initiatingSession.id());
        assertEquals("acceptingSessionId not stable over restarts", acceptingSessionId, acceptingSession.id());
        assertSequenceFromInitToAcceptAt(sequNumAfter, sequNumAfter);

        sendTestRequest(initiatingSession);
        assertReceivedTestRequest(initiatingLibrary, acceptingLibrary, acceptingOtfAcceptor);
    }
}
