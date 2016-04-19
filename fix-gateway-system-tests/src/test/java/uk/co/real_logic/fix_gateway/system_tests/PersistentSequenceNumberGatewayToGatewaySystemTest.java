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
import uk.co.real_logic.fix_gateway.engine.EngineConfiguration;
import uk.co.real_logic.fix_gateway.engine.FixEngine;
import uk.co.real_logic.fix_gateway.library.FixLibrary;
import uk.co.real_logic.fix_gateway.library.LibraryConfiguration;
import uk.co.real_logic.fix_gateway.library.SessionConfiguration;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static uk.co.real_logic.fix_gateway.TestFixtures.launchMediaDriver;
import static uk.co.real_logic.fix_gateway.library.SessionConfiguration.AUTOMATIC_INITIAL_SEQUENCE_NUMBER;
import static uk.co.real_logic.fix_gateway.system_tests.SystemTestUtil.*;

public class PersistentSequenceNumberGatewayToGatewaySystemTest extends AbstractGatewayToGatewaySystemTest
{
    private File backupLocation;

    @Before
    public void setUp() throws IOException
    {
        delete(ACCEPTOR_LOGS);
        delete(CLIENT_LOGS);
        backupLocation = File.createTempFile("backup", "tmp");

        launch(AUTOMATIC_INITIAL_SEQUENCE_NUMBER, false);
    }

    @Test(timeout = 10_000L)
    public void sequenceNumbersCanPersistOverRestarts()
    {
        sequenceNumbersCanPersistOverRestarts(AUTOMATIC_INITIAL_SEQUENCE_NUMBER);
    }

    @Test(timeout = 10_000L)
    public void customInitialSequenceNumbersCanBeSet()
    {
        sequenceNumbersCanPersistOverRestarts(4);
    }

    @Test(timeout = 10_000L)
    public void sequenceNumbersCanBeReset()
    {
        exchangeMessagesAroundARestart(AUTOMATIC_INITIAL_SEQUENCE_NUMBER, 1, true);
    }

    private void launch(final int initialSequenceNumber, final boolean reset)
    {
        mediaDriver = launchMediaDriver();

        final EngineConfiguration config =
            acceptingConfig(port, "engineCounters", ACCEPTOR_ID, INITIATOR_ID);
        config.acceptorSequenceNumbersResetUponReconnect(false);
        acceptingEngine = FixEngine.launch(config);
        initiatingEngine = launchInitiatingGatewayWithSameLogs(initAeronPort);

        final LibraryConfiguration acceptingLibraryConfig =
            acceptingLibraryConfig(acceptingSessionHandler, ACCEPTOR_ID, INITIATOR_ID, "fix-acceptor");
        acceptingLibraryConfig.acceptorSequenceNumbersResetUponReconnect(false);
        acceptingLibrary = FixLibrary.connect(acceptingLibraryConfig);
        initiatingLibrary = newInitiatingLibrary(initAeronPort, initiatingSessionHandler, 1);

        if (reset)
        {
            acceptingEngine.resetSessionIds(backupLocation, ADMIN_IDLE_STRATEGY);
            initiatingEngine.resetSessionIds(backupLocation, ADMIN_IDLE_STRATEGY);
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

        initiatingSession = initiatingLibrary.initiate(config);

        assertConnected(initiatingSession);
        sessionLogsOn(initiatingLibrary, acceptingLibrary, initiatingSession);
        acceptingSession = acquireSession(acceptingSessionHandler, acceptingLibrary);
    }

    private void sequenceNumbersCanPersistOverRestarts(final int initialSequenceNumber)
    {
        exchangeMessagesAroundARestart(initialSequenceNumber, 4, false);
    }

    private void exchangeMessagesAroundARestart(final int initialSequenceNumber,
                                                final int sequNumAfter,
                                                final boolean reset)
    {
        sendTestRequest(initiatingSession);
        assertReceivedTestRequest(initiatingLibrary, acceptingLibrary, acceptingOtfAcceptor);
        assertSequenceFromInitToAcceptAt(2, 2);

        final long initiatedSessionId = initiatingSession.id();
        final long acceptingSessionId = acceptingSession.id();

        initiatingSession.startLogout();
        assertSessionsDisconnected();

        close();

        launch(initialSequenceNumber, reset);

        assertEquals("initiatedSessionId not stable over restarts", initiatedSessionId, initiatingSession.id());
        assertEquals("acceptingSessionId not stable over restarts", acceptingSessionId, acceptingSession.id());
        assertSequenceFromInitToAcceptAt(sequNumAfter, sequNumAfter);

        sendTestRequest(initiatingSession);
        assertReceivedTestRequest(initiatingLibrary, acceptingLibrary, acceptingOtfAcceptor);
    }

    @After
    public void cleanupBackup()
    {
        assertTrue("Failed to delete: " + backupLocation, backupLocation.delete());
    }
}
