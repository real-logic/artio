/*
 * Copyright 2019 Adaptive Financial Consulting Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.co.real_logic.artio.system_tests;

import org.junit.After;
import org.junit.Test;
import uk.co.real_logic.artio.Reply;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.library.LibraryConfiguration;
import uk.co.real_logic.artio.session.Session;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static uk.co.real_logic.artio.Constants.LOGOUT_MESSAGE_AS_STR;
import static uk.co.real_logic.artio.TestFixtures.launchMediaDriver;
import static uk.co.real_logic.artio.library.SessionConfiguration.AUTOMATIC_INITIAL_SEQUENCE_NUMBER;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.*;
import static uk.co.real_logic.artio.validation.SessionPersistenceStrategy.alwaysPersistent;

public class StateResetAndCloseTest extends AbstractGatewayToGatewaySystemTest
{

    private final Backup backup = new Backup();

    @After
    public void cleanup()
    {
        backup.cleanup();
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldPerformEndOfDayOperationWithLibrarySession()
    {
        shouldPerformEndOfDayOperation(true);
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldPerformEndOfDayOperationWithGatewaySession()
    {
        shouldPerformEndOfDayOperation(false);
    }

    // Reproduces a case where a library reconnect, continuing a previous recording
    @Test
    public void shouldResetStateEvenWithALibraryReconnect()
    {
        deleteLogs();
        mediaDriver = launchMediaDriver();
        launch(AUTOMATIC_INITIAL_SEQUENCE_NUMBER, AUTOMATIC_INITIAL_SEQUENCE_NUMBER);
        acquireAcceptingSession();

        messagesCanBeExchanged();

        testSystem.remove(acceptingLibrary);
        awaitLibraryDisconnect(acceptingEngine, testSystem);

        testSystem.add(acceptingLibrary);
        awaitLibraryCount(acceptingEngine, testSystem, 2);

        testSystem.awaitBlocking(() -> acceptingEngine.close());
        acceptingEngine.resetState(null);
    }

    private void shouldPerformEndOfDayOperation(final boolean libraryOwnsSession)
    {
        deleteLogs();
        mediaDriver = launchMediaDriver();
        launch(AUTOMATIC_INITIAL_SEQUENCE_NUMBER, AUTOMATIC_INITIAL_SEQUENCE_NUMBER);
        if (libraryOwnsSession)
        {
            acquireAcceptingSession();
        }

        messagesCanBeExchanged();
        if (libraryOwnsSession)
        {
            assertSequenceFromInitToAcceptAt(2, 2);
        }

        testSystem.awaitBlocking(() -> acceptingEngine.close());
        assertTrue(acceptingEngine.isClosed());

        final FixMessage logout = testSystem.awaitMessageOf(initiatingOtfAcceptor, LOGOUT_MESSAGE_AS_STR);
        assertEquals(3, logout.messageSequenceNumber());

        testSystem.awaitBlocking(() -> backup.resetState(acceptingEngine));

        backup.assertStateReset(mediaDriver, lessThanOrEqualTo(4));

        clearMessages();
        acceptingSession = null;
        close();

        // resetState should be idempotent
        backup.resetState(acceptingEngine);

        launchMediaDriverWithDirs();
        launch(1, 1);
        if (libraryOwnsSession)
        {
            acquireAcceptingSession();
        }

        messagesCanBeExchanged();

        if (libraryOwnsSession)
        {
            assertSequenceFromInitToAcceptAt(2, 2);
        }

        assertAcceptingSessionHasSequenceIndex(0);
        backup.assertRecordingsTruncated();
    }

    private void launch(
        final int initiatorInitialSentSequenceNumber,
        final int initialReceivedSequenceNumber)
    {
        final EngineConfiguration acceptingConfig = acceptingConfig(port, ACCEPTOR_ID, INITIATOR_ID, nanoClock);
        acceptingConfig.sessionPersistenceStrategy(alwaysPersistent());
        acceptingEngine = FixEngine.launch(acceptingConfig);

        final EngineConfiguration initiatingConfig = initiatingConfig(libraryAeronPort, nanoClock);
        initiatingEngine = FixEngine.launch(initiatingConfig);

        final LibraryConfiguration acceptingLibraryConfig = acceptingLibraryConfig(acceptingHandler, nanoClock);
        acceptingLibrary = connect(acceptingLibraryConfig);

        final LibraryConfiguration initiatingLibraryConfig =
            initiatingLibraryConfig(libraryAeronPort, initiatingHandler, nanoClock);
        initiatingLibrary = connect(initiatingLibraryConfig);

        testSystem = new TestSystem(acceptingLibrary, initiatingLibrary);

        final Reply<Session> reply = connectPersistentSessions(
            initiatorInitialSentSequenceNumber, initialReceivedSequenceNumber, false);
        assertEquals("Reply failed: " + reply, Reply.State.COMPLETED, reply.state());
        initiatingSession = reply.resultIfPresent();
    }
}
