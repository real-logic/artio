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

import io.aeron.archive.client.AeronArchive;
import org.agrona.IoUtil;
import org.junit.After;
import org.junit.Test;
import uk.co.real_logic.artio.Reply;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.library.LibraryConfiguration;
import uk.co.real_logic.artio.session.Session;

import java.io.File;
import java.util.Objects;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static uk.co.real_logic.artio.Constants.LOGOUT_MESSAGE_AS_STR;
import static uk.co.real_logic.artio.TestFixtures.launchMediaDriver;
import static uk.co.real_logic.artio.library.SessionConfiguration.AUTOMATIC_INITIAL_SEQUENCE_NUMBER;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.*;
import static uk.co.real_logic.artio.validation.PersistenceLevel.INDEXED;

public class StateResetAndCloseTest extends AbstractGatewayToGatewaySystemTest
{

    private File backupLocation = new File("backup");

    @After
    public void cleanup()
    {
        if (backupLocation.exists())
        {
            IoUtil.delete(backupLocation, false);
        }
    }

    @Test
    public void shouldPerformEndOfDayOperationWithLibrarySession()
    {
        shouldPerformEndOfDayOperation(true);
    }

    @Test
    public void shouldPerformEndOfDayOperationWithGatewaySession()
    {
        shouldPerformEndOfDayOperation(false);
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

        testSystem.awaitBlocking(() -> acceptingEngine.resetState(backupLocation));
        assertRecordingsDeleted();

        assertTrue("backupLocation missing", backupLocation.exists());
        assertTrue("backupLocation not directory", backupLocation.isDirectory());

        clearMessages();
        close();

        // resetState should be idempotent
        acceptingEngine.resetState(backupLocation);

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
        assertRecordingsTruncated();
    }

    private void assertRecordingsDeleted()
    {
        final File archiveDir = mediaDriver.archive().context().archiveDir();
        final File[] recordings = archiveDir.listFiles(file -> file.getName().endsWith(".rec"));
        final int numberOfRecordings = Objects.requireNonNull(recordings).length;
        assertEquals(4, numberOfRecordings);
    }

    private void assertRecordingsTruncated()
    {
        try (AeronArchive archive = AeronArchive.connect())
        {
            archive.listRecording(0,
                (controlSessionId,
                correlationId,
                recordingId,
                startTimestamp,
                stopTimestamp,
                startPosition,
                stopPosition,
                initialTermId,
                segmentFileLength,
                termBufferLength,
                mtuLength,
                sessionId,
                streamId,
                strippedChannel,
                originalChannel,
                sourceIdentity) ->
                {
                    assertEquals(0, stopPosition);
                });
        }
    }

    private void launch(
        final int initiatorInitialSentSequenceNumber,
        final int initialReceivedSequenceNumber)
    {
        final EngineConfiguration acceptingConfig = acceptingConfig(port, ACCEPTOR_ID, INITIATOR_ID);
        acceptingConfig.sessionPersistenceStrategy(logon -> INDEXED);
        acceptingEngine = FixEngine.launch(acceptingConfig);

        final EngineConfiguration initiatingConfig = initiatingConfig(libraryAeronPort);
        initiatingEngine = FixEngine.launch(initiatingConfig);

        final LibraryConfiguration acceptingLibraryConfig = acceptingLibraryConfig(acceptingHandler);
        acceptingLibrary = connect(acceptingLibraryConfig);

        final LibraryConfiguration initiatingLibraryConfig =
            initiatingLibraryConfig(libraryAeronPort, initiatingHandler);
        initiatingLibrary = connect(initiatingLibraryConfig);

        testSystem = new TestSystem(acceptingLibrary, initiatingLibrary);

        final Reply<Session> reply = connectPersistentSessions(
            initiatorInitialSentSequenceNumber, initialReceivedSequenceNumber, false);
        assertEquals("Repy failed: " + reply, Reply.State.COMPLETED, reply.state());
        initiatingSession = reply.resultIfPresent();
    }
}
