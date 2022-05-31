/*
 * Copyright 2020 Adaptive Financial Consulting Ltd.
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
import org.agrona.collections.Long2LongHashMap;
import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.artio.DebugLogger;
import uk.co.real_logic.artio.LogTag;
import uk.co.real_logic.artio.MonitoringAgentFactory;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.engine.RecordingCoordinator;
import uk.co.real_logic.artio.engine.SessionInfo;

import java.io.File;
import java.util.Map;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.logbuffer.LogBufferDescriptor.TERM_MIN_LENGTH;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static uk.co.real_logic.artio.FixMatchers.hasSequenceIndex;
import static uk.co.real_logic.artio.TestFixtures.launchMediaDriver;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.*;

public class ArchivePruneSystemTest extends AbstractGatewayToGatewaySystemTest
{
    private boolean saveOnShutdownTesting = true;

    @Before
    public void launch()
    {
        deleteLogs();

        mediaDriver = launchMediaDriver(TERM_MIN_LENGTH);

        newAcceptingEngine(true);

        initiatingEngine = launchInitiatingEngine(libraryAeronPort, nanoClock);

        newAcceptingLibrary();
        initiatingLibrary = newInitiatingLibrary(libraryAeronPort, initiatingHandler, nanoClock);
        testSystem = new TestSystem(acceptingLibrary, initiatingLibrary);

        connectSessions();
    }

    private void newAcceptingLibrary()
    {
        acceptingLibrary = connect(acceptingLibraryConfig(acceptingHandler, nanoClock));
    }

    private void newAcceptingEngine(final boolean deleteLogFileDirOnStart)
    {
        final EngineConfiguration acceptingConfig = acceptingConfig(port, ACCEPTOR_ID, INITIATOR_ID, nanoClock)
            .deleteLogFileDirOnStart(deleteLogFileDirOnStart);
        acceptingConfig.monitoringAgentFactory(MonitoringAgentFactory.none());
        acceptingEngine = FixEngine.launch(acceptingConfig);
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldPruneAwayOldArchivePositions()
    {
        setupSessionWithSegmentOfFiles();

        resetSequenceNumberWithNewLogon();

        assertPruneWorks(false, true);
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldPruneAwayOldArchivePositionsWithFreeLibraryIds()
    {
        setupSessionWithSegmentOfFiles();

        // Combine a FixEngine.resetSequenceNumber with disconnecting and then reconnecting the library to
        // Ensure that we test a free recording id case.
        resetSequenceWithResetSequenceNumber();

        closeAcceptingLibrary();
        newAcceptingLibrary();

        closeAcceptingEngine();
        closeAcceptingLibrary();

        newAcceptingEngine(false);

        assertPruneWorks(false, false);
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldPruneAwayOldArchivePositionsForFixEngineResetSequenceNumbers()
    {
        setupSessionWithSegmentOfFiles();
        resetSequenceWithResetSequenceNumber();
        assertEngineSequenceIndexBecomes(1);

        assertPruneWorks(true, true);
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldPruneAwayOldArchivePositionsForSessionTrySendSequenceReset()
    {
        setupSessionWithSegmentOfFiles();
        final long sessionId = acceptingSession.id();
        acceptingSession = null;

        acquireAcceptingSession();
        assertOfflineSession(sessionId, acceptingSession);

        testSystem.awaitSend("failed to trySendSequenceReset", () ->
            acceptingSession.trySendSequenceReset(1, 1));

        assertThat(acceptingSession, hasSequenceIndex(1));
        assertEngineSequenceIndexBecomes(1);

        assertPruneWorks(true, true);
    }

    private void assertEngineSequenceIndexBecomes(final int sequenceIndex)
    {
        final SessionInfo sessionInfo = acceptingEngine.allSessions().get(0);
        testSystem.await("Engine failed to update sequence index",
            () -> sessionInfo.sequenceIndex() == sequenceIndex);
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldPruneAwayOldArchivePositionsForSessionTryResetSequenceNumbers()
    {
        acquireAcceptingSession();

        exchangeOverASegmentOfMessages();

        testSystem.awaitSend("failed to trySendSequenceReset", () -> acceptingSession.tryResetSequenceNumbers());

        testSystem.await("Failed to received logon in reply", () ->
            acceptingSession.lastReceivedMsgSeqNum() == 1);

        assertAcceptingSessionHasSequenceIndex(1);
        assertInitiatingSequenceIndexIs(1);

        final File file = RecordingCoordinator.recordingIdsFile(acceptingEngine.configuration());
        assertTrue("Failed to create recording coordinator file", file.exists());

        assertPruneWorks(false, true);
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldPruneAwayOldArchivePositionsForSessionTryResetSequenceNumbersProcessTerminated()
    {
        RecordingCoordinator.saveOnShutdownTesting(false);
        saveOnShutdownTesting = false;
        try
        {
            shouldPruneAwayOldArchivePositionsForSessionTryResetSequenceNumbers();
        }
        finally
        {
            RecordingCoordinator.saveOnShutdownTesting(true);
        }
    }

    private void assertPruneWorks(final boolean reconnectSession, final boolean hasConnectedLibrary)
    {
        try (AeronArchive archive = newArchive(acceptingEngine))
        {
            final Long2LongHashMap prePruneRecordingIdToStartPos = getRecordingStartPos(archive);

            final Long2LongHashMap minimumPosition = new Long2LongHashMap(NULL_VALUE);
            // Specify a recordingId to not prune in order to test that the archive takes account of the
            // minimumPosition parameter.
            final long notPrunedRecordingId = 1;
            minimumPosition.put(notPrunedRecordingId, 0);

            final Long2LongHashMap recordingIdToStartPos = testSystem.pruneArchive(minimumPosition, acceptingEngine);
            final Long2LongHashMap prunedRecordingIdToStartPos = getRecordingStartPos(archive);

            DebugLogger.log(LogTag.STATE_CLEANUP,
                "prePruneRecordingIdToStartPos = " + prePruneRecordingIdToStartPos +
                ", prunedRecordingIdToStartPos = " + prunedRecordingIdToStartPos +
                ", recordingIdToStartPos = " + recordingIdToStartPos);

            assertThat(recordingIdToStartPos.toString(), recordingIdToStartPos, not(hasKey(notPrunedRecordingId)));
            assertThat(recordingIdToStartPos.toString(), recordingIdToStartPos, hasKey(0L));
            assertThat(recordingIdToStartPos.toString(), recordingIdToStartPos, hasKey(4L));

            assertRecordingsPruned(
                prePruneRecordingIdToStartPos, recordingIdToStartPos, prunedRecordingIdToStartPos);

            if (hasConnectedLibrary)
            {
                if (reconnectSession)
                {
                    connectSessions();
                }
                messagesCanBeExchanged();
            }

            // Restart engines to ensure that the positions can be continued after pruning.
            closeAcceptingEngine();
            closeAcceptingLibrary();

            final File file = RecordingCoordinator.recordingIdsFile(acceptingEngine.configuration());
            assertTrue("Failed to create recording coordinator file", file.exists());

            newAcceptingEngine(false);
            newAcceptingLibrary();
            testSystem.add(acceptingLibrary);

            connectSessions();
            messagesCanBeExchanged();

            // Ensure that the recordings have been extended
            final Long2LongHashMap endRecordingIdToStartPos = getRecordingStartPos(archive);

            if (!saveOnShutdownTesting)
            {
                // If we didn't save the file it's reasonable to continue to operate the prune but have extra
                // unknown recordings
                endRecordingIdToStartPos.keySet().retainAll(prunedRecordingIdToStartPos.keySet());
            }

            assertEquals(prunedRecordingIdToStartPos, endRecordingIdToStartPos);
        }
    }

    private void setupSessionWithSegmentOfFiles()
    {
        acquireAcceptingSession();

        exchangeOverASegmentOfMessages();

        logoutAcceptingSession();
        assertSessionsDisconnected();
    }

    private void exchangeOverASegmentOfMessages()
    {
        for (int i = 0; i < 500; i++)
        {
            messagesCanBeExchanged();
        }
    }

    public static void assertRecordingsPruned(
        final Long2LongHashMap prePruneRecordingIdToStartPos,
        final Long2LongHashMap recordingIdToStartPos,
        final Long2LongHashMap prunedRecordingIdToStartPos)
    {
        final Long2LongHashMap.EntrySet entries = recordingIdToStartPos.entrySet();
        assertThat(entries, not(empty()));

        for (final Map.Entry<Long, Long> entry : entries)
        {
            final long recordingId = entry.getKey();
            final long startPosition = entry.getValue();

            assertThat(prePruneRecordingIdToStartPos.get(recordingId), lessThan(startPosition));
            assertEquals(prunedRecordingIdToStartPos.get(recordingId), recordingIdToStartPos.get(recordingId));
        }
    }

    public static AeronArchive newArchive(final FixEngine engine)
    {
        final AeronArchive.Context archiveContext = engine.configuration().archiveContextClone();
        return AeronArchive.connect(archiveContext);
    }

    public static Long2LongHashMap getRecordingStartPos(final AeronArchive archive)
    {
        final Long2LongHashMap startPositions = new Long2LongHashMap(NULL_VALUE);
        archive.listRecordings(0, 100,
            (controlSessionId, correlationId, recordingId,
            startTimestamp, stopTimestamp, startPosition, stopPosition, initialTermId, segmentFileLength,
            termBufferLength, mtuLength, sessionId, streamId, strippedChannel, originalChannel, sourceIdentity) ->
            startPositions.put(recordingId, startPosition));
        return startPositions;
    }

    private void resetSequenceNumberWithNewLogon()
    {
        connectSessions();
        acquireAcceptingSession();
        messagesCanBeExchanged(acceptingSession, acceptingOtfAcceptor);
    }

    private void resetSequenceWithResetSequenceNumber()
    {
        final long sessionId = acceptingSession.id();
        acceptingSession = null;

        testSystem.resetSequenceNumber(acceptingEngine, sessionId);
    }
}
