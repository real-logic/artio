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
import uk.co.real_logic.artio.Reply;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.FixEngine;

import java.util.Map;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.logbuffer.LogBufferDescriptor.TERM_MIN_LENGTH;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static uk.co.real_logic.artio.TestFixtures.launchMediaDriver;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.*;

public class ArchivePruneSystemTest extends AbstractGatewayToGatewaySystemTest
{

    @Before
    public void launch()
    {
        mediaDriver = launchMediaDriver(TERM_MIN_LENGTH);

        newAcceptingEngine(true);

        initiatingEngine = launchInitiatingEngine(libraryAeronPort);

        newAcceptingLibrary();
        initiatingLibrary = newInitiatingLibrary(libraryAeronPort, initiatingHandler);
        testSystem = new TestSystem(acceptingLibrary, initiatingLibrary);

        connectSessions();
    }

    private void newAcceptingLibrary()
    {
        acceptingLibrary = connect(acceptingLibraryConfig(acceptingHandler));
    }

    private void newAcceptingEngine(final boolean deleteLogFileDirOnStart)
    {
        final EngineConfiguration acceptingConfig = acceptingConfig(port, ACCEPTOR_ID, INITIATOR_ID)
            .deleteLogFileDirOnStart(deleteLogFileDirOnStart);
        acceptingConfig.printErrorMessages(true);
        acceptingEngine = FixEngine.launch(acceptingConfig);
    }

    @Test
    public void shouldPruneAwayOldArchivePositions()
    {
        acquireAcceptingSession();

        exchangeOverASegmentOfMessages();

        logoutAcceptingSession();
        assertSessionsDisconnected();

        connectSessions();
        acquireAcceptingSession();
        final long retainPosition = messagesCanBeExchanged(acceptingSession, acceptingOtfAcceptor);

        try (AeronArchive archive = newArchive())
        {
            final Long2LongHashMap prePruneRecordingIdToStartPos = checkRecordings(archive);

            final Long2LongHashMap minimumPosition = new Long2LongHashMap(NULL_VALUE);
            final long notPrunedRecordingId = 1;
            minimumPosition.put(notPrunedRecordingId, 0);

            final Long2LongHashMap recordingIdToStartPos = pruneArchive(minimumPosition);

            final Long2LongHashMap prunedRecordingIdToStartPos = checkRecordings(archive);

            assertThat(recordingIdToStartPos, not(hasKey(notPrunedRecordingId)));
            assertRecordingsPruned(
                retainPosition, prePruneRecordingIdToStartPos, recordingIdToStartPos, prunedRecordingIdToStartPos);

            messagesCanBeExchanged();

            // Restart engines to ensure that the positions can be continued after pruning.
            closeAcceptingEngine();
            closeAcceptingLibrary();

            newAcceptingEngine(false);
            newAcceptingLibrary();
            testSystem.add(acceptingLibrary);

            connectSessions();
            messagesCanBeExchanged();

            // Ensure that the recordings have been extended
            final Long2LongHashMap endRecordingIdToStartPos = checkRecordings(archive);
            assertEquals(prunedRecordingIdToStartPos, endRecordingIdToStartPos);
        }
    }

    private void exchangeOverASegmentOfMessages()
    {
        for (int i = 0; i < 500; i++)
        {
            messagesCanBeExchanged();
        }
    }

    private void assertRecordingsPruned(
        final long retainPosition,
        final Long2LongHashMap prePruneRecordingIdToStartPos,
        final Long2LongHashMap recordingIdToStartPos,
        final Long2LongHashMap prunedRecordingIdToStartPos)
    {
        for (final Map.Entry<Long, Long> entry : recordingIdToStartPos.entrySet())
        {
            final long recordingId = entry.getKey();
            final long startPosition = entry.getValue();

            assertThat(prePruneRecordingIdToStartPos.get(recordingId), lessThan(startPosition));
            assertEquals(prunedRecordingIdToStartPos.get(recordingId), recordingIdToStartPos.get(recordingId));
            assertThat(startPosition, lessThan(retainPosition));
        }
    }

    private Long2LongHashMap pruneArchive(final Long2LongHashMap minimumPosition)
    {
        final Reply<Long2LongHashMap> pruneReply = acceptingEngine.pruneArchive(minimumPosition);
        assertNotNull(pruneReply);
        testSystem.awaitCompletedReplies(pruneReply);

        return pruneReply.resultIfPresent();
    }

    private AeronArchive newArchive()
    {
        final AeronArchive.Context archiveContext = acceptingEngine.configuration().archiveContextClone();
        return AeronArchive.connect(archiveContext);
    }

    private Long2LongHashMap checkRecordings(final AeronArchive archive)
    {
        final Long2LongHashMap startPositions = new Long2LongHashMap(NULL_VALUE);
        archive.listRecordings(0, 100,
            (controlSessionId, correlationId, recordingId,
            startTimestamp, stopTimestamp, startPosition, stopPosition, initialTermId, segmentFileLength,
            termBufferLength, mtuLength, sessionId, streamId, strippedChannel, originalChannel, sourceIdentity) ->
            startPositions.put(recordingId, startPosition));
        return startPositions;
    }
}
