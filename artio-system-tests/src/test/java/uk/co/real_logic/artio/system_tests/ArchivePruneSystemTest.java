package uk.co.real_logic.artio.system_tests;

import io.aeron.archive.client.AeronArchive;
import org.agrona.collections.Long2LongHashMap;
import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.artio.Reply;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.library.LibraryConfiguration;

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

        final EngineConfiguration acceptingConfig = acceptingConfig(port, ACCEPTOR_ID, INITIATOR_ID)
            .deleteLogFileDirOnStart(true);
        acceptingConfig.printErrorMessages(true);
        acceptingEngine = FixEngine.launch(acceptingConfig);

        initiatingEngine = launchInitiatingEngine(libraryAeronPort);

        final LibraryConfiguration acceptingLibraryConfig = acceptingLibraryConfig(acceptingHandler);
        acceptingLibrary = connect(acceptingLibraryConfig);
        initiatingLibrary = newInitiatingLibrary(libraryAeronPort, initiatingHandler);
        testSystem = new TestSystem(acceptingLibrary, initiatingLibrary);

        connectSessions();
    }

    @Test
    public void shouldPruneAwayOldArchivePositions()
    {
        acquireAcceptingSession();

        // Send > 1 segment of data through the stream.
        for (int i = 0; i < 500; i++)
        {
            messagesCanBeExchanged();
        }

        logoutAcceptingSession();
        assertSessionsDisconnected();

        connectSessions();
        acquireAcceptingSession();
        final long retainPosition = messagesCanBeExchanged(acceptingSession, acceptingOtfAcceptor);

        final AeronArchive.Context archiveContext = acceptingEngine.configuration().archiveContextClone();
        try (AeronArchive archive = AeronArchive.connect(archiveContext))
        {
            final Long2LongHashMap prePruneRecordingIdToStartPos = checkRecordings(archive);

            final Long2LongHashMap minimumPosition = new Long2LongHashMap(NULL_VALUE);
            final long notPrunedRecordingId = 1;
            minimumPosition.put(notPrunedRecordingId, 0);

            final Reply<Long2LongHashMap> pruneReply = acceptingEngine.pruneArchive(minimumPosition);
            assertNotNull(pruneReply);
            testSystem.awaitCompletedReplies(pruneReply);

            final Long2LongHashMap recordingIdToStartPos = pruneReply.resultIfPresent();
            final Long2LongHashMap prunedRecordingIdToStartPos = checkRecordings(archive);

            assertThat(recordingIdToStartPos, not(hasKey(notPrunedRecordingId)));

            for (final Map.Entry<Long, Long> entry : recordingIdToStartPos.entrySet())
            {
                final long recordingId = entry.getKey();
                final long startPosition = entry.getValue();

                assertThat(prePruneRecordingIdToStartPos.get(recordingId), lessThan(startPosition));
                assertEquals(prunedRecordingIdToStartPos.get(recordingId), recordingIdToStartPos.get(recordingId));
                assertThat(startPosition, lessThan(retainPosition));
            }
        }
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
