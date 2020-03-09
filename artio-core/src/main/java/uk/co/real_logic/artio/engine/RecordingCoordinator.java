/*
 * Copyright 2015-2020 Real Logic Limited, Adaptive Financial Consulting Ltd.
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
package uk.co.real_logic.artio.engine;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.archive.status.RecordingPos;
import org.agrona.IoUtil;
import org.agrona.collections.IntHashSet;
import org.agrona.collections.IntHashSet.IntIterator;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.collections.LongHashSet;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.CountersReader;
import uk.co.real_logic.artio.CommonConfiguration;
import uk.co.real_logic.artio.engine.logger.RecordingIdLookup;
import uk.co.real_logic.artio.storage.messages.MessageHeaderDecoder;
import uk.co.real_logic.artio.storage.messages.MessageHeaderEncoder;
import uk.co.real_logic.artio.storage.messages.PreviousRecordingDecoder;
import uk.co.real_logic.artio.storage.messages.PreviousRecordingDecoder.InboundRecordingsDecoder;
import uk.co.real_logic.artio.storage.messages.PreviousRecordingDecoder.OutboundRecordingsDecoder;
import uk.co.real_logic.artio.storage.messages.PreviousRecordingEncoder;
import uk.co.real_logic.artio.storage.messages.PreviousRecordingEncoder.InboundRecordingsEncoder;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static io.aeron.CommonContext.IPC_CHANNEL;
import static io.aeron.archive.codecs.SourceLocation.LOCAL;
import static io.aeron.archive.codecs.SourceLocation.REMOTE;
import static org.agrona.concurrent.status.CountersReader.NULL_COUNTER_ID;
import static uk.co.real_logic.artio.storage.messages.MessageHeaderDecoder.ENCODED_LENGTH;

/**
 * Not thread safe.
 */
public class RecordingCoordinator implements AutoCloseable
{
    private static final String FILE_NAME = "recording_coordinator";

    // Only used on startup and shutdown
    private final IdleStrategy idleStrategy = CommonConfiguration.backoffIdleStrategy();

    private final IntHashSet trackedSessionIds = new IntHashSet();
    private final AeronArchive archive;
    private final String channel;
    private final CountersReader counters;
    private final EngineConfiguration configuration;
    private final RecordingIdLookup inboundLookup;
    private final RecordingIdLookup outboundLookup;
    private final File recordingIdsFile;
    private final LongHashSet inboundRecordingIds = new LongHashSet();
    private final LongHashSet outboundRecordingIds = new LongHashSet();

    private Long2LongHashMap inboundAeronSessionIdToCompletionPosition;
    private Long2LongHashMap outboundAeronSessionIdToCompletionPosition;

    private boolean closed = false;

    RecordingCoordinator(
        final AeronArchive archive,
        final EngineConfiguration configuration,
        final IdleStrategy archiverIdleStrategy)
    {
        this.archive = archive;
        this.configuration = configuration;
        this.channel = configuration.libraryAeronChannel();

        recordingIdsFile = new File(configuration.logFileDir(), FILE_NAME);
        loadRecordingIdsFile();

        if (configuration.logAnyMessages())
        {
            final AeronArchive.Context archiveContext = archive.context();
            final Aeron aeron = archiveContext.aeron();
            counters = aeron.countersReader();
            inboundLookup = new RecordingIdLookup(archiverIdleStrategy, counters);
            outboundLookup = new RecordingIdLookup(archiverIdleStrategy, counters);

            if (configuration.logInboundMessages())
            {
                // Inbound we're writing from the Framer thread, always local
                startRecording(archive, configuration.inboundLibraryStream(), LOCAL);
            }

            if (configuration.logOutboundMessages())
            {
                // Outbound libraries might be on an IPC box.
                final SourceLocation location = channel.equals(IPC_CHANNEL) ? LOCAL : REMOTE;
                startRecording(archive, configuration.outboundLibraryStream(), location);
            }
        }
        else
        {
            counters = null;
            inboundLookup = null;
            outboundLookup = null;
        }
    }

    private void loadRecordingIdsFile()
    {
        if (recordingIdsFile.exists())
        {
            final MappedByteBuffer mappedBuffer = IoUtil.mapExistingFile(recordingIdsFile, FILE_NAME);
            final UnsafeBuffer buffer = new UnsafeBuffer(mappedBuffer);
            try
            {
                final MessageHeaderDecoder header = new MessageHeaderDecoder();
                final PreviousRecordingDecoder previousRecording = new PreviousRecordingDecoder();

                header.wrap(buffer, 0);
                previousRecording.wrap(buffer, ENCODED_LENGTH, header.blockLength(), header.version());

                for (InboundRecordingsDecoder inboundRecording : previousRecording.inboundRecordings())
                {
                    inboundRecordingIds.add(inboundRecording.recordingId());
                }

                for (OutboundRecordingsDecoder outboundRecording : previousRecording.outboundRecordings())
                {
                    outboundRecordingIds.add(outboundRecording.recordingId());
                }
            }
            finally
            {
                IoUtil.unmap(mappedBuffer);
            }
        }

        System.out.println("LOADED inboundRecordingIds = " + inboundRecordingIds);
        System.out.println("LOADED outboundRecordingIds = " + outboundRecordingIds);
    }

    private void saveRecordingIdsFile()
    {
        System.out.println("SAVING inboundRecordingIds = " + inboundRecordingIds);
        System.out.println("SAVING outboundRecordingIds = " + outboundRecordingIds);

        try
        {
            final File saveFile = File.createTempFile(FILE_NAME, "tmp");
            final MappedByteBuffer mappedBuffer = IoUtil.mapExistingFile(saveFile, FILE_NAME);
            final UnsafeBuffer buffer = new UnsafeBuffer(mappedBuffer);
            try
            {
                final MessageHeaderEncoder header = new MessageHeaderEncoder();
                final PreviousRecordingEncoder previousRecording = new PreviousRecordingEncoder();

                previousRecording.wrapAndApplyHeader(buffer, 0, header);

                final int inboundSize = inboundRecordingIds.size();
                final InboundRecordingsEncoder inbound = previousRecording.inboundRecordingsCount(inboundSize);
                final LongHashSet.LongIterator it = inboundRecordingIds.iterator();
                while (it.hasNext())
                {
                    inbound.next().recordingId(it.nextValue());
                }

                final int outboundSize = outboundRecordingIds.size();
                final InboundRecordingsEncoder outbound = previousRecording.inboundRecordingsCount(outboundSize);
                final LongHashSet.LongIterator outIt = outboundRecordingIds.iterator();
                while (outIt.hasNext())
                {
                    outbound.next().recordingId(outIt.nextValue());
                }
            }
            finally
            {
                mappedBuffer.force();
                IoUtil.unmap(mappedBuffer);
            }

            if (!saveFile.renameTo(recordingIdsFile))
            {
                // TODO
                System.err.println("RENAME FAIL");
            }
        }
        catch (final IOException e)
        {
            // TODO
            e.printStackTrace();
        }
    }

    private void startRecording(final AeronArchive archive, final int streamId, final SourceLocation location)
    {
        // If the Engine has been killed and thus not signalled to the archiver that its recording has been
        // Stopped we forcibly stop it here.
        try
        {
            archive.stopRecording(channel, streamId);

            if (configuration.printStartupWarnings())
            {
                System.err.printf(
                    "Warning: stopped currently running recording for streamId=%d channel=%s%n", streamId, channel);
            }
        }
        catch (final ArchiveException e)
        {
            // Deliberately blank - this is the normal case
        }

        archive.startRecording(channel, streamId, location);
    }

    // Only called on single threaded engine startup
    public void track(final Publication publication)
    {
        final int streamId = publication.streamId();
        if (configuration.isRelevantStreamId(streamId))
        {
            trackedSessionIds.add(publication.sessionId());
        }
    }

    // Only called on single threaded engine startup
    void awaitReady()
    {
        while (!trackedSessionIds.isEmpty())
        {
            final IntIterator sessionIdIterator = trackedSessionIds.iterator();
            while (sessionIdIterator.hasNext())
            {
                final int sessionId = sessionIdIterator.nextValue();
                if (hasRecordingStarted(sessionId))
                {
                    sessionIdIterator.remove();
                }
            }

            idleStrategy.idle();
        }
        idleStrategy.reset();
    }

    // Called only on Framer.quiesce(), uses shutdown order
    public void completionPositions(
        final Long2LongHashMap inboundAeronSessionIdToCompletionPosition,
        final Long2LongHashMap outboundAeronSessionIdToCompletionPosition)
    {
        this.inboundAeronSessionIdToCompletionPosition = inboundAeronSessionIdToCompletionPosition;
        this.outboundAeronSessionIdToCompletionPosition = outboundAeronSessionIdToCompletionPosition;
    }

    // Must be called after the framer has shutdown, uses shutdown order
    public void close()
    {
        if (configuration.gracefulShutdown())
        {
            if (!closed)
            {
                awaitRecordingsCompletion();
                shutdownArchiver();
                closed = true;
            }
        }
    }

    private void awaitRecordingsCompletion()
    {
        if (configuration.logInboundMessages())
        {
            awaitRecordingsCompletion(inboundAeronSessionIdToCompletionPosition, inboundRecordingIds);
        }

        if (configuration.logOutboundMessages())
        {
            awaitRecordingsCompletion(outboundAeronSessionIdToCompletionPosition, outboundRecordingIds);
        }

        saveRecordingIdsFile();
    }

    private void awaitRecordingsCompletion(
        final Long2LongHashMap aeronSessionIdToCompletionPosition, final LongHashSet recordingIds)
    {
        if (aeronSessionIdToCompletionPosition == null)
        {
            throw new IllegalStateException(
                "Unknown completionPositions when shutting down the RecordingCoordinator");
        }

        recordingIds.clear();

        final List<CompletingRecording> completingRecordings = new ArrayList<>();
        aeronSessionIdToCompletionPosition.longForEach((sessionId, completionPosition) ->
        {
            final int counterId = RecordingPos.findCounterIdBySession(counters, (int)sessionId);
            // Recording has completed
            if (counterId != NULL_COUNTER_ID)
            {
                final CompletingRecording recording = new CompletingRecording(completionPosition, counterId);
                recordingIds.add(recording.recordingId);
                completingRecordings.add(recording);
            }
        });

        while (!completingRecordings.isEmpty())
        {
            completingRecordings.removeIf(CompletingRecording::hasRecordingCompleted);

            idleStrategy.idle();
        }
        idleStrategy.reset();
    }

    private void shutdownArchiver()
    {
        if (configuration.logInboundMessages())
        {
            archive.stopRecording(channel, configuration.inboundLibraryStream());
        }

        if (configuration.logOutboundMessages())
        {
            archive.stopRecording(channel, configuration.outboundLibraryStream());
        }

        if (configuration.logAnyMessages())
        {
            archive.close();
        }
    }

    private boolean hasRecordingStarted(final int sessionId)
    {
        return RecordingPos.findCounterIdBySession(counters, sessionId) != NULL_COUNTER_ID;
    }

    private class CompletingRecording
    {
        private final long completedPosition;
        private final long recordingId;
        private final int counterId;

        CompletingRecording(final long completedPosition, final int counterId)
        {
            this.completedPosition = completedPosition;

            this.counterId = counterId;
            recordingId = RecordingPos.getRecordingId(counters, this.counterId);
        }

        boolean hasRecordingCompleted()
        {
            final long recordedPosition = counters.getCounterValue(counterId);
            if (recordedPosition >= completedPosition)
            {
                return true;
            }

            if (!RecordingPos.isActive(counters, counterId, recordingId))
            {
                throw new IllegalStateException("recording has stopped unexpectedly: " + recordingId);
            }

            return false;
        }
    }

    RecordingIdLookup inboundRecordingIdLookup()
    {
        return inboundLookup;
    }

    RecordingIdLookup outboundRecordingIdLookup()
    {
        return outboundLookup;
    }

}
