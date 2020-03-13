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
import io.aeron.ChannelUri;
import io.aeron.ExclusivePublication;
import io.aeron.archive.client.AeronArchive;
import io.aeron.archive.client.ArchiveException;
import io.aeron.archive.client.RecordingDescriptorConsumer;
import io.aeron.archive.codecs.SourceLocation;
import io.aeron.archive.status.RecordingPos;
import org.agrona.ErrorHandler;
import org.agrona.IoUtil;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.collections.Long2ObjectHashMap;
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
import uk.co.real_logic.artio.storage.messages.PreviousRecordingEncoder.OutboundRecordingsEncoder;

import java.io.File;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.LongConsumer;

import static io.aeron.CommonContext.IPC_CHANNEL;
import static io.aeron.CommonContext.MTU_LENGTH_PARAM_NAME;
import static io.aeron.archive.codecs.SourceLocation.LOCAL;
import static io.aeron.archive.codecs.SourceLocation.REMOTE;
import static org.agrona.concurrent.status.CountersReader.NULL_COUNTER_ID;
import static uk.co.real_logic.artio.storage.messages.MessageHeaderDecoder.ENCODED_LENGTH;

/**
 * Not thread safe.
 */
public class RecordingCoordinator implements AutoCloseable, RecordingDescriptorConsumer
{
    private static final String FILE_NAME = "recording_coordinator";

    // Only used on startup and shutdown
    private final IdleStrategy idleStrategy = CommonConfiguration.backoffIdleStrategy();

    private final SourceLocation outboundLocation;
    private final LongHashSet trackedRegistrationIds = new LongHashSet();
    private final Aeron aeron;
    private final AeronArchive archive;
    private final String channel;
    private final CountersReader counters;
    private final EngineConfiguration configuration;
    private final RecordingIdLookup inboundLookup;
    private final RecordingIdLookup outboundLookup;
    private final File recordingIdsFile;
    private final ErrorHandler errorHandler;

    private final RecordingIds inboundRecordingIds = new RecordingIds();
    private final RecordingIds outboundRecordingIds = new RecordingIds();
    private final Long2ObjectHashMap<LibraryExtendPosition> libraryIdToExtendPosition = new Long2ObjectHashMap<>();

    private Long2LongHashMap inboundAeronSessionIdToCompletionPosition;
    private Long2LongHashMap outboundAeronSessionIdToCompletionPosition;

    private boolean closed = false;

    private LibraryExtendPosition libraryExtendPosition;

    RecordingCoordinator(
        final Aeron aeron,
        final AeronArchive archive,
        final EngineConfiguration configuration,
        final IdleStrategy archiverIdleStrategy,
        final ErrorHandler errorHandler)
    {
        this.aeron = aeron;
        this.archive = archive;
        this.configuration = configuration;
        this.channel = configuration.libraryAeronChannel();

        recordingIdsFile = new File(configuration.logFileDir(), FILE_NAME);
        this.errorHandler = errorHandler;
        outboundLocation = channel.equals(IPC_CHANNEL) ? LOCAL : REMOTE;
        loadRecordingIdsFile();

        if (configuration.logAnyMessages())
        {
            final AeronArchive.Context archiveContext = archive.context();
            counters = this.aeron.countersReader();
            inboundLookup = new RecordingIdLookup(archiverIdleStrategy, counters);
            outboundLookup = new RecordingIdLookup(archiverIdleStrategy, counters);
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

                for (final InboundRecordingsDecoder inboundRecording : previousRecording.inboundRecordings())
                {
                    inboundRecordingIds.free.add(inboundRecording.recordingId());
                }

                for (final OutboundRecordingsDecoder outboundRecording : previousRecording.outboundRecordings())
                {
                    outboundRecordingIds.free.add(outboundRecording.recordingId());
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
        libraryIdToExtendPosition.values().forEach(pos -> outboundRecordingIds.free.add(pos.recordingId));

        System.out.println("SAVING inboundRecordingIds = " + inboundRecordingIds);
        System.out.println("SAVING outboundRecordingIds = " + outboundRecordingIds);

        try
        {
            final int inboundSize = inboundRecordingIds.size();
            final int outboundSize = outboundRecordingIds.size();

            final File saveFile = File.createTempFile(FILE_NAME, "tmp");
            final int requiredLength = MessageHeaderEncoder.ENCODED_LENGTH + PreviousRecordingEncoder.BLOCK_LENGTH +
                InboundRecordingsEncoder.HEADER_SIZE + OutboundRecordingsEncoder.HEADER_SIZE +
                InboundRecordingsEncoder.recordingIdEncodingLength() * inboundSize +
                OutboundRecordingsEncoder.recordingIdEncodingLength() * outboundSize;
            final MappedByteBuffer mappedBuffer = IoUtil.mapExistingFile(saveFile, FILE_NAME, 0, requiredLength);
            final UnsafeBuffer buffer = new UnsafeBuffer(mappedBuffer);
            try
            {
                final MessageHeaderEncoder header = new MessageHeaderEncoder();
                final PreviousRecordingEncoder previousRecording = new PreviousRecordingEncoder();

                previousRecording.wrapAndApplyHeader(buffer, 0, header);

                final InboundRecordingsEncoder inbound = previousRecording.inboundRecordingsCount(inboundSize);
                inboundRecordingIds.forEach(id -> inbound.next().recordingId(id));

                final OutboundRecordingsEncoder outbound = previousRecording.outboundRecordingsCount(outboundSize);
                outboundRecordingIds.forEach(id -> outbound.next().recordingId(id));

                mappedBuffer.force();
            }
            finally
            {
                IoUtil.unmap(mappedBuffer);
            }

            if (!saveFile.renameTo(recordingIdsFile))
            {
                errorHandler.onError(new IllegalStateException("Unable to rename temporary recordingIdsFile " +
                    saveFile + " to " + recordingIdsFile));
            }
        }
        catch (final Throwable e)
        {
            errorHandler.onError(e);
        }
    }

    // Only called on single threaded engine startup
    public ExclusivePublication track(final String aeronChannel, final int streamId)
    {
        if (configuration.isRelevantStreamId(streamId))
        {
            final RecordingIds recordingIds = streamId == configuration.inboundLibraryStream() ?
                inboundRecordingIds : outboundRecordingIds;
            final LibraryExtendPosition libraryExtendPosition = acquireRecording(recordingIds);
            final ExclusivePublication publication;
            if (libraryExtendPosition != null)
            {
                final String channel = createExtendedChannel(aeronChannel,
                    libraryExtendPosition.stopPosition,
                    libraryExtendPosition.initialTermId,
                    libraryExtendPosition.termBufferLength,
                    libraryExtendPosition.mtuLength);
                publication = aeron.addExclusivePublication(channel, streamId);
                extendRecording(streamId, libraryExtendPosition, publication.sessionId());
            }
            else
            {
                publication = aeron.addExclusivePublication(aeronChannel, streamId);
                startRecording(streamId, publication.sessionId(), LOCAL);
            }

            awaitRecordingStart(publication.sessionId(), idleStrategy, recordingIds.used);

            return publication;
        }
        else
        {
            return aeron.addExclusivePublication(aeronChannel, streamId);
        }
    }

    public static String createExtendedChannel(
        final String aeronChannel,
        final long stopPosition,
        final int initialTermId,
        final int termBufferLength,
        final int mtuLength)
    {
        final ChannelUri channelUri = ChannelUri.parse(aeronChannel);
        channelUri.initialPosition(
            stopPosition,
            initialTermId,
            termBufferLength);
        channelUri.put(MTU_LENGTH_PARAM_NAME, Integer.toString(mtuLength));
        return channelUri.toString();
    }

    private void extendRecording(
        final int streamId, final LibraryExtendPosition libraryExtendPosition, final int sessionId)
    {
        try
        {
            final String recordingChannel = ChannelUri.addSessionId(channel, sessionId);
            final long registrationId = archive.extendRecording(
                libraryExtendPosition.recordingId, recordingChannel, streamId, LOCAL);
            trackedRegistrationIds.add(registrationId);
        }
        catch (final ArchiveException e)
        {
            errorHandler.onError(e);
        }
    }

    private LibraryExtendPosition acquireRecording(final RecordingIds recordingIds)
    {
        libraryExtendPosition = null;

        final LongHashSet.LongIterator it = recordingIds.free.iterator();
        if (it.hasNext())
        {
            final long recordingId = it.nextValue();
            it.remove();

            final int count = archive.listRecording(recordingId, this);
            if (count != 1)
            {
                System.err.println("ERR");
            }
        }

        return libraryExtendPosition;
    }

    // Called from Framer thread.
    public LibraryExtendPosition trackLibrary(final int sessionId, final int libraryId)
    {
        if (configuration.logOutboundMessages())
        {
            final int streamId = configuration.outboundLibraryStream();
            final IdleStrategy idleStrategy = configuration.framerIdleStrategy();

            LibraryExtendPosition extendPosition = libraryIdToExtendPosition.remove(libraryId);
            if (extendPosition != null)
            {
                // Library has moved to a previously configured extend position
                extendRecording(streamId, extendPosition, sessionId);
                System.out.println("acknowledge extend recording");
                return null;
            }

            extendPosition = acquireRecording(outboundRecordingIds);
            if (extendPosition != null)
            {
                // Library needs to be informed of its extend position.
                libraryIdToExtendPosition.put(libraryId, extendPosition);
                System.out.println("Setup extend recording");
                return extendPosition;
            }
            else
            {
                if (startRecording(streamId, sessionId, outboundLocation))
                {
                    awaitRecordingStart(sessionId, idleStrategy, outboundRecordingIds.used);
                }
            }
        }

        return null;
    }

    public void onRecordingDescriptor(final long controlSessionId, final long correlationId, final long recordingId,
                                      final long startTimestamp, final long stopTimestamp, final long startPosition,
                                      final long stopPosition, final int initialTermId, final int segmentFileLength,
                                      final int termBufferLength, final int mtuLength, final int sessionId,
                                      final int streamId, final String strippedChannel, final String originalChannel,
                                      final String sourceIdentity)
    {
        this.libraryExtendPosition = new LibraryExtendPosition(
            recordingId, stopPosition, initialTermId, termBufferLength, mtuLength);
    }

    private boolean startRecording(
        final int streamId,
        final int sessionId,
        final SourceLocation local)
    {
        try
        {
            final String channel = ChannelUri.addSessionId(this.channel, sessionId);
            final long registrationId = archive.startRecording(channel, streamId, local);
            trackedRegistrationIds.add(registrationId);

            return true;
        }
        catch (final ArchiveException e)
        {
            // Perfectly reasonable error if we're already recording the session.
            if (!e.getMessage().contains("recording exists for"))
            {
                throw e;
            }

            return false;
        }
    }

    private void awaitRecordingStart(
        final int sessionId, final IdleStrategy idleStrategy, final LongHashSet recordingIds)
    {
        int counterId;
        do
        {
            counterId = RecordingPos.findCounterIdBySession(counters, sessionId);
            idleStrategy.idle();
        }
        while (counterId == NULL_COUNTER_ID);
        idleStrategy.reset();

        final long recordingId = RecordingPos.getRecordingId(counters, counterId);
        recordingIds.add(recordingId);
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
            awaitRecordingsCompletion(inboundAeronSessionIdToCompletionPosition);
        }

        if (configuration.logOutboundMessages())
        {
            awaitRecordingsCompletion(outboundAeronSessionIdToCompletionPosition);
        }

        saveRecordingIdsFile();
    }

    private void awaitRecordingsCompletion(
        final Long2LongHashMap aeronSessionIdToCompletionPosition)
    {
        if (aeronSessionIdToCompletionPosition == null)
        {
            throw new IllegalStateException(
                "Unknown completionPositions when shutting down the RecordingCoordinator");
        }

        final List<CompletingRecording> completingRecordings = new ArrayList<>();
        aeronSessionIdToCompletionPosition.longForEach((sessionId, completionPosition) ->
        {
            final int counterId = RecordingPos.findCounterIdBySession(counters, (int)sessionId);
            // Recording has completed
            if (counterId != NULL_COUNTER_ID)
            {
                final CompletingRecording recording = new CompletingRecording(completionPosition, counterId);
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
        final LongHashSet.LongIterator it = trackedRegistrationIds.iterator();
        while (it.hasNext())
        {
            final long registrationId = it.nextValue();
            archive.stopRecording(registrationId);
        }

        if (configuration.logAnyMessages())
        {
            archive.close();
        }
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

    static final class RecordingIds
    {
        private LongHashSet free = new LongHashSet();
        private LongHashSet used = new LongHashSet();

        int size()
        {
            return free.size() + used.size();
        }

        void forEach(final LongConsumer recordingIdConsumer)
        {
            forEach(free, recordingIdConsumer);
            forEach(used, recordingIdConsumer);
        }

        private void forEach(final LongHashSet set, final LongConsumer recordingIdConsumer)
        {
            final LongHashSet.LongIterator it = set.iterator();
            while (it.hasNext())
            {
                recordingIdConsumer.accept(it.nextValue());
            }
        }

        public String toString()
        {
            return "RecordingIds{" +
                "free=" + free +
                ", used=" + used +
                '}';
        }
    }

    public static class LibraryExtendPosition
    {
        public final long recordingId;
        public final long stopPosition;
        public final int initialTermId;
        public final int termBufferLength;
        public final int mtuLength;

        LibraryExtendPosition(
            final long recordingId,
            final long stopPosition,
            final int initialTermId,
            final int termBufferLength,
            final int mtuLength)
        {
            this.recordingId = recordingId;
            this.stopPosition = stopPosition;
            this.initialTermId = initialTermId;
            this.termBufferLength = termBufferLength;
            this.mtuLength = mtuLength;
        }
    }

}
