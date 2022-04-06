/*
 * Copyright 2015-2022 Real Logic Limited, Adaptive Financial Consulting Ltd.
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
package uk.co.real_logic.artio.engine.logger;

import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.IoUtil;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.collections.Long2ObjectCache;
import org.agrona.collections.LongHashSet;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.artio.LogTag;
import uk.co.real_logic.artio.messages.MessageHeaderDecoder;
import uk.co.real_logic.artio.storage.messages.ReplayIndexRecordDecoder;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.function.LongFunction;

import static io.aeron.Aeron.NULL_VALUE;
import static io.aeron.CommonContext.IPC_CHANNEL;
import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static org.agrona.UnsafeAccess.UNSAFE;
import static uk.co.real_logic.artio.engine.logger.ReplayIndexDescriptor.*;
import static uk.co.real_logic.artio.engine.logger.Replayer.MOST_RECENT_MESSAGE;

/**
 * Queries an index of a composite key of session id and sequence number.
 *
 * This object isn't thread-safe, but the underlying replay index is a single-writer, multiple-reader threadsafe index.
 */
public class ReplayQuery implements AutoCloseable
{
    private final MessageHeaderDecoder messageFrameHeader = new MessageHeaderDecoder();
    private final ReplayIndexRecordDecoder indexRecord = new ReplayIndexRecordDecoder();

    private final LongFunction<SessionQuery> newSessionQuery = SessionQuery::new;
    private final Long2ObjectCache<SessionQuery> fixSessionToIndex;
    private final String logFileDir;
    private final File logFileDirFile;
    private final ExistingBufferFactory indexBufferFactory;
    private final int requiredStreamId;
    private final IdleStrategy idleStrategy;
    private final AeronArchive aeronArchive;
    private final ErrorHandler errorHandler;
    private final int archiveReplayStream;
    private final int segmentSize;
    private final int segmentSizeBitShift;
    private final int segmentCount;
    private final long indexFileSize;

    private Subscription replaySubscription;

    public ReplayQuery(
        final String logFileDir,
        final int cacheNumSets,
        final int cacheSetSize,
        final ExistingBufferFactory indexBufferFactory,
        final int requiredStreamId,
        final IdleStrategy idleStrategy,
        final AeronArchive aeronArchive,
        final ErrorHandler errorHandler,
        final int archiveReplayStream,
        final int indexFileCapacity,
        final int indexSegmentCapacity)
    {
        this.logFileDir = logFileDir;
        this.indexBufferFactory = indexBufferFactory;
        this.requiredStreamId = requiredStreamId;
        this.idleStrategy = idleStrategy;
        this.aeronArchive = aeronArchive;
        this.errorHandler = errorHandler;
        this.archiveReplayStream = archiveReplayStream;

        this.indexFileSize = ReplayIndexDescriptor.capacityToBytes(indexFileCapacity);
        this.segmentSize = ReplayIndexDescriptor.capacityToBytesInt(indexSegmentCapacity);
        this.segmentSizeBitShift = Long.numberOfTrailingZeros(segmentSize);
        this.segmentCount = ReplayIndexDescriptor.segmentCount(indexFileCapacity, indexSegmentCapacity);

        logFileDirFile = new File(logFileDir);
        fixSessionToIndex = new Long2ObjectCache<>(cacheNumSets, cacheSetSize, SessionQuery::close);
    }

    /**
     *
     * @param sessionId the FIX session id of the stream to replay.
     * @param beginSequenceNumber sequence number to begin replay at (inclusive).
     * @param beginSequenceIndex the sequence index to begin replay at (inclusive).
     * @param endSequenceNumber sequence number to end replay at (inclusive).
     * @param endSequenceIndex the sequence index to end replay at (inclusive).
     * @param logTag the operation to tag log entries with
     * @param tracker the tracker to which messages are replayed
     * @return number of messages replayed
     */
    public ReplayOperation query(
        final long sessionId,
        final int beginSequenceNumber,
        final int beginSequenceIndex,
        final int endSequenceNumber,
        final int endSequenceIndex,
        final LogTag logTag,
        final MessageTracker tracker)
    {
        return lookupSessionQuery(sessionId)
            .query(beginSequenceNumber, beginSequenceIndex, endSequenceNumber, endSequenceIndex, logTag, tracker);
    }

    public void queryStartPositions(final Long2LongHashMap newStartPositions)
    {
        final LongHashSet allSessionIds = listReplayIndexSessionIds(logFileDirFile, requiredStreamId);

        // Run over existing session queries first in order to minimise cache evictions then reloads.
        for (final SessionQuery query : fixSessionToIndex.values())
        {
            aggregateLowerPosition(query.queryStartPositions(), newStartPositions);
            allSessionIds.remove(query.fixSessionId);
        }

        final LongHashSet.LongIterator sessionIdIt = allSessionIds.iterator();
        while (sessionIdIt.hasNext())
        {
            final long sessionId = sessionIdIt.nextValue();
            final SessionQuery query = lookupSessionQuery(sessionId);
            aggregateLowerPosition(query.queryStartPositions(), newStartPositions);
        }
    }

    private SessionQuery lookupSessionQuery(final long sessionId)
    {
        return fixSessionToIndex.computeIfAbsent(sessionId, newSessionQuery);
    }

    public void close()
    {
        fixSessionToIndex.clear();

        CloseHelper.close(replaySubscription);
    }

    private final class SessionQuery implements AutoCloseable
    {
        private final long fixSessionId;

        private final File headerFile;
        private final UnsafeBuffer headerBuffer;
        private final UnsafeBuffer[] segmentBuffers;

        private final int actingBlockLength;
        private final int actingVersion;

        SessionQuery(final long fixSessionId)
        {
            segmentBuffers = new UnsafeBuffer[segmentCount];
            headerFile = replayIndexHeaderFile(logFileDir, fixSessionId, requiredStreamId);
            headerBuffer = new UnsafeBuffer(indexBufferFactory.map(headerFile));
            this.fixSessionId = fixSessionId;

            messageFrameHeader.wrap(headerBuffer, 0);
            actingBlockLength = messageFrameHeader.blockLength();
            actingVersion = messageFrameHeader.version();
        }

        ReplayOperation query(
            final int beginSequenceNumber,
            final int beginSequenceIndex,
            final int endSequenceNumber,
            final int endSequenceIndex,
            final LogTag logTag,
            final MessageTracker messageTracker)
        {
            final UnsafeBuffer[] segmentBuffers = this.segmentBuffers;
            final int segmentSize = ReplayQuery.this.segmentSize;
            final int segmentSizeBitShift = ReplayQuery.this.segmentSizeBitShift;
            final ReplayIndexRecordDecoder indexRecord = ReplayQuery.this.indexRecord;
            final IdleStrategy idleStrategy = ReplayQuery.this.idleStrategy;
            final UnsafeBuffer headerBuffer = this.headerBuffer;
            final long indexFileSize = ReplayQuery.this.indexFileSize;
            final int actingBlockLength = this.actingBlockLength;
            final int actingVersion = this.actingVersion;

            final boolean upToMostRecentMessage = endSequenceNumber == MOST_RECENT_MESSAGE;

            // LOOKUP THE RANGE FROM THE INDEX
            // NB: this is a List as we are looking up recordings in the correct order to replay them.
            final List<RecordingRange> ranges = new ArrayList<>();
            RecordingRange currentRange = null;

            long iteratorPosition = getIteratorPosition();
            long stopIteratingPosition = iteratorPosition + indexFileSize;

            int lastSequenceNumber = -1;
            while (iteratorPosition < stopIteratingPosition)
            {
                final long changePosition = endChangeVolatile(headerBuffer);

                // Lapped by writer
                if (changePosition > iteratorPosition &&
                    (iteratorPosition + indexFileSize) <= beginChangeVolatile(headerBuffer))
                {
                    iteratorPosition = changePosition;
                    stopIteratingPosition = iteratorPosition + indexFileSize;
                }

                final UnsafeBuffer segmentBuffer = segmentBuffer(
                    iteratorPosition, segmentSizeBitShift, segmentBuffers, indexFileSize);
                final int offset = offsetInSegment(iteratorPosition, segmentSize);

                indexRecord.wrap(segmentBuffer, offset, actingBlockLength, actingVersion);
                final long beginPosition = indexRecord.position();
                final int sequenceIndex = indexRecord.sequenceIndex();
                final int sequenceNumber = indexRecord.sequenceNumber();
                final long recordingId = indexRecord.recordingId();
                final int readLength = indexRecord.length();

                UNSAFE.loadFence(); // LoadLoad required so previous loads don't move past version check below.

                // if the block was read atomically with no updates
                if (changePosition == beginChangeVolatile(headerBuffer))
                {
                    idleStrategy.reset();

                    final boolean afterEnd = !upToMostRecentMessage && (sequenceIndex > endSequenceIndex ||
                        (sequenceIndex == endSequenceIndex && sequenceNumber > endSequenceNumber));
                    if (beginPosition == 0 || afterEnd)
                    {
                        break;
                    }

                    final boolean withinQueryRange = sequenceIndex > beginSequenceIndex ||
                        (sequenceIndex == beginSequenceIndex && sequenceNumber >= beginSequenceNumber);
                    if (withinQueryRange)
                    {
                        currentRange = addRange(
                            ranges,
                            currentRange,
                            lastSequenceNumber,
                            beginPosition,
                            sequenceNumber,
                            recordingId,
                            readLength);
                        lastSequenceNumber = sequenceNumber;
                        iteratorPosition += RECORD_LENGTH;
                    }
                    else // before start of query
                    {
                        iteratorPosition = skipToStart(beginSequenceNumber, iteratorPosition, sequenceNumber);
                    }
                }
                else
                {
                    idleStrategy.idle();
                }
            }

            if (currentRange != null)
            {
                ranges.add(currentRange);
            }

            return newReplayOperation(ranges, logTag, messageTracker);
        }

        private UnsafeBuffer segmentBuffer(
            final long position,
            final int segmentSizeBitShift,
            final UnsafeBuffer[] segmentBuffers,
            final long indexFileSize)
        {
            final int segmentIndex = ReplayIndexDescriptor.segmentIndex(position, segmentSizeBitShift, indexFileSize);
            UnsafeBuffer segmentBuffer = segmentBuffers[segmentIndex];
            if (segmentBuffer == null)
            {
                final File file = replayIndexSegmentFile(logFileDir, fixSessionId, requiredStreamId, segmentIndex);
                segmentBuffer = new UnsafeBuffer(indexBufferFactory.map(file));
                segmentBuffers[segmentIndex] = segmentBuffer;
            }
            return segmentBuffer;
        }

        private long skipToStart(final int beginSequenceNumber, final long iteratorPosition, final int sequenceNumber)
        {
            if (sequenceNumber < beginSequenceNumber)
            {
                // Pre: sequenceIndex <= beginSequenceIndex
                return jumpPosition(beginSequenceNumber, sequenceNumber, iteratorPosition);
            }
            else
            {
                // Pre: sequenceIndex < beginSequenceIndex
                // Don't have a good way to estimate the jump, so just scan forward.
                return iteratorPosition + RECORD_LENGTH;
            }
        }

        private long jumpPosition(final int beginSequenceNumber, final int sequenceNumber, final long iteratorPosition)
        {
            final int sequenceNumberJump = beginSequenceNumber - sequenceNumber;
            final int jumpInBytes = sequenceNumberJump * RECORD_LENGTH;
            return iteratorPosition + jumpInBytes;
        }

        private ReplayOperation newReplayOperation(
            final List<RecordingRange> ranges, final LogTag logTag, final MessageTracker messageTracker)
        {
            if (replaySubscription == null)
            {
                replaySubscription = aeronArchive.context().aeron().addSubscription(
                    IPC_CHANNEL, archiveReplayStream);
            }

            return new ReplayOperation(
                ranges,
                aeronArchive,
                errorHandler,
                replaySubscription,
                archiveReplayStream,
                logTag,
                messageTracker);
        }

        private RecordingRange addRange(
            final List<RecordingRange> ranges,
            final RecordingRange currentRange,
            final int lastSequenceNumber,
            final long beginPosition,
            final int sequenceNumber,
            final long recordingId,
            final int readLength)
        {
            RecordingRange range = currentRange;
            if (range == null)
            {
                range = new RecordingRange(recordingId, fixSessionId);
            }
            else if (range.recordingId != recordingId)
            {
                ranges.add(range);
                range = new RecordingRange(recordingId, fixSessionId);
            }

            range.add(
                trueBeginPosition(beginPosition),
                readLength + FRAME_ALIGNMENT);

            // FIX messages can be fragmented, so number of range adds != count
            if (lastSequenceNumber != sequenceNumber)
            {
                range.count++;
            }
            return range;
        }

        private long getIteratorPosition()
        {
            // positions on a monotonically increasing scale
            long iteratorPosition = beginChangeVolatile(headerBuffer);
            // First iteration around you need to start at 0
            if (iteratorPosition < indexFileSize)
            {
                iteratorPosition = 0;
            }
            return iteratorPosition;
        }

        public Long2LongHashMap queryStartPositions()
        {
            final Long2LongHashMap recordingIdToStartPosition = new Long2LongHashMap(NULL_VALUE);

            // If we detect a delete-based-reset of this replay query from the replay indexer then clean ourselves up
            if (!headerFile.exists())
            {
                fixSessionToIndex.remove(fixSessionId);
                return recordingIdToStartPosition;
            }

            final UnsafeBuffer headerBuffer = this.headerBuffer;
            final long indexFileSize = ReplayQuery.this.indexFileSize;
            final ReplayIndexRecordDecoder indexRecord = ReplayQuery.this.indexRecord;
            final int actingBlockLength = this.actingBlockLength;
            final int actingVersion = this.actingVersion;
            final int segmentSizeBitShift = ReplayQuery.this.segmentSizeBitShift;
            final UnsafeBuffer[] segmentBuffers = this.segmentBuffers;
            final int segmentSize = ReplayQuery.this.segmentSize;
            final IdleStrategy idleStrategy = ReplayQuery.this.idleStrategy;

            long iteratorPosition = getIteratorPosition();
            long stopIteratingPosition = iteratorPosition + indexFileSize;

            int highestSequenceIndex = 0;

            while (iteratorPosition != stopIteratingPosition)
            {
                final long changePosition = endChangeVolatile(headerBuffer);

                // Lapped by writer
                if (changePosition > iteratorPosition &&
                    (iteratorPosition + indexFileSize) <= beginChangeVolatile(headerBuffer))
                {
                    iteratorPosition = changePosition;
                    stopIteratingPosition = iteratorPosition + indexFileSize;
                }

                final UnsafeBuffer segmentBuffer = segmentBuffer(
                    iteratorPosition, segmentSizeBitShift, segmentBuffers, indexFileSize);
                final int offset = offsetInSegment(iteratorPosition, segmentSize);

                indexRecord.wrap(segmentBuffer, offset, actingBlockLength, actingVersion);
                final long beginPosition = indexRecord.position();
                final int sequenceIndex = indexRecord.sequenceIndex();
                final long recordingId = indexRecord.recordingId();

                UNSAFE.loadFence(); // LoadLoad required so previous loads don't move past version check below.

                // if the block was read atomically with no updates
                if (changePosition == beginChangeVolatile(headerBuffer))
                {
                    idleStrategy.reset();

                    if (beginPosition == 0)
                    {
                        return recordingIdToStartPosition;
                    }

                    highestSequenceIndex = updateStartPosition(
                        sequenceIndex, highestSequenceIndex, recordingIdToStartPosition, recordingId, beginPosition);

                    iteratorPosition += RECORD_LENGTH;
                }
                else
                {
                    idleStrategy.idle();
                }
            }

            return recordingIdToStartPosition;
        }

        public void close()
        {
            IoUtil.unmap(headerBuffer.byteBuffer());
            for (final UnsafeBuffer segmentBuffer : segmentBuffers)
            {
                if (segmentBuffer != null)
                {
                    IoUtil.unmap(segmentBuffer.byteBuffer());
                }
            }
        }
    }

    static int updateStartPosition(
        final int sequenceIndex,
        final int highestSequenceIndex,
        final Long2LongHashMap recordingIdToStartPosition,
        final long recordingId,
        final long beginPosition)
    {
        if (sequenceIndex > highestSequenceIndex)
        {
            // Don't want the lower positions of a previous sequence index to matter.
            recordingIdToStartPosition.clear();
            recordingIdToStartPosition.put(recordingId, trueBeginPosition(beginPosition));
            // new highestSequenceIndex
            return sequenceIndex;
        }
        else if (sequenceIndex == highestSequenceIndex)
        {
            // Might have other messages on different recording ids
            final long oldPosition = recordingIdToStartPosition.get(recordingId);
            if (oldPosition == NULL_VALUE)
            {
                recordingIdToStartPosition.put(recordingId, trueBeginPosition(beginPosition));
            }
        }

        return highestSequenceIndex;
    }

    static long trueBeginPosition(final long beginPosition)
    {
        return beginPosition - FRAME_ALIGNMENT;
    }

    static void aggregateLowerPosition(
        final Long2LongHashMap recordingIdToStartPosition, final Long2LongHashMap newStartPositions)
    {
        final Long2LongHashMap.EntryIterator it = recordingIdToStartPosition.entrySet().iterator();
        while (it.hasNext())
        {
            it.next();

            final long recordingId = it.getLongKey();
            final long position = it.getLongValue();

            final long oldPosition = newStartPositions.get(recordingId);
            if (oldPosition == NULL_VALUE || position < oldPosition)
            {
                newStartPositions.put(recordingId, position);
            }
        }
    }

}
