/*
 * Copyright 2015-2017 Real Logic Ltd.
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
package uk.co.real_logic.artio.engine.logger;

import io.aeron.ControlledFragmentAssembler;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.IoUtil;
import org.agrona.collections.Long2ObjectCache;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.artio.DebugLogger;
import uk.co.real_logic.artio.LogTag;
import uk.co.real_logic.artio.messages.FixMessageDecoder;
import uk.co.real_logic.artio.messages.MessageHeaderDecoder;
import uk.co.real_logic.artio.storage.messages.ReplayIndexRecordDecoder;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.function.LongFunction;

import static io.aeron.CommonContext.IPC_CHANNEL;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;
import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;
import static org.agrona.UnsafeAccess.UNSAFE;
import static uk.co.real_logic.artio.GatewayProcess.ARCHIVE_REPLAY_STREAM;
import static uk.co.real_logic.artio.dictionary.generation.CodecUtil.MISSING_LONG;
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
    private final ExistingBufferFactory indexBufferFactory;
    private final int requiredStreamId;
    private final IdleStrategy idleStrategy;
    private final AeronArchive aeronArchive;
    private final String channel;
    private final ErrorHandler errorHandler;
    private final RecordingBarrier recordingBarrier;

    private final MessageTracker messageTracker = new MessageTracker();
    private final ControlledFragmentAssembler assembler = new ControlledFragmentAssembler(messageTracker);

    public ReplayQuery(
        final String logFileDir,
        final int cacheNumSets,
        final int cacheSetSize,
        final ExistingBufferFactory indexBufferFactory,
        final int requiredStreamId,
        final IdleStrategy idleStrategy,
        final AeronArchive aeronArchive,
        final String channel,
        final ErrorHandler errorHandler,
        final RecordingBarrier recordingBarrier)
    {
        this.logFileDir = logFileDir;
        this.indexBufferFactory = indexBufferFactory;
        this.requiredStreamId = requiredStreamId;
        this.idleStrategy = idleStrategy;
        this.aeronArchive = aeronArchive;
        this.channel = channel;
        this.errorHandler = errorHandler;
        this.recordingBarrier = recordingBarrier;

        fixSessionToIndex = new Long2ObjectCache<>(cacheNumSets, cacheSetSize, SessionQuery::close);
    }

    /**
     *
     * @param handler the handler to pass the messages to
     * @param sessionId the FIX session id of the stream to replay.
     * @param beginSequenceNumber sequence number to begin replay at (inclusive).
     * @param endSequenceNumber sequence number to end replay at (inclusive).
     * @return number of messages replayed
     */
    public int query(
        final ControlledFragmentHandler handler,
        final long sessionId,
        final int beginSequenceNumber,
        final int beginSequenceIndex,
        final int endSequenceNumber,
        final int endSequenceIndex)
    {
        return fixSessionToIndex
            .computeIfAbsent(sessionId, newSessionQuery)
            .query(handler, beginSequenceNumber, beginSequenceIndex, endSequenceNumber, endSequenceIndex);
    }

    public void close()
    {
        fixSessionToIndex.clear();
    }

    private final class SessionQuery implements AutoCloseable
    {
        private final ByteBuffer wrappedBuffer;
        private final UnsafeBuffer buffer;
        private final int capacity;

        SessionQuery(final long sessionId)
        {
            wrappedBuffer = indexBufferFactory.map(logFile(logFileDir, sessionId, requiredStreamId));
            buffer = new UnsafeBuffer(wrappedBuffer);
            capacity = recordCapacity(buffer.capacity());
        }

        int query(
            final ControlledFragmentHandler handler,
            final int beginSequenceNumber,
            final int beginSequenceIndex,
            final int endSequenceNumber,
            final int endSequenceIndex)
        {
            messageFrameHeader.wrap(buffer, 0);

            final int actingBlockLength = messageFrameHeader.blockLength();
            final int actingVersion = messageFrameHeader.version();
            final int requiredStreamId = ReplayQuery.this.requiredStreamId;
            final boolean upToMostRecentMessage = endSequenceNumber == MOST_RECENT_MESSAGE;

            // LOOKUP THE RANGE FROM THE INDEX
            // NB: this is a List as we are looking up recordings in the correct order to replay them.
            final List<RecordingRange> ranges = new ArrayList<>();
            RecordingRange currentRange = null;

            // positions on a monotonically increasing scale
            long iteratorPosition = beginChangeVolatile(buffer);
            // First iteration around you need to start at 0
            if (iteratorPosition < capacity)
            {
                iteratorPosition = 0;
            }
            long stopIteratingPosition = iteratorPosition + capacity;

            int lastSequenceNumber = -1;
            while (iteratorPosition != stopIteratingPosition)
            {
                final long changePosition = endChangeVolatile(buffer);

                // Lapped by writer
                if (changePosition > iteratorPosition && (iteratorPosition + capacity) <= beginChangeVolatile(buffer))
                {
                    iteratorPosition = changePosition;
                    stopIteratingPosition = iteratorPosition + capacity;
                }

                final int offset = offset(iteratorPosition, capacity);

                indexRecord.wrap(buffer, offset, actingBlockLength, actingVersion);
                final int streamId = indexRecord.streamId();
                long beginPosition = indexRecord.position();
                final int sequenceIndex = indexRecord.sequenceIndex();
                final int sequenceNumber = indexRecord.sequenceNumber();
                final long recordingId = indexRecord.recordingId();
                int readLength = indexRecord.length();

                UNSAFE.loadFence(); // LoadLoad required so previous loads don't move past version check below.

                // if the block was read atomically with no updates
                if (changePosition == beginChangeVolatile(buffer))
                {
                    idleStrategy.reset();

                    if (beginPosition == 0)
                    {
                        break;
                    }

                    final boolean endOk = upToMostRecentMessage || sequenceIndex < endSequenceIndex ||
                        (sequenceIndex == endSequenceIndex && sequenceNumber <= endSequenceNumber);
                    final boolean startOk = sequenceIndex > beginSequenceIndex ||
                        (sequenceIndex == beginSequenceIndex && sequenceNumber >= beginSequenceNumber);
                    if (startOk && endOk && streamId == requiredStreamId)
                    {
                        if (currentRange == null)
                        {
                            currentRange = new RecordingRange(recordingId);
                        }
                        else if (currentRange.recordingId != recordingId)
                        {
                            ranges.add(currentRange);
                            currentRange = new RecordingRange(recordingId);
                        }

                        beginPosition -= FRAME_ALIGNMENT;
                        readLength += FRAME_ALIGNMENT;
                        currentRange.add(beginPosition, readLength);

                        // FIX messages can be fragmented, so number of range adds != count
                        if (lastSequenceNumber != sequenceNumber)
                        {
                            currentRange.count++;
                        }
                        lastSequenceNumber = sequenceNumber;
                    }
                    iteratorPosition += RECORD_LENGTH;
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

            return replayTheRange(handler, ranges);
        }

        private int replayTheRange(final ControlledFragmentHandler handler, final List<RecordingRange> ranges)
        {
            /*System.out.println("Replaying ranges: " + ranges);*/

            int replayedMessages = 0;
            for (int i = 0, size = ranges.size(); i < size; i++)
            {
                final RecordingRange recordingRange = ranges.get(i);
                // System.err.println(recordingRange);
                final long beginPosition = recordingRange.position;
                final long length = recordingRange.length;
                final long endPosition = beginPosition + length;

                recordingBarrier.await(recordingRange.recordingId, endPosition, idleStrategy);

                try (Subscription subscription = aeronArchive.replay(
                    recordingRange.recordingId,
                    beginPosition,
                    length,
                    IPC_CHANNEL,
                    ARCHIVE_REPLAY_STREAM))
                {
                    messageTracker.wrap(handler);

                    // TODO: if we fail properly abort and don't retry.
                    while (messageTracker.count < recordingRange.count)
                    {
                        subscription.controlledPoll(assembler, Integer.MAX_VALUE);

                        idleStrategy.idle();
                    }
                    idleStrategy.reset();

                    replayedMessages += recordingRange.count;
                }
                catch (final Throwable exception)
                {
                    errorHandler.onError(exception);

                    return replayedMessages;
                }
            }

            return replayedMessages;
        }

        public void close()
        {
            if (wrappedBuffer instanceof MappedByteBuffer)
            {
                IoUtil.unmap((MappedByteBuffer)wrappedBuffer);
            }
        }
    }

    static final class RecordingRange
    {
        final long recordingId;
        long position = MISSING_LONG;
        int length;
        int count;

        RecordingRange(final long recordingId)
        {
            this.recordingId = recordingId;
            this.count = 0;
        }

        void add(final long addPosition, final int addLength)
        {
            final long currentPosition = this.position;

            if (currentPosition == MISSING_LONG)
            {
                this.position = addPosition;
                this.length = addLength;
                return;
            }

            final long currentEnd = currentPosition + this.length;
            final long addEnd = addPosition + addLength;
            final long newEnd = Math.max(currentEnd, addEnd);

            if (currentPosition < addPosition)
            {
                // Add to the end
                this.length = (int)(newEnd - currentPosition);
            }
            else if (addPosition < currentPosition)
            {
                // Add to the start
                this.position = addPosition;
                this.length = (int)(newEnd - addPosition);
            }
            else
            {
                DebugLogger.log(LogTag.INDEX, "currentPosition == addPosition, %d", currentPosition);
            }
        }

        @Override
        public String toString()
        {
            return "RecordingRange{" +
                "recordingId=" + recordingId +
                ", position=" + position +
                ", length=" + length +
                ", count=" + count +
                '}';
        }
    }

    private static class MessageTracker implements ControlledFragmentHandler
    {
        private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();

        ControlledFragmentHandler messageHandler;
        int count;

        @Override
        public Action onFragment(
            final DirectBuffer buffer, final int offset, final int length, final Header header)
        {
            messageHeaderDecoder.wrap(buffer, offset);

            if (messageHeaderDecoder.templateId() == FixMessageDecoder.TEMPLATE_ID)
            {
                count++;
                return messageHandler.onFragment(buffer, offset, length, header);
            }

            return CONTINUE;
        }

        void wrap(final ControlledFragmentHandler handler)
        {
            this.messageHandler = handler;
            this.count = 0;
        }
    }
}
