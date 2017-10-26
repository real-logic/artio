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

import io.aeron.logbuffer.ControlledFragmentHandler;
import org.agrona.IoUtil;
import org.agrona.collections.Long2ObjectCache;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.artio.messages.MessageHeaderDecoder;
import uk.co.real_logic.artio.storage.messages.ReplayIndexRecordDecoder;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.function.LongFunction;

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
    private final ExistingBufferFactory indexBufferFactory;
    private final ArchiveReader archiveReader;
    private final int requiredStreamId;
    private final IdleStrategy idleStrategy;

    public ReplayQuery(
        final String logFileDir,
        final int cacheNumSets,
        final int cacheSetSize,
        final ExistingBufferFactory indexBufferFactory,
        final ArchiveReader archiveReader,
        final int requiredStreamId,
        final IdleStrategy idleStrategy)
    {
        this.logFileDir = logFileDir;
        this.indexBufferFactory = indexBufferFactory;
        this.archiveReader = archiveReader;
        this.requiredStreamId = requiredStreamId;
        this.idleStrategy = idleStrategy;
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
        archiveReader.close();
    }

    private final class SessionQuery implements AutoCloseable
    {
        private final ByteBuffer wrappedBuffer;
        private final UnsafeBuffer buffer;
        private final int capacity;

        private SessionQuery(final long sessionId)
        {
            wrappedBuffer = indexBufferFactory.map(logFile(logFileDir, sessionId, requiredStreamId));
            buffer = new UnsafeBuffer(wrappedBuffer);
            capacity = recordCapacity(buffer.capacity());
        }

        private int query(
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

            int count = 0;
            int lastAeronSessionId = 0;
            ArchiveReader.SessionReader sessionReader = null;

            // positions on a monotonically increasing scale
            long iteratorPosition = beginChangeVolatile(buffer);
            // First iteration around you need to start at 0
            if (iteratorPosition < capacity)
            {
                iteratorPosition = 0;
            }
            long stopIteratingPosition = iteratorPosition + capacity;

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
                final long position = indexRecord.position();
                final int aeronSessionId = indexRecord.aeronSessionId();
                final int sequenceIndex = indexRecord.sequenceIndex();
                final int sequenceNumber = indexRecord.sequenceNumber();

                UNSAFE.loadFence(); // LoadLoad required so previous loads don't move past version check below.

                // if the block was read atomically with no updates
                if (changePosition == beginChangeVolatile(buffer))
                {
                    idleStrategy.reset();

                    if (position == 0)
                    {
                        break;
                    }

                    if (sessionReader == null || aeronSessionId != lastAeronSessionId)
                    {
                        lastAeronSessionId = aeronSessionId;
                        sessionReader = archiveReader.session(aeronSessionId);
                    }

                    // You can't find the entry in the log file so treat the same as
                    // ArchiveReader.read() returning NO_MESSAGE.
                    if (sessionReader == null)
                    {
                        break;
                    }

                    final boolean endOk = upToMostRecentMessage || sequenceIndex < endSequenceIndex ||
                        (sequenceIndex == endSequenceIndex && sequenceNumber <= endSequenceNumber);
                    final boolean startOk = sequenceIndex > beginSequenceIndex ||
                        (sequenceIndex == beginSequenceIndex && sequenceNumber >= beginSequenceNumber);
                    if (startOk && endOk && streamId == requiredStreamId)
                    {
                        final long readTo = sessionReader.read(position, handler);
                        if (readTo < 0 || readTo == position)
                        {
                            break;
                        }

                        count++;
                    }
                    iteratorPosition += RECORD_LENGTH;
                }
                else
                {
                    idleStrategy.idle();
                }
            }

            return count;
        }

        public void close()
        {
            if (wrappedBuffer instanceof MappedByteBuffer)
            {
                IoUtil.unmap((MappedByteBuffer)wrappedBuffer);
            }
        }
    }
}
