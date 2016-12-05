/*
 * Copyright 2015-2016 Real Logic Ltd.
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
package uk.co.real_logic.fix_gateway.engine.logger;

import io.aeron.logbuffer.ControlledFragmentHandler;
import org.agrona.IoUtil;
import org.agrona.collections.Long2ObjectCache;
import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.messages.MessageHeaderDecoder;
import uk.co.real_logic.fix_gateway.storage.messages.ReplayIndexRecordDecoder;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.function.LongFunction;

import static uk.co.real_logic.fix_gateway.engine.logger.ReplayIndex.logFile;
import static uk.co.real_logic.fix_gateway.engine.logger.Replayer.MOST_RECENT_MESSAGE;

/**
 * Queries an index of a composite key of session id and sequence number
 */
public class ReplayQuery implements AutoCloseable
{
    private final MessageHeaderDecoder messageFrameHeader = new MessageHeaderDecoder();
    private final ReplayIndexRecordDecoder indexRecord = new ReplayIndexRecordDecoder();

    private final LongFunction<SessionQuery> newSessionQuery = SessionQuery::new;
    private final Long2ObjectCache<SessionQuery> sessionToIndex;
    private final String logFileDir;
    private final ExistingBufferFactory indexBufferFactory;
    private final ArchiveReader archiveReader;
    private final int requiredStreamId;

    public ReplayQuery(
        final String logFileDir,
        final int cacheNumSets,
        final int cacheSetSize,
        final ExistingBufferFactory indexBufferFactory,
        final ArchiveReader archiveReader,
        final int requiredStreamId)
    {
        this.logFileDir = logFileDir;
        this.indexBufferFactory = indexBufferFactory;
        this.archiveReader = archiveReader;
        this.requiredStreamId = requiredStreamId;
        sessionToIndex = new Long2ObjectCache<>(cacheNumSets, cacheSetSize, SessionQuery::close);
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
        return sessionToIndex
            .computeIfAbsent(sessionId, newSessionQuery)
            .query(handler, beginSequenceNumber, beginSequenceIndex, endSequenceNumber, endSequenceIndex);
    }

    public void close()
    {
        sessionToIndex.clear();
        archiveReader.close();
    }

    private final class SessionQuery implements AutoCloseable
    {
        private final ByteBuffer wrappedBuffer;
        private final UnsafeBuffer buffer;

        private SessionQuery(final long sessionId)
        {
            wrappedBuffer = indexBufferFactory.map(logFile(logFileDir, sessionId, requiredStreamId));
            buffer = new UnsafeBuffer(wrappedBuffer);
        }

        // TODO: potential optimisation of jumping straight to the beginSeqNo offset
        private int query(
            final ControlledFragmentHandler handler,
            final int beginSequenceNumber,
            final int beginSequenceIndex,
            final int endSequenceNumber,
            final int endSequenceIndex)
        {
            messageFrameHeader.wrap(buffer, 0);
            int index = MessageHeaderDecoder.ENCODED_LENGTH;
            final int actingBlockLength = messageFrameHeader.blockLength();
            final int actingVersion = messageFrameHeader.version();
            final int requiredStreamId = ReplayQuery.this.requiredStreamId;
            final boolean upToMostRecentMessage = endSequenceNumber == MOST_RECENT_MESSAGE;

            int count = 0;
            int lastAeronSessionId = 0;
            ArchiveReader.SessionReader sessionReader = null;

            while (true)
            {
                indexRecord.wrap(buffer, index, actingBlockLength, actingVersion);
                final int streamId = indexRecord.streamId();
                final long position = indexRecord.position();
                if (position == 0)
                {
                    return count;
                }

                final int aeronSessionId = indexRecord.aeronSessionId();
                if (sessionReader == null || aeronSessionId != lastAeronSessionId)
                {
                    lastAeronSessionId = aeronSessionId;
                    sessionReader = archiveReader.session(aeronSessionId);
                }

                final int sequenceIndex = indexRecord.sequenceIndex();
                final int sequenceNumber = indexRecord.sequenceNumber();

                final boolean endOk = upToMostRecentMessage || sequenceIndex < endSequenceIndex ||
                    (sequenceIndex == endSequenceIndex && sequenceNumber <= endSequenceNumber);
                final boolean startOk = sequenceIndex > beginSequenceIndex ||
                    (sequenceIndex == beginSequenceIndex && sequenceNumber >= beginSequenceNumber);
                if (startOk && endOk && streamId == requiredStreamId)
                {
                    final long readTo = sessionReader.read(position, handler);
                    if (readTo < 0 || readTo == position)
                    {
                        return count;
                    }

                    count++;
                }
                index += actingBlockLength;
            }
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
