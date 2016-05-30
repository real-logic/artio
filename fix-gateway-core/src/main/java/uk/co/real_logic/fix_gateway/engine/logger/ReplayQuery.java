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
import io.aeron.logbuffer.FragmentHandler;
import org.agrona.IoUtil;
import org.agrona.collections.Long2ObjectCache;
import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.messages.MessageHeaderDecoder;
import uk.co.real_logic.fix_gateway.messages.ReplayIndexRecordDecoder;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.function.LongFunction;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;
import static uk.co.real_logic.fix_gateway.engine.logger.ReplayIndex.logFile;

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
     * @param beginSeqNo sequence number to begin replay at (inclusive).
     * @param endSeqNo sequence number to end replay at (inclusive).
     * @return number of messages replayed
     */
    public int query(
        final FragmentHandler handler, final long sessionId, final int beginSeqNo, final int endSeqNo)
    {
        // TODO: remove method and apply appropriate actions
        return query((buffer, offset, length, header) ->
        {
            handler.onFragment(buffer, offset, length, header);
            return CONTINUE;
        }, sessionId, beginSeqNo, endSeqNo);
    }

    /**
     *
     * @param handler the handler to pass the messages to
     * @param sessionId the FIX session id of the stream to replay.
     * @param beginSeqNo sequence number to begin replay at (inclusive).
     * @param endSeqNo sequence number to end replay at (inclusive).
     * @return number of messages replayed
     */
    public int query(
        final ControlledFragmentHandler handler, final long sessionId, final int beginSeqNo, final int endSeqNo)
    {
        return sessionToIndex
            .computeIfAbsent(sessionId, newSessionQuery)
            .query(handler, beginSeqNo, endSeqNo);
    }

    public void close()
    {
        sessionToIndex.clear();
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
        // Needs thinking about out of order sequence numbers due to duplicates and resends
        private int query(final ControlledFragmentHandler handler, final int beginSeqNo, final int endSeqNo)
        {
            messageFrameHeader.wrap(buffer, 0);
            int index = messageFrameHeader.encodedLength();
            final int actingBlockLength = messageFrameHeader.blockLength();
            final int actingVersion = messageFrameHeader.version();
            final int requiredStreamId = ReplayQuery.this.requiredStreamId;

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

                final int sequenceNumber = indexRecord.sequenceNumber();
                if (sequenceNumber >= beginSeqNo && sequenceNumber <= endSeqNo && streamId == requiredStreamId)
                {
                    if (sessionReader.read(position, handler) < 0)
                    {
                        return count;
                    }

                    count++;
                }
                index = indexRecord.limit();
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
