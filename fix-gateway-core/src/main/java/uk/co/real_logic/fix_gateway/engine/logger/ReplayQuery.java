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

import io.aeron.logbuffer.FragmentHandler;
import org.agrona.IoUtil;
import org.agrona.collections.Long2ObjectCache;
import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.messages.MessageHeaderDecoder;
import uk.co.real_logic.fix_gateway.messages.ReplayIndexRecordDecoder;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.function.LongFunction;

import static uk.co.real_logic.fix_gateway.GatewayProcess.OUTBOUND_LIBRARY_STREAM;
import static uk.co.real_logic.fix_gateway.engine.logger.ReplayIndex.logFile;

/**
 * Queries an index of a composite key of session id and sequence number
 */
class ReplayQuery implements AutoCloseable
{
    private final MessageHeaderDecoder messageFrameHeader = new MessageHeaderDecoder();
    private final ReplayIndexRecordDecoder indexRecord = new ReplayIndexRecordDecoder();

    private final LongFunction<SessionQuery> newSessionQuery = SessionQuery::new;
    private final Long2ObjectCache<SessionQuery> sessionToIndex;
    private final String logFileDir;
    private final ExistingBufferFactory indexBufferFactory;

    private final ArchiveReader outboundArchiveReader;

    ReplayQuery(
        final String logFileDir,
        final int cacheNumSets,
        final int cacheSetSize,
        final ExistingBufferFactory indexBufferFactory,
        final ArchiveReader outboundArchiveReader)
    {
        this.logFileDir = logFileDir;
        this.indexBufferFactory = indexBufferFactory;
        this.outboundArchiveReader = outboundArchiveReader;
        sessionToIndex = new Long2ObjectCache<>(cacheNumSets, cacheSetSize, SessionQuery::close);
    }

    public int query(
        final FragmentHandler handler, final long sessionId, final int beginSeqNo, final int endSeqNo)
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
            wrappedBuffer = indexBufferFactory.map(logFile(logFileDir, sessionId));
            buffer = new UnsafeBuffer(wrappedBuffer);
        }

        // TODO: potential optimisation of jumping straight to the beginSeqNo offset
        // Needs thinking about out of order sequence numbers due to duplicates and resends
        private int query(final FragmentHandler handler, final int beginSeqNo, final int endSeqNo)
        {
            messageFrameHeader.wrap(buffer, 0);
            int index = messageFrameHeader.encodedLength();
            final int actingBlockLength = messageFrameHeader.blockLength();
            final int actingVersion = messageFrameHeader.version();

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
                    sessionReader = outboundArchiveReader.session(aeronSessionId);
                }

                final int sequenceNumber = indexRecord.sequenceNumber();
                if (sequenceNumber >= beginSeqNo && sequenceNumber <= endSeqNo && streamId == OUTBOUND_LIBRARY_STREAM)
                {
                    count++;
                    if (sessionReader.read(position, handler) < 0)
                    {
                        return count;
                    }
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
