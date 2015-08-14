/*
 * Copyright 2015 Real Logic Ltd.
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

import uk.co.real_logic.agrona.IoUtil;
import uk.co.real_logic.agrona.collections.LongLruCache;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.messages.MessageHeaderDecoder;
import uk.co.real_logic.fix_gateway.messages.ReplayIndexRecordDecoder;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

import static uk.co.real_logic.fix_gateway.GatewayProcess.OUTBOUND_LIBRARY_STREAM;
import static uk.co.real_logic.fix_gateway.engine.logger.ReplayIndex.logFile;

/**
 * Queries an index of a composite key of session id and sequence number
 */
public class ReplayQuery implements AutoCloseable
{
    private final MessageHeaderDecoder messageFrameHeader = new MessageHeaderDecoder();
    private final ReplayIndexRecordDecoder indexRecord = new ReplayIndexRecordDecoder();

    private final LongLruCache<SessionQuery> sessionToIndex;
    private final String logFileDir;
    private final ExistingBufferFactory indexBufferFactory;

    private final ArchiveReader archiveReader;

    public ReplayQuery(
        final String logFileDir,
        final int loggerCacheCapacity,
        final ExistingBufferFactory indexBufferFactory,
        final ArchiveReader archiveReader)
    {
        this.logFileDir = logFileDir;
        this.indexBufferFactory = indexBufferFactory;
        this.archiveReader = archiveReader;
        sessionToIndex = new LongLruCache<>(loggerCacheCapacity, SessionQuery::new, SessionQuery::close);
    }

    public int query(
        final LogHandler handler, final long sessionId, final int beginSeqNo, final int endSeqNo)
    {
        return sessionToIndex
            .lookup(sessionId)
            .query(handler, beginSeqNo, endSeqNo);
    }

    public void close()
    {
        sessionToIndex.close();
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
        private int query(final LogHandler handler, final int beginSeqNo, final int endSeqNo)
        {
            messageFrameHeader.wrap(buffer, 0);
            int index = messageFrameHeader.encodedLength();
            final int actingBlockLength = messageFrameHeader.blockLength();
            final int actingVersion = messageFrameHeader.version();
            int count = 0;

            while (true)
            {
                indexRecord.wrap(buffer, index, actingBlockLength, actingVersion);
                final int streamId = indexRecord.streamId();
                final int aeronSessionId = indexRecord.aeronSessionId();
                final long position = indexRecord.position();
                if (position == 0)
                {
                    return count;
                }

                final int sequenceNumber = indexRecord.sequenceNumber();
                if (sequenceNumber >= beginSeqNo && sequenceNumber <= endSeqNo && streamId == OUTBOUND_LIBRARY_STREAM)
                {
                    count++;
                    if (!archiveReader.read(streamId, aeronSessionId, position, handler))
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
