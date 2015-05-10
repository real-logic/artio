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
package uk.co.real_logic.fix_gateway.logger;

import uk.co.real_logic.agrona.IoUtil;
import uk.co.real_logic.agrona.collections.Long2ObjectHashMap;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.messages.MessageHeaderDecoder;
import uk.co.real_logic.fix_gateway.messages.ReplayIndexRecordDecoder;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.function.LongFunction;

/**
 * Queries an index of a composite key of session id and sequence number
 */
public class ReplayQuery
{
    private final MessageHeaderDecoder messageFrameHeader = new MessageHeaderDecoder();
    private final ReplayIndexRecordDecoder indexRecord = new ReplayIndexRecordDecoder();

    private final Long2ObjectHashMap<SessionQuery> sessionToIndex = new Long2ObjectHashMap<>();
    private final LongFunction<SessionQuery> newSessionQuery = SessionQuery::new;
    private final BufferFactory indexBufferFactory;

    private final ArchiveReader archiveReader;

    public ReplayQuery(final BufferFactory indexBufferFactory, final ArchiveReader archiveReader)
    {
        this.indexBufferFactory = indexBufferFactory;
        this.archiveReader = archiveReader;
    }

    // TODO: add some way of notifying of missing records from the query.
    public void query(
        final LogHandler handler, final long sessionId, final int beginSeqNo, final int endSeqNo)
    {
        sessionToIndex.computeIfAbsent(sessionId, newSessionQuery)
                      .query(handler, beginSeqNo, endSeqNo);
    }

    public void close()
    {
        sessionToIndex.values().forEach(SessionQuery::close);
    }

    private final class SessionQuery
    {
        private final ByteBuffer wrappedBuffer;
        private final UnsafeBuffer buffer;

        private SessionQuery(final long sessionId)
        {
            wrappedBuffer = indexBufferFactory.map(ReplayIndex.logFile(sessionId));
            buffer = new UnsafeBuffer(wrappedBuffer);
        }

        // TODO: potential optimisation of jumping straight to the beginSeqNo offset
        // Needs thinking about out of order sequence numbers due to duplicates and resends
        private void query(final LogHandler handler, final int beginSeqNo, final int endSeqNo)
        {
            messageFrameHeader.wrap(buffer, 0, indexRecord.sbeSchemaVersion());
            int index = messageFrameHeader.size();
            final int actingBlockLength = messageFrameHeader.blockLength();
            final int actingVersion = messageFrameHeader.version();

            while (true)
            {
                indexRecord.wrap(buffer, index, actingBlockLength, actingVersion);
                final int streamId = indexRecord.streamId();
                final long position = indexRecord.position();
                if (position == 0)
                {
                    return;
                }

                final int sequenceNumber = indexRecord.sequenceNumber();
                if (sequenceNumber >= beginSeqNo && sequenceNumber <= endSeqNo)
                {
                    if (!archiveReader.read(streamId, position, handler))
                    {
                        return;
                    }
                }
                index = indexRecord.limit();
            }
        }

        public void close()
        {
            if (wrappedBuffer instanceof MappedByteBuffer)
            {
                IoUtil.unmap((MappedByteBuffer) wrappedBuffer);
            }
        }
    }
}
