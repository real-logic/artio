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

import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.IoUtil;
import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.collections.Long2ObjectHashMap;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.decoder.HeaderDecoder;
import uk.co.real_logic.fix_gateway.messages.*;
import uk.co.real_logic.fix_gateway.util.AsciiFlyweight;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.function.LongFunction;

import static uk.co.real_logic.fix_gateway.logger.LogDirectoryDescriptor.INDEX_FILE_SIZE;
import static uk.co.real_logic.fix_gateway.logger.LogDirectoryDescriptor.LOG_FILE_DIR;

/**
 * Builds an index of a composite key of session id and sequence number
 */
public class ReplayIndex implements Index
{
    static File logFile(final long sessionId)
    {
        return new File(String.format(LOG_FILE_DIR + File.separator + "replay-index-%d", sessionId));
    }

    private final AsciiFlyweight asciiFlyweight = new AsciiFlyweight();

    private final MessageHeaderDecoder frameHeaderDecoder = new MessageHeaderDecoder();
    private final FixMessageDecoder messageFrame = new FixMessageDecoder();
    private final HeaderDecoder fixHeader = new HeaderDecoder();

    private final ReplayIndexRecordEncoder replayIndexRecord = new ReplayIndexRecordEncoder();
    private final MessageHeaderEncoder indexHeaderEncoder = new MessageHeaderEncoder();

    private final LongFunction<SessionIndex> newSessionIndex = SessionIndex::new;

    private final Long2ObjectHashMap<SessionIndex> sessionToIndex = new Long2ObjectHashMap<>();

    private final BufferFactory bufferFactory;

    public ReplayIndex(final BufferFactory bufferFactory)
    {
        this.bufferFactory = bufferFactory;
    }

    public void close()
    {
        sessionToIndex.values().forEach(SessionIndex::close);
    }

    public void indexRecord(final DirectBuffer srcBuffer, final int srcOffset, final int srcLength, final int streamId)
    {
        int offset = srcOffset;
        frameHeaderDecoder.wrap(srcBuffer, offset, frameHeaderDecoder.size());
        if (frameHeaderDecoder.templateId() == FixMessageEncoder.TEMPLATE_ID)
        {
            final int actingBlockLength = frameHeaderDecoder.blockLength();
            offset += frameHeaderDecoder.size();

            messageFrame.wrap(srcBuffer, offset, actingBlockLength, frameHeaderDecoder.version());

            offset += actingBlockLength + 2;

            asciiFlyweight.wrap(srcBuffer);
            fixHeader.decode(asciiFlyweight, offset, messageFrame.bodyLength());

            sessionToIndex
                .computeIfAbsent(messageFrame.session(), newSessionIndex)
                .onRecord(streamId, srcOffset, fixHeader.msgSeqNum());
        }
    }

    private final class SessionIndex
    {
        private final ByteBuffer wrappedBuffer;
        private final MutableDirectBuffer buffer;

        private int index = indexHeaderEncoder.size();

        private SessionIndex(final long sessionId)
        {
            this.wrappedBuffer = bufferFactory.map(logFile(sessionId), INDEX_FILE_SIZE);
            this.buffer = new UnsafeBuffer(wrappedBuffer);
            indexHeaderEncoder
                .wrap(buffer, 0, replayIndexRecord.sbeSchemaVersion())
                .blockLength(replayIndexRecord.sbeBlockLength())
                .templateId(replayIndexRecord.sbeTemplateId())
                .schemaId(replayIndexRecord.sbeSchemaId())
                .version(replayIndexRecord.sbeSchemaVersion());
        }

        public void onRecord(final int streamId, final long position, final int sequenceNumber)
        {
            replayIndexRecord
                .wrap(buffer, index)
                .streamId(streamId)
                .position(position)
                .sequenceNumber(sequenceNumber);

            index = replayIndexRecord.limit();
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
