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
package uk.co.real_logic.fix_gateway.engine.logger;

import io.aeron.logbuffer.FrameDescriptor;
import org.agrona.*;
import org.agrona.collections.Long2ObjectCache;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.decoder.HeaderDecoder;
import uk.co.real_logic.fix_gateway.messages.FixMessageDecoder;
import uk.co.real_logic.fix_gateway.messages.FixMessageEncoder;
import uk.co.real_logic.fix_gateway.messages.MessageHeaderDecoder;
import uk.co.real_logic.fix_gateway.messages.MessageHeaderEncoder;
import uk.co.real_logic.fix_gateway.storage.messages.ReplayIndexRecordEncoder;
import uk.co.real_logic.fix_gateway.util.AsciiBuffer;
import uk.co.real_logic.fix_gateway.util.MutableAsciiBuffer;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.function.LongFunction;

import static org.agrona.UnsafeAccess.UNSAFE;
import static uk.co.real_logic.fix_gateway.engine.logger.ReplayIndexDescriptor.*;
import static uk.co.real_logic.fix_gateway.messages.MessageStatus.OK;

/**
 * Builds an index of a composite key of session id and sequence number for a given stream.
 *
 * Written Positions are stored in a separate file at {@link ReplayIndexDescriptor#replayPositionPath(String, int)}.
 *
 * Buffer Consists of:
 *
 * MessageHeader
 * Head position counter
 * Tail position counter
 * Multiple ReplayIndexRecord entries
 */
public class ReplayIndex implements Index
{
    private final LongFunction<SessionIndex> newSessionIndex = SessionIndex::new;
    private final AsciiBuffer asciiBuffer = new MutableAsciiBuffer();
    private final MessageHeaderDecoder frameHeaderDecoder = new MessageHeaderDecoder();
    private final FixMessageDecoder messageFrame = new FixMessageDecoder();
    private final HeaderDecoder fixHeader = new HeaderDecoder();
    private final ReplayIndexRecordEncoder replayIndexRecord = new ReplayIndexRecordEncoder();
    private final MessageHeaderEncoder indexHeaderEncoder = new MessageHeaderEncoder();
    private final IndexedPositionWriter positionWriter;
    private final IndexedPositionReader positionReader;

    private final Long2ObjectCache<SessionIndex> fixSessionIdToIndex;

    private final String logFileDir;
    private final int requiredStreamId;
    private final int indexFileSize;
    private final BufferFactory bufferFactory;
    private final AtomicBuffer positionBuffer;

    public ReplayIndex(
        final String logFileDir,
        final int requiredStreamId,
        final int indexFileSize,
        final int cacheNumSets,
        final int cacheSetSize,
        final BufferFactory bufferFactory,
        final AtomicBuffer positionBuffer,
        final ErrorHandler errorHandler)
    {
        this.logFileDir = logFileDir;
        this.requiredStreamId = requiredStreamId;
        this.indexFileSize = indexFileSize;
        checkIndexFileSize(indexFileSize);
        this.bufferFactory = bufferFactory;
        this.positionBuffer = positionBuffer;
        fixSessionIdToIndex = new Long2ObjectCache<>(cacheNumSets, cacheSetSize, SessionIndex::close);
        positionWriter = new IndexedPositionWriter(
            positionBuffer, errorHandler, 0, replayPositionPath(logFileDir, requiredStreamId));
        positionReader = new IndexedPositionReader(positionBuffer);
    }

    public void indexRecord(
        final DirectBuffer srcBuffer,
        final int srcOffset,
        final int srcLength,
        final int streamId,
        final int aeronSessionId,
        final long endPosition)
    {
        if (streamId != requiredStreamId)
        {
            return;
        }

        int offset = srcOffset;
        frameHeaderDecoder.wrap(srcBuffer, offset);
        if (frameHeaderDecoder.templateId() == FixMessageEncoder.TEMPLATE_ID)
        {
            final int actingBlockLength = frameHeaderDecoder.blockLength();
            offset += frameHeaderDecoder.encodedLength();

            messageFrame.wrap(srcBuffer, offset, actingBlockLength, frameHeaderDecoder.version());
            if (messageFrame.status() == OK)
            {
                offset += actingBlockLength + 2;

                asciiBuffer.wrap(srcBuffer);
                fixHeader.decode(asciiBuffer, offset, messageFrame.bodyLength());

                final int alignedLength = BitUtil.align(srcLength, FrameDescriptor.FRAME_ALIGNMENT);
                final long beginPosition = endPosition - alignedLength;

                final int sequenceNumber = fixHeader.msgSeqNum();
                final int sequenceIndex = messageFrame.sequenceIndex();
                final long fixSessionId = messageFrame.session();

                fixSessionIdToIndex
                    .computeIfAbsent(fixSessionId, newSessionIndex)
                    .onRecord(streamId, aeronSessionId, beginPosition, endPosition, sequenceNumber, sequenceIndex);
            }
        }
    }

    public void close()
    {
        positionWriter.close();
        fixSessionIdToIndex.clear();
        IoUtil.unmap(positionBuffer.byteBuffer());
    }

    public void readLastPosition(final IndexedPositionConsumer consumer)
    {
        positionReader.readLastPosition(consumer);
    }

    private final class SessionIndex implements AutoCloseable
    {
        private final ByteBuffer wrappedBuffer;
        private final AtomicBuffer buffer;
        private final int capacity;

        private SessionIndex(final long fixSessionId)
        {
            final File logFile = logFile(logFileDir, fixSessionId, requiredStreamId);
            final boolean exists = logFile.exists();
            this.wrappedBuffer = bufferFactory.map(logFile, indexFileSize);
            this.buffer = new UnsafeBuffer(wrappedBuffer);

            capacity = recordCapacity(buffer.capacity());
            if (!exists)
            {
                indexHeaderEncoder
                    .wrap(buffer, 0)
                    .blockLength(replayIndexRecord.sbeBlockLength())
                    .templateId(replayIndexRecord.sbeTemplateId())
                    .schemaId(replayIndexRecord.sbeSchemaId())
                    .version(replayIndexRecord.sbeSchemaVersion());
            }
        }

        private void onRecord(
            final int streamId,
            final int aeronSessionId,
            final long beginPosition,
            final long endPosition,
            final int sequenceNumber,
            final int sequenceIndex)
        {
            final long beginChangePosition = beginChange(buffer);
            final long changePosition = beginChangePosition + RECORD_LENGTH;

            beginChangeOrdered(buffer, changePosition);
            UNSAFE.storeFence();

            final int offset = offset(beginChangePosition, capacity);

            replayIndexRecord
                .wrap(buffer, offset)
                .streamId(streamId)
                .aeronSessionId(aeronSessionId)
                .position(beginPosition)
                .sequenceNumber(sequenceNumber)
                .sequenceIndex(sequenceIndex);

            positionWriter.indexedUpTo(aeronSessionId, endPosition);
            positionWriter.updateChecksums();

            endChangeOrdered(buffer, changePosition);
        }

        public void close()
        {
            IoUtil.unmap(wrappedBuffer);
        }
    }
}
