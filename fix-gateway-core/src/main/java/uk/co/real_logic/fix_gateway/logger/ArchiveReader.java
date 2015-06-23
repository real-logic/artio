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

import uk.co.real_logic.aeron.protocol.DataHeaderFlyweight;
import uk.co.real_logic.agrona.IoUtil;
import uk.co.real_logic.agrona.collections.Int2ObjectHashMap;
import uk.co.real_logic.agrona.collections.IntLruCache;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.messages.ArchiveMetaDataDecoder;
import uk.co.real_logic.fix_gateway.messages.FixMessageDecoder;
import uk.co.real_logic.fix_gateway.messages.MessageHeaderDecoder;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.function.IntFunction;

import static java.lang.Integer.numberOfTrailingZeros;
import static uk.co.real_logic.aeron.logbuffer.LogBufferDescriptor.computeTermIdFromPosition;
import static uk.co.real_logic.aeron.logbuffer.LogBufferDescriptor.computeTermOffsetFromPosition;
import static uk.co.real_logic.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;

public class ArchiveReader
{
    public static final int MESSAGE_FRAME_BLOCK_LENGTH =
        MessageHeaderDecoder.SIZE + FixMessageDecoder.BLOCK_LENGTH + FixMessageDecoder.bodyHeaderSize();

    private final FixMessageDecoder messageFrame = new FixMessageDecoder();

    private final IntLruCache<StreamReader> streamIdToReader;
    private final ExistingBufferFactory archiveBufferFactory;
    private final ArchiveMetaData metaData;
    private final LogDirectoryDescriptor directoryDescriptor;

    public ArchiveReader(
        final ExistingBufferFactory archiveBufferFactory,
        final ArchiveMetaData metaData,
        final String logFileDir,
        final int loggerCacheCapacity)
    {
        this.archiveBufferFactory = archiveBufferFactory;
        this.metaData = metaData;
        directoryDescriptor = new LogDirectoryDescriptor(logFileDir);
        streamIdToReader = new IntLruCache<>(loggerCacheCapacity, StreamReader::new);
    }

    public boolean read(final int streamId, final long position, final LogHandler handler)
    {
        return streamIdToReader
            .lookup(streamId)
            .read(position, handler);
    }

    private final class StreamReader implements AutoCloseable
    {
        private final int streamId;
        private final Int2ObjectHashMap<ByteBuffer> termIdToBuffer = new Int2ObjectHashMap<>();
        private final IntFunction<ByteBuffer> newBuffer = this::newBuffer;
        private final UnsafeBuffer buffer = new UnsafeBuffer(0, 0);
        private final DataHeaderFlyweight dataHeader = new DataHeaderFlyweight();
        private final int initialTermId;
        private final int positionBitsToShift;

        private StreamReader(final int streamId)
        {
            this.streamId = streamId;
            final ArchiveMetaDataDecoder streamMetaData = metaData.read(streamId);
            initialTermId = streamMetaData.initialTermId();
            positionBitsToShift = numberOfTrailingZeros(streamMetaData.termBufferLength());
        }

        private ByteBuffer newBuffer(final int termId)
        {
            return archiveBufferFactory.map(directoryDescriptor.logFile(streamId, termId));
        }

        private boolean read(final long position, final LogHandler handler)
        {
            final int termId = computeTermIdFromPosition(position, positionBitsToShift, initialTermId);
            final ByteBuffer termBuffer = termIdToBuffer.computeIfAbsent(termId, newBuffer);
            final int aeronHeaderOffset = computeTermOffsetFromPosition(position, positionBitsToShift) - HEADER_LENGTH;

            buffer.wrap(termBuffer);
            dataHeader.wrap(buffer, aeronHeaderOffset);

            final int aeronDataOffset = aeronHeaderOffset + HEADER_LENGTH;
            final int fixMessageOffset = aeronDataOffset + MESSAGE_FRAME_BLOCK_LENGTH;
            final int messageLength = dataHeader.frameLength() - (HEADER_LENGTH + MESSAGE_FRAME_BLOCK_LENGTH);

            return handler.onLogEntry(messageFrame, buffer, aeronDataOffset, fixMessageOffset, messageLength);
        }

        @Override
        public void close() throws Exception
        {
            termIdToBuffer.values().forEach(
                (buffer) ->
                {
                    if (buffer instanceof MappedByteBuffer)
                    {
                        IoUtil.unmap((MappedByteBuffer)buffer);
                    }
                });
        }
    }
}
