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

import uk.co.real_logic.aeron.common.concurrent.logbuffer.Header;
import uk.co.real_logic.aeron.driver.Configuration;
import uk.co.real_logic.agrona.collections.Int2ObjectHashMap;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.messages.FixMessageDecoder;
import uk.co.real_logic.fix_gateway.messages.MessageHeaderDecoder;

import java.nio.ByteBuffer;
import java.util.function.IntFunction;

import static java.lang.Integer.numberOfTrailingZeros;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.computeTermIdFromPosition;
import static uk.co.real_logic.aeron.common.concurrent.logbuffer.LogBufferDescriptor.computeTermOffsetFromPosition;
import static uk.co.real_logic.aeron.common.protocol.HeaderFlyweight.HEADER_LENGTH;

public class ArchiveReader
{
    // TODO: load these out of a configuration file.
    private static final int POSITION_BITS_TO_SHIFT = numberOfTrailingZeros(Configuration.termBufferLength());

    public static final int MESSAGE_FRAME_BLOCK_LENGTH = 8 + FixMessageDecoder.BLOCK_LENGTH;

    private final MessageHeaderDecoder messageFrameHeader = new MessageHeaderDecoder();
    private final FixMessageDecoder messageFrame = new FixMessageDecoder();

    private final IntFunction<StreamReader> newStreamReader = StreamReader::new;
    private final Int2ObjectHashMap<StreamReader> streamIdToReader = new Int2ObjectHashMap<>();
    private final ReadableBufferFactory archiveBufferFactory;
    private final ArchiveMetaData metaData;

    public ArchiveReader(final ReadableBufferFactory archiveBufferFactory, final ArchiveMetaData metaData)
    {
        this.archiveBufferFactory = archiveBufferFactory;
        this.metaData = metaData;
    }

    public boolean read(final int streamId, final long position, final LogHandler handler)
    {
        return streamIdToReader.computeIfAbsent(streamId, newStreamReader)
                               .read(position, handler);
    }

    private final class StreamReader
    {
        private final int streamId;
        private final Int2ObjectHashMap<ByteBuffer> termIdToBuffer = new Int2ObjectHashMap<>();
        private final IntFunction<ByteBuffer> newBuffer = this::newBuffer;
        private final UnsafeBuffer buffer = new UnsafeBuffer(0, 0);
        private final Header header = new Header();
        private final int initialTermId;

        private StreamReader(final int streamId)
        {
            this.streamId = streamId;
            initialTermId = metaData.read(streamId).initialTermId();
            header.buffer(buffer);
        }

        private ByteBuffer newBuffer(final int termId)
        {
            return archiveBufferFactory.map(LogDirectoryDescriptor.logFile(streamId, termId));
        }

        private boolean read(final long position, final LogHandler handler)
        {
            final int termId = computeTermIdFromPosition(position, POSITION_BITS_TO_SHIFT, initialTermId);
            final ByteBuffer termBuffer = termIdToBuffer.computeIfAbsent(termId, newBuffer);
            final int aeronFrameOffset = computeTermOffsetFromPosition(position, POSITION_BITS_TO_SHIFT);

            buffer.wrap(termBuffer);
            header.offset(aeronFrameOffset);

            final int startOffset = aeronFrameOffset + HEADER_LENGTH;
            final int messageOffset = startOffset + MESSAGE_FRAME_BLOCK_LENGTH;
            final int length = header.frameLength() - (HEADER_LENGTH + MESSAGE_FRAME_BLOCK_LENGTH);

            return handler.onLogEntry(messageFrame, buffer, startOffset, messageOffset, length);
        }
    }
}
