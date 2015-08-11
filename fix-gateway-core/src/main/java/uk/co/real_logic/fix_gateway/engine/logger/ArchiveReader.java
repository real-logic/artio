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

import uk.co.real_logic.aeron.protocol.DataHeaderFlyweight;
import uk.co.real_logic.agrona.IoUtil;
import uk.co.real_logic.agrona.collections.BiInt2ObjectMap;
import uk.co.real_logic.agrona.collections.BiInt2ObjectMap.EntryFunction;
import uk.co.real_logic.agrona.collections.IntLruCache;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.messages.ArchiveMetaDataDecoder;
import uk.co.real_logic.fix_gateway.messages.FixMessageDecoder;
import uk.co.real_logic.fix_gateway.messages.MessageHeaderDecoder;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

import static java.lang.Integer.numberOfTrailingZeros;
import static uk.co.real_logic.aeron.logbuffer.LogBufferDescriptor.computeTermIdFromPosition;
import static uk.co.real_logic.aeron.logbuffer.LogBufferDescriptor.computeTermOffsetFromPosition;
import static uk.co.real_logic.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;

public class ArchiveReader
{
    public static final int MESSAGE_FRAME_BLOCK_LENGTH =
        MessageHeaderDecoder.ENCODED_LENGTH + FixMessageDecoder.BLOCK_LENGTH + FixMessageDecoder.bodyHeaderLength();

    private final FixMessageDecoder messageFrame = new FixMessageDecoder();

    private final EntryFunction<SessionReader> newSessionReader = SessionReader::new;
    private final BiInt2ObjectMap<SessionReader> streamAndSessionToReader;
    private final ExistingBufferFactory archiveBufferFactory;
    private final ArchiveMetaData metaData;
    private final int loggerCacheCapacity;
    private final LogDirectoryDescriptor directoryDescriptor;

    public ArchiveReader(
        final ExistingBufferFactory archiveBufferFactory,
        final ArchiveMetaData metaData,
        final String logFileDir,
        final int loggerCacheCapacity)
    {
        this.archiveBufferFactory = archiveBufferFactory;
        this.metaData = metaData;
        this.loggerCacheCapacity = loggerCacheCapacity;
        directoryDescriptor = new LogDirectoryDescriptor(logFileDir);
        streamAndSessionToReader = new BiInt2ObjectMap<>();
    }

    public boolean read(final int streamId, final int aeronSessionId, final long position, final LogHandler handler)
    {
        return streamAndSessionToReader
            .computeIfAbsent(streamId, aeronSessionId, newSessionReader)
            .read(position, handler);
    }

    private final class SessionReader implements AutoCloseable
    {
        private final int streamId;
        private final int sessionId;
        private final IntLruCache<ByteBuffer> termIdToBuffer =
            new IntLruCache<>(loggerCacheCapacity, this::newBuffer, this::closeBuffer);
        private final UnsafeBuffer buffer = new UnsafeBuffer(0, 0);
        private final DataHeaderFlyweight dataHeader = new DataHeaderFlyweight();
        private final int initialTermId;
        private final int positionBitsToShift;

        private SessionReader(final int streamId, final int sessionId)
        {
            this.streamId = streamId;
            this.sessionId = sessionId;
            final ArchiveMetaDataDecoder streamMetaData = metaData.read(streamId, sessionId);
            initialTermId = streamMetaData.initialTermId();
            positionBitsToShift = numberOfTrailingZeros(streamMetaData.termBufferLength());
        }

        private ByteBuffer newBuffer(final int termId)
        {
            final File logFile = directoryDescriptor.logFile(streamId, sessionId, termId);
            return archiveBufferFactory.map(logFile);
        }

        private boolean read(final long position, final LogHandler handler)
        {
            final int termId = computeTermIdFromPosition(position, positionBitsToShift, initialTermId);
            final ByteBuffer termBuffer = termIdToBuffer.lookup(termId);
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
            termIdToBuffer.close();
        }

        private void closeBuffer(final ByteBuffer buffer)
        {
            if (buffer instanceof MappedByteBuffer)
            {
                IoUtil.unmap((MappedByteBuffer)buffer);
            }
        }
    }
}
