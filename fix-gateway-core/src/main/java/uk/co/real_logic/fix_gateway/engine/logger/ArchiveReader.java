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

import uk.co.real_logic.aeron.logbuffer.BlockHandler;
import uk.co.real_logic.aeron.logbuffer.FragmentHandler;
import uk.co.real_logic.aeron.logbuffer.Header;
import uk.co.real_logic.aeron.logbuffer.LogBufferDescriptor;
import uk.co.real_logic.aeron.protocol.DataHeaderFlyweight;
import uk.co.real_logic.agrona.IoUtil;
import uk.co.real_logic.agrona.collections.Int2ObjectHashMap;
import uk.co.real_logic.agrona.collections.IntLruCache;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.messages.ArchiveMetaDataDecoder;
import uk.co.real_logic.fix_gateway.replication.StreamIdentifier;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.function.IntFunction;

import static java.lang.Integer.numberOfTrailingZeros;
import static uk.co.real_logic.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;

public class ArchiveReader implements AutoCloseable
{
    private final IntFunction<SessionReader> newSessionReader = this::newSessionReader;

    private final Int2ObjectHashMap<SessionReader> aeronSessionIdToReader;
    private final ExistingBufferFactory archiveBufferFactory;
    private final ArchiveMetaData metaData;
    private final int loggerCacheCapacity;
    private final StreamIdentifier streamId;
    private final LogDirectoryDescriptor directoryDescriptor;

    public ArchiveReader(
        final ExistingBufferFactory archiveBufferFactory,
        final ArchiveMetaData metaData,
        final String logFileDir,
        final int loggerCacheCapacity,
        final StreamIdentifier streamId)
    {
        this.archiveBufferFactory = archiveBufferFactory;
        this.metaData = metaData;
        this.loggerCacheCapacity = loggerCacheCapacity;
        this.streamId = streamId;
        directoryDescriptor = new LogDirectoryDescriptor(logFileDir);
        aeronSessionIdToReader = new Int2ObjectHashMap<>();
    }

    public void close()
    {
        metaData.close();
        aeronSessionIdToReader.values().forEach(SessionReader::close);
    }

    /**
     * Reads a message out of the log archive.
     *
     * @param aeronSessionId the session to read from
     * @param position the log position to start reading at
     * @param handler the handler to pass the data into
     * @return true if the message has been read, false otherwise
     */
    public boolean read(final int aeronSessionId, final long position, final FragmentHandler handler)
    {
        final SessionReader sessionReader = sessionReader(aeronSessionId);
        return sessionReader != null && sessionReader.read(position, handler);
    }

    /**
     * Reads a block of bytes out of the log archive.
     *
     * A block will only be read if the archive contains the whole block.
     *
     * @param aeronSessionId the session to read from
     * @param position the log position to start reading at
     * @param length the length of data read
     * @param handler the handler to pass the data into
     * @return true if the message has been read, false otherwise
     */
    public boolean readBlock(
        final int aeronSessionId, final long position, final int length, final BlockHandler handler)
    {
        final SessionReader sessionReader = sessionReader(aeronSessionId);
        return sessionReader != null && sessionReader.readBlock(position, length, handler);
    }

    private SessionReader sessionReader(final int aeronSessionId)
    {
        return aeronSessionIdToReader.computeIfAbsent(aeronSessionId, newSessionReader);
    }

    private SessionReader newSessionReader(final int sessionId)
    {
        final ArchiveMetaDataDecoder streamMetaData = metaData.read(streamId, sessionId);
        if (streamMetaData == null)
        {
            return null;
        }

        return new SessionReader(sessionId, streamMetaData.initialTermId(), streamMetaData.termBufferLength());
    }

    private final class SessionReader implements AutoCloseable
    {
        private final int sessionId;
        private final IntLruCache<ByteBuffer> termIdToBuffer =
            new IntLruCache<>(loggerCacheCapacity, this::newBuffer, this::closeBuffer);
        private final UnsafeBuffer buffer = new UnsafeBuffer(0, 0);
        private final DataHeaderFlyweight dataHeader = new DataHeaderFlyweight();
        private final int initialTermId;
        private final int positionBitsToShift;
        private final Header header;

        private SessionReader(final int sessionId, final int initialTermId, final int termBufferLength)
        {
            this.sessionId = sessionId;
            this.initialTermId = initialTermId;
            positionBitsToShift = numberOfTrailingZeros(termBufferLength);
            header = new Header(this.initialTermId, termBufferLength);
        }

        private ByteBuffer newBuffer(final int termId)
        {
            final File logFile = directoryDescriptor.logFile(streamId, sessionId, termId);
            if (!logFile.exists())
            {
                return null;
            }

            return archiveBufferFactory.map(logFile);
        }

        private boolean read(final long position, final FragmentHandler handler)
        {
            final int termId = computeTermIdFromPosition(position);
            final ByteBuffer termBuffer = termIdToBuffer.lookup(termId);
            if (termBuffer == null)
            {
                return false;
            }

            final int termOffset = computeTermOffsetFromPosition(position);
            final int headerOffset = termOffset - HEADER_LENGTH;

            buffer.wrap(termBuffer);
            dataHeader.wrap(buffer, headerOffset);
            final int frameLength = dataHeader.frameLength();
            if (frameLength == 0)
            {
                return false;
            }

            header.buffer(buffer);
            header.offset(headerOffset);

            handler.onFragment(buffer, termOffset, frameLength - HEADER_LENGTH, header);

            return true;
        }

        private boolean readBlock(final long position, final int requestedLength, final BlockHandler handler)
        {
            final int termId = computeTermIdFromPosition(position);
            final ByteBuffer termBuffer = termIdToBuffer.lookup(termId);
            if (termBuffer == null)
            {
                return false;
            }

            buffer.wrap(termBuffer);
            final int offset = computeTermOffsetFromPosition(position);
            final int remainder = termBuffer.capacity() - offset;
            final int length = Math.min(requestedLength, remainder);

            handler.onBlock(buffer, offset, length, sessionId, termId);

            return true;
        }

        private int computeTermOffsetFromPosition(final long position)
        {
            return LogBufferDescriptor.computeTermOffsetFromPosition(position, positionBitsToShift);
        }

        private int computeTermIdFromPosition(final long position)
        {
            return LogBufferDescriptor.computeTermIdFromPosition(position, positionBitsToShift, initialTermId);
        }

        public void close()
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
