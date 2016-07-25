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

import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.logbuffer.RawBlockHandler;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.LangUtil;
import org.agrona.collections.Int2ObjectCache;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.replication.ReservedValue;
import uk.co.real_logic.fix_gateway.replication.StreamIdentifier;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.function.IntFunction;
import java.util.zip.CRC32;

import static io.aeron.driver.Configuration.TERM_BUFFER_LENGTH_DEFAULT;
import static io.aeron.logbuffer.LogBufferDescriptor.computePosition;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;

public class Archiver implements Agent, RawBlockHandler
{
    private static final int POLL_LENGTH = TERM_BUFFER_LENGTH_DEFAULT;

    public static final long UNKNOWN_POSITION = -1;

    private final IntFunction<SessionArchiver> newSessionArchiver = this::newSessionArchiver;
    private final ArchiveMetaData metaData;
    private final Int2ObjectCache<SessionArchiver> sessionIdToArchive;
    private final StreamIdentifier streamId;
    private final LogDirectoryDescriptor directoryDescriptor;
    private final CRC32 checksum = new CRC32();

    private DataHeaderFlyweight header = new DataHeaderFlyweight();
    private Subscription subscription;

    public Archiver(
        final ArchiveMetaData metaData,
        final int cacheNumSets,
        final int cacheSetSize,
        final StreamIdentifier streamId)
    {
        this.metaData = metaData;
        this.directoryDescriptor = metaData.directoryDescriptor();
        this.streamId = streamId;
        sessionIdToArchive = new Int2ObjectCache<>(cacheNumSets, cacheSetSize, SessionArchiver::close);
    }

    public Archiver subscription(final Subscription subscription)
    {
        this.subscription = subscription;
        return this;
    }

    public int doWork()
    {
        if (subscription == null)
        {
            return 0;
        }

        return (int) subscription.rawPoll(this, POLL_LENGTH);
    }

    private SessionArchiver newSessionArchiver(final int sessionId)
    {
        final Image image = subscription.imageBySessionId(sessionId);
        if (image == null)
        {
            return null;
        }

        final int initialTermId = image.initialTermId();
        final int termBufferLength = image.termBufferLength();
        metaData.write(streamId, sessionId, initialTermId, termBufferLength);
        return new SessionArchiver(sessionId, image);
    }

    public String roleName()
    {
        return "Archiver";
    }

    public void onBlock(
        final FileChannel fileChannel,
        final long fileOffset,
        final UnsafeBuffer termBuffer,
        final int termOffset,
        final int length,
        final int aeronSessionId,
        final int termId)
    {
        session(aeronSessionId).onBlock(
            fileChannel, fileOffset, termBuffer, termOffset, length, aeronSessionId, termId);
    }

    public long positionOf(final int aeronSessionId)
    {
        final SessionArchiver archive = session(aeronSessionId);

        if (archive == null)
        {
            return UNKNOWN_POSITION;
        }

        return archive.archivedPosition();
    }

    public boolean patch(
        final int aeronSessionId,
        final DirectBuffer bodyBuffer,
        final int bodyOffset,
        final int bodyLength)
    {
        return session(aeronSessionId).patch(bodyBuffer, bodyOffset, bodyLength);
    }

    public SessionArchiver session(final int sessionId)
    {
        return sessionIdToArchive.computeIfAbsent(sessionId, newSessionArchiver);
    }

    public void onClose()
    {
        CloseHelper.close(subscription);
        sessionIdToArchive.clear();
        metaData.close();
    }

    public class SessionArchiver implements AutoCloseable, RawBlockHandler
    {
        public static final int UNKNOWN = -1;
        private final int sessionId;
        private final Image image;
        private final int termBufferLength;
        private final int positionBitsToShift;

        private int currentTermId = UNKNOWN;
        private RandomAccessFile currentLogFile;
        private FileChannel currentLogChannel;

        protected SessionArchiver(final int sessionId, final Image image)
        {
            this.sessionId = sessionId;
            this.image = image;
            termBufferLength = image.termBufferLength();
            positionBitsToShift = Integer.numberOfTrailingZeros(termBufferLength);
        }

        public int poll()
        {
            return image.rawPoll(this, POLL_LENGTH);
        }

        public void onBlock(
            final FileChannel fileChannel,
            final long fileOffset,
            final UnsafeBuffer termBuffer,
            int termOffset,
            final int length,
            final int sessionId,
            final int termId)
        {
            try
            {
                if (termId != currentTermId)
                {
                    close();
                    final File location = logFile(termId);
                    currentLogFile = openFile(location);
                    currentLogChannel = currentLogFile.getChannel();
                    currentTermId = termId;
                }

                writeChecksumForBlock(termBuffer, termOffset, length);

                final long transferred = fileChannel.transferTo(fileOffset, length, currentLogChannel);
                if (transferred != length)
                {
                    final File location = logFile(termId);
                    throw new IllegalStateException(String.format(
                        "Failed to transfer %d bytes to %s, only transferred %d bytes",
                        length,
                        location,
                        transferred));
                }
            }
            catch (IOException e)
            {
                LangUtil.rethrowUnchecked(e);
            }
        }

        private void writeChecksumForBlock(final UnsafeBuffer termBuffer, int termOffset, final int length)
        {
            final ByteBuffer byteBuffer = termBuffer.byteBuffer();
            final int end = termOffset + length - HEADER_LENGTH;
            int remaining = length;
            while (termOffset < end)
            {
                header.wrap(termBuffer, termOffset, remaining);
                final int frameLength = header.frameLength();
                final int messageOffset = termOffset + HEADER_LENGTH;
                checksum.reset();

                if (byteBuffer != null)
                {
                    // This offset is as a result of the bytebuffer not having a one-to-one mapping
                    // to UnsafeBuffer instances as of Aeron 1.0
                    final int bufferAdjustmentOffset =
                        (int) (termBuffer.addressOffset() - ((sun.nio.ch.DirectBuffer) byteBuffer).address());
                    final int limit = termOffset + frameLength;
                    if (messageOffset > limit)
                    {
                        throw new IllegalArgumentException(
                            String.format("%d is > than %d or < 0", messageOffset, limit));
                    }
                    byteBuffer
                        .limit(limit + bufferAdjustmentOffset)
                        .position(messageOffset + bufferAdjustmentOffset);

                    checksum.update(byteBuffer);
                }
                else
                {
                    final int messageLength = frameLength - HEADER_LENGTH;
                    final byte[] bytes = termBuffer.byteArray();

                    checksum.update(bytes, messageOffset, messageLength);
                }

                writeChecksum(header);

                final int alignedFrameLength = ArchiveDescriptor.alignTerm(frameLength);
                termOffset += alignedFrameLength;
                remaining -= alignedFrameLength;
            }
        }

        public long archivedPosition()
        {
            return image.position();
        }

        public boolean patch(
            final DirectBuffer bodyBuffer, final int readOffset, final int bodyLength)
        {
            header.wrap(bodyBuffer, readOffset, bodyLength);
            final int termId = header.termId();
            final int termWriteOffset = header.termOffset();
            final long position = computePosition(termId, termWriteOffset, positionBitsToShift, image.initialTermId());

            if (position + bodyLength >= archivedPosition())
            {
                // Can only patch historic files
                return false;
            }

            try
            {
                checkOverflow(bodyLength, termWriteOffset);

                // Find the files to patch
                final RandomAccessFile patchTermLogFile;
                final FileChannel patchTermLogChannel;
                if (termId == currentTermId)
                {
                    patchTermLogChannel = currentLogChannel;
                    patchTermLogFile = currentLogFile;
                }
                else
                {
                    // if file doesn't exist it gets created here
                    final File file = logFile(termId);
                    patchTermLogFile = openFile(file);
                    patchTermLogChannel = patchTermLogFile.getChannel();
                }

                writeToFile(
                    bodyBuffer, readOffset, bodyLength, termWriteOffset, patchTermLogChannel, patchTermLogFile);

                close(patchTermLogChannel);

                return true;
            }
            catch (IOException e)
            {
                LangUtil.rethrowUnchecked(e);
                return false;
            }
        }

        public void close()
        {
            CloseHelper.close(currentLogChannel);
        }

        private RandomAccessFile openFile(final File location) throws IOException
        {
            final RandomAccessFile file = new RandomAccessFile(location, "rwd");
            file.setLength(termBufferLength);
            return file;
        }

        private File logFile(final int termId)
        {
            return directoryDescriptor.logFile(streamId, sessionId, termId);
        }

        private void checkOverflow(final int bodyLength, final int termOffset)
        {
            if (termOffset + bodyLength > termBufferLength)
            {
                throw new IllegalArgumentException("Unable to write patch beyond the length of the log buffer");
            }
        }

        private void writeToFile(
            final DirectBuffer bodyBuffer,
            final int readOffset,
            final int bodyLength,
            int termWriteOffset,
            final FileChannel patchTermLogChannel,
            final RandomAccessFile patchTermLogFile) throws IOException
        {
            final int messageOffset = readOffset + HEADER_LENGTH;
            checksum.reset();

            final ByteBuffer byteBuffer = bodyBuffer.byteBuffer();
            if (byteBuffer != null)
            {
                // Update Checksum
                final int limit = readOffset + bodyLength;
                byteBuffer.limit(limit).position(messageOffset);
                checksum.update(byteBuffer);
                writeChecksum(header);

                // Write patch
                byteBuffer.limit(limit).position(readOffset);
                while (byteBuffer.remaining() > 0)
                {
                    termWriteOffset += patchTermLogChannel.write(byteBuffer, termWriteOffset);
                }
            }
            else
            {
                // Update Checksum
                final byte[] bytes = bodyBuffer.byteArray();
                checksum.update(bytes, messageOffset, bodyLength - HEADER_LENGTH);

                writeChecksum(header);

                // Write patch
                patchTermLogFile.seek(termWriteOffset);
                patchTermLogFile.write(bytes, readOffset, bodyLength);
            }
        }

        private void writeChecksum(final DataHeaderFlyweight header)
        {
            final int clusterStreamId = ReservedValue.clusterStreamId(header.reservedValue());
            final int checksumValue = (int) checksum.getValue();
            header.reservedValue(ReservedValue.of(clusterStreamId, checksumValue));
        }

        private void close(final FileChannel patchTermLogChannel) throws IOException
        {
            if (patchTermLogChannel != currentLogChannel)
            {
                patchTermLogChannel.close();
            }
        }
    }
}
