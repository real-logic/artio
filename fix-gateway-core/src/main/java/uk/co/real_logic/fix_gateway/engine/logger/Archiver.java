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
import io.aeron.logbuffer.FileBlockHandler;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.LangUtil;
import org.agrona.collections.Int2ObjectCache;
import org.agrona.concurrent.Agent;
import uk.co.real_logic.fix_gateway.replication.StreamIdentifier;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.function.IntFunction;
import java.util.zip.CRC32;

import static io.aeron.driver.Configuration.termBufferLength;
import static io.aeron.logbuffer.LogBufferDescriptor.computePosition;

public class Archiver implements Agent, FileBlockHandler
{
    private static final int POLL_LENGTH = termBufferLength();

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

        return (int) subscription.filePoll(this, POLL_LENGTH);
    }

    private SessionArchiver newSessionArchiver(final int sessionId)
    {
        final Image image = subscription.getImage(sessionId);
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
        final long offset,
        final int length,
        final int aeronSessionId,
        final int termId)
    {
        session(aeronSessionId).onBlock(fileChannel, offset, length, aeronSessionId, termId);
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
        subscription.close();
        sessionIdToArchive.clear();
        metaData.close();
    }

    public class SessionArchiver implements AutoCloseable, FileBlockHandler
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
            return image.filePoll(this, POLL_LENGTH);
        }

        public void onBlock(
            final FileChannel fileChannel, final long offset, final int length, final int sessionId, final int termId)
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

                final long transferred = fileChannel.transferTo(offset, length, currentLogChannel);
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
                    bodyBuffer, readOffset, bodyLength, termWriteOffset, patchTermLogChannel, patchTermLogFile, header);

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
            final RandomAccessFile patchTermLogFile,
            final DataHeaderFlyweight header) throws IOException
        {
            checksum.reset();

            final ByteBuffer byteBuffer = bodyBuffer.byteBuffer();
            if (byteBuffer != null)
            {
                byteBuffer
                    .limit(readOffset + bodyLength)
                    .position(readOffset);

                checksum.update(byteBuffer);
                writeChecksum(header);

                while (byteBuffer.remaining() > 0)
                {
                    termWriteOffset += patchTermLogChannel.write(byteBuffer, termWriteOffset);
                }
            }
            else
            {
                final byte[] bytes = bodyBuffer.byteArray();
                checksum.update(bytes, readOffset, bodyLength);
                writeChecksum(header);
                patchTermLogFile.seek(termWriteOffset);
                patchTermLogFile.write(bytes, readOffset, bodyLength);
            }
        }

        private void writeChecksum(final DataHeaderFlyweight header)
        {
            header.reservedValue((int) checksum.getValue());
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
