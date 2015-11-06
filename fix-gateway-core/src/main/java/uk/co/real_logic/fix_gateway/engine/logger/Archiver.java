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

import uk.co.real_logic.aeron.Image;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.logbuffer.FileBlockHandler;
import uk.co.real_logic.agrona.CloseHelper;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.LangUtil;
import uk.co.real_logic.agrona.collections.IntLruCache;
import uk.co.real_logic.agrona.concurrent.Agent;
import uk.co.real_logic.fix_gateway.replication.StreamIdentifier;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static uk.co.real_logic.aeron.driver.Configuration.termBufferLength;
import static uk.co.real_logic.aeron.logbuffer.LogBufferDescriptor.computeTermIdFromPosition;
import static uk.co.real_logic.aeron.logbuffer.LogBufferDescriptor.computeTermOffsetFromPosition;

public class Archiver implements Agent, FileBlockHandler
{
    public static final long UNKNOWN_POSITION = -1;

    private static final int POLL_LENGTH = termBufferLength();

    private final ArchiveMetaData metaData;
    private final IntLruCache<SessionArchive> sessionIdToArchive;
    private final Subscription subscription;
    private final StreamIdentifier streamId;
    private final LogDirectoryDescriptor directoryDescriptor;

    public Archiver(
        final ArchiveMetaData metaData,
        final LogDirectoryDescriptor directoryDescriptor,
        final int loggerCacheCapacity,
        final Subscription subscription)
    {
        this.metaData = metaData;
        this.directoryDescriptor = directoryDescriptor;
        this.subscription = subscription;
        streamId = new StreamIdentifier(subscription);
        sessionIdToArchive = new IntLruCache<>(loggerCacheCapacity, sessionId ->
        {
            final Image image = subscription.getImage(sessionId);
            final int initialTermId = image.initialTermId();
            final int termBufferLength = image.termBufferLength();
            metaData.write(streamId, sessionId, initialTermId, termBufferLength);
            return new SessionArchive(sessionId, image);
        }, SessionArchive::close);
    }

    public int doWork()
    {
        return (int) subscription.filePoll(this, POLL_LENGTH);
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
        sessionIdToArchive
            .lookup(aeronSessionId)
            .archive(fileChannel, offset, length, termId);
    }

    public long positionOf(final int aeronSessionId)
    {
        final SessionArchive archive = sessionIdToArchive.lookup(aeronSessionId);

        if (archive == null)
        {
            return UNKNOWN_POSITION;
        }

        return archive.position();
    }

    public void patch(final int aeronSessionId,
                      final long position,
                      final DirectBuffer bodyBuffer,
                      final int bodyOffset,
                      final int bodyLength)
    {
        sessionIdToArchive
            .lookup(aeronSessionId)
            .patch(position, bodyBuffer, bodyOffset, bodyLength);
    }

    public void onClose()
    {
        subscription.close();
        sessionIdToArchive.close();
        metaData.close();
    }

    private final class SessionArchive implements AutoCloseable
    {
        public static final int UNKNOWN = -1;
        private final int sessionId;
        private final Image image;
        private final int termBufferLength;
        private final int positionBitsToShift;

        private int currentTermId = UNKNOWN;
        private RandomAccessFile currentLogFile;
        private FileChannel currentLogChannel;

        private SessionArchive(final int sessionId, final Image image)
        {
            this.sessionId = sessionId;
            this.image = image;
            termBufferLength = image.termBufferLength();
            positionBitsToShift = Integer.numberOfTrailingZeros(termBufferLength);
        }

        private void archive(
            final FileChannel fileChannel, final long offset, final int length, final int termId)
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

                if (fileChannel.transferTo(offset, length, currentLogChannel) != length)
                {
                    // TODO
                    System.err.println("Transfer to failure");
                }
            }
            catch (IOException e)
            {
                LangUtil.rethrowUnchecked(e);
            }
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

        private long position()
        {
            return image.position();
        }

        private void patch(
            final long position, final DirectBuffer bodyBuffer, final int bodyOffset, final int bodyLength)
        {
            try
            {
                final int patchTermId = computeTermIdFromPosition(position, positionBitsToShift, image.initialTermId());
                final int termOffset = computeTermOffsetFromPosition(position, positionBitsToShift);

                checkOverflow(bodyLength, termOffset);

                // Find the files to patch
                final RandomAccessFile patchTermLogFile;
                final FileChannel patchTermLogChannel;
                if (patchTermId == currentTermId)
                {
                    patchTermLogChannel = currentLogChannel;
                    patchTermLogFile = currentLogFile;
                }
                else
                {
                    final File file = logFile(patchTermId);
                    // NB: if file doesn't exist it gets created here
                    patchTermLogFile = openFile(file);
                    patchTermLogChannel = patchTermLogFile.getChannel();
                }

                writeToFile(
                    bodyBuffer, bodyOffset, bodyLength, termOffset, patchTermLogChannel, patchTermLogFile);

                close(patchTermLogChannel);
            }
            catch (IOException e)
            {
                LangUtil.rethrowUnchecked(e);
            }
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
            final int bodyOffset,
            final int bodyLength,
            int termOffset,
            final FileChannel patchTermLogChannel,
            final RandomAccessFile patchTermLogFile) throws IOException
        {
            final ByteBuffer byteBuffer = bodyBuffer.byteBuffer();
            if (byteBuffer != null)
            {
                byteBuffer
                    .position(bodyOffset)
                    .limit(bodyOffset + bodyLength);

                while (byteBuffer.remaining() > 0)
                {
                    termOffset += patchTermLogChannel.write(byteBuffer, termOffset);
                }
            }
            else
            {
                final byte[] bytes = bodyBuffer.byteArray();
                patchTermLogFile.seek(termOffset);
                patchTermLogFile.write(bytes, bodyOffset, bodyLength);
            }
        }

        private void close(final FileChannel patchTermLogChannel) throws IOException
        {
            if (patchTermLogChannel != currentLogChannel)
            {
                patchTermLogChannel.close();
            }
        }

        public void close()
        {
            CloseHelper.close(currentLogChannel);
        }
    }
}
