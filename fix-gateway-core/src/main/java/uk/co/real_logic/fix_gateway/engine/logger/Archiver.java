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

import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.logbuffer.FileBlockHandler;
import uk.co.real_logic.agrona.CloseHelper;
import uk.co.real_logic.agrona.LangUtil;
import uk.co.real_logic.agrona.collections.IntLruCache;
import uk.co.real_logic.agrona.concurrent.Agent;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;

import static java.nio.file.StandardOpenOption.*;
import static uk.co.real_logic.aeron.driver.Configuration.termBufferLength;

public class Archiver implements Agent, FileBlockHandler
{
    private static final int FRAGMENT_LIMIT = 10;

    private final IntLruCache<SessionArchive> sessionIdToArchive;
    private final ArchiveMetaData metaData;
    private final Subscription subscription;
    private final LogDirectoryDescriptor directoryDescriptor;

    public Archiver(
        final ArchiveMetaData metaData,
        final String logFileDir,
        final int loggerCacheCapacity,
        final Subscription subscription)
    {
        this.metaData = metaData;
        directoryDescriptor = new LogDirectoryDescriptor(logFileDir);
        this.subscription = subscription;
        sessionIdToArchive = new IntLruCache<>(loggerCacheCapacity, sessionId ->
        {
            final int streamId = this.subscription.streamId();
            final int initialTermId = subscription.getImage(sessionId).initialTermId();
            metaData.write(streamId, sessionId, initialTermId, termBufferLength());
            return new SessionArchive(streamId, sessionId);
        }, SessionArchive::close);
    }

    public void onBlock(
        final FileChannel fileChannel,
        final long offset,
        final int length,
        final int sessionId,
        final int termId)
    {
        sessionIdToArchive
            .lookup(sessionId)
            .archive(fileChannel, offset, length, termId);
    }

    private final class SessionArchive implements AutoCloseable
    {
        public static final int UNKNOWN = -1;
        private final int streamId;
        private final int sessionId;

        private int currentTermId = UNKNOWN;
        private FileChannel currentLogFile;

        private SessionArchive(final int streamId, final int sessionId)
        {
            this.streamId = streamId;
            this.sessionId = sessionId;
        }

        private void archive(
            final FileChannel fileChannel, final long offset, final int length, final int termId)
        {
            try
            {
                if (termId != currentTermId)
                {
                    close();
                    final File file = directoryDescriptor.logFile(streamId, sessionId, termId);
                    currentLogFile = FileChannel.open(file.toPath(), CREATE, APPEND, WRITE);
                    currentTermId = termId;
                }

                if (fileChannel.transferTo(offset, length, currentLogFile) != length)
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

        public void close()
        {
            CloseHelper.close(currentLogFile);
        }
    }

    public int doWork() throws Exception
    {
        return (int) subscription.filePoll(this, termBufferLength());
    }

    public String roleName()
    {
        return "Archiver";
    }

    public void onClose()
    {
        subscription.close();
        sessionIdToArchive.close();
        metaData.close();
    }
}
