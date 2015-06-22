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

import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.logbuffer.FragmentHandler;
import uk.co.real_logic.aeron.logbuffer.Header;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.IoUtil;
import uk.co.real_logic.agrona.collections.IntLruCache;
import uk.co.real_logic.agrona.concurrent.Agent;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.replication.ReplicationStreams;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;

import static uk.co.real_logic.aeron.driver.Configuration.termBufferLength;
import static uk.co.real_logic.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;

public class Archiver implements Agent, FragmentHandler
{
    private static final int FRAGMENT_LIMIT = 10;

    private final IntLruCache<StreamArchive> streamIdToArchive;
    private final BufferFactory bufferFactory;
    private final ArchiveMetaData metaData;
    private final Subscription subscription;
    private final LogDirectoryDescriptor directoryDescriptor;

    public Archiver(
        final BufferFactory bufferFactory,
        final ReplicationStreams streams,
        final ArchiveMetaData metaData,
        final String logFileDir,
        final int loggerCacheCapacity)
    {
        this.bufferFactory = bufferFactory;
        this.metaData = metaData;
        directoryDescriptor = new LogDirectoryDescriptor(logFileDir);
        this.subscription = streams.dataSubscription();
        streamIdToArchive = new IntLruCache<>(loggerCacheCapacity, StreamArchive::new);
    }

    public void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        streamIdToArchive
            .lookup(header.streamId())
            .archive(buffer, offset, length, header);
    }

    private final class StreamArchive implements AutoCloseable
    {
        public static final int UNKNOWN = -1;
        private final UnsafeBuffer currentBuffer = new UnsafeBuffer(0, 0);
        private final int streamId;

        private ByteBuffer wrappedBuffer = null;

        private int initialTermId = UNKNOWN;
        private int currentTermId = UNKNOWN;

        private StreamArchive(final int streamId)
        {
            this.streamId = streamId;
        }

        private void archive(final DirectBuffer buffer, final int offset, final int length, final Header header)
        {
            if (initialTermId == UNKNOWN)
            {
                initialTermId = header.initialTermId();
                metaData.write(streamId, initialTermId, termBufferLength());
            }

            final int termId = header.termId();
            if (termId != currentTermId)
            {
                close();
                wrappedBuffer = bufferFactory.map(directoryDescriptor.logFile(streamId, termId), termBufferLength());
                currentBuffer.wrap(wrappedBuffer);
                currentTermId = termId;
            }

            final int headerOffset = offset - HEADER_LENGTH;
            final int totalLength = length + HEADER_LENGTH;
            currentBuffer.putBytes(headerOffset, buffer, headerOffset, totalLength);
        }

        public void close()
        {
            if (wrappedBuffer != null && wrappedBuffer instanceof MappedByteBuffer)
            {
                IoUtil.unmap((MappedByteBuffer)wrappedBuffer);
            }
        }
    }

    public int doWork() throws Exception
    {
        return subscription.poll(this, FRAGMENT_LIMIT);
    }

    public String roleName()
    {
        return "Archiver";
    }

    public void onClose()
    {
        subscription.close();
        streamIdToArchive.close();
    }
}
