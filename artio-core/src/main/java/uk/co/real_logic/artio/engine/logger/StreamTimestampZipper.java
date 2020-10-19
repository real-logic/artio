/*
 * Copyright 2020 Monotonic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.co.real_logic.artio.engine.logger;

import io.aeron.Aeron;
import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import uk.co.real_logic.artio.ilink.ILinkMessageConsumer;
import uk.co.real_logic.artio.messages.FixMessageDecoder;
import uk.co.real_logic.artio.messages.ILinkMessageDecoder;
import uk.co.real_logic.artio.messages.MessageHeaderDecoder;
import uk.co.real_logic.artio.messages.ReplayerTimestampDecoder;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import static java.lang.Math.min;
import static uk.co.real_logic.artio.messages.FixMessageDecoder.metaDataHeaderLength;
import static uk.co.real_logic.artio.messages.FixMessageDecoder.metaDataSinceVersion;

public class StreamTimestampZipper
{
    private static final TimestampComparator TIMESTAMP_COMPARATOR = new TimestampComparator();
    private static final OffsetComparator OFFSET_COMPARATOR = new OffsetComparator();

    private final int compactionSize;
    private final StreamPoller[] pollers;
    private final FragmentAssembler fragmentAssembler;
    private final LogEntryHandler logEntryHandler;

    private final List<BufferedPosition> positions = new ArrayList<>();
    private final ExpandableArrayBuffer reorderBuffer = new ExpandableArrayBuffer();
    private int reorderBufferOffset;

    public StreamTimestampZipper(
        final Aeron aeron,
        final String libraryAeronChannel,
        final FixMessageConsumer fixMessageConsumer,
        final ILinkMessageConsumer iLinkMessageConsumer,
        final int compactionSize,
        final int... streamIds)
    {
        this.compactionSize = compactionSize;
        pollers = new StreamPoller[streamIds.length];
        for (int i = 0; i < streamIds.length; i++)
        {
            final int streamId = streamIds[i];
            pollers[i] = new StreamPoller(aeron.addSubscription(libraryAeronChannel, streamId));
        }
        logEntryHandler = new LogEntryHandler(fixMessageConsumer, iLinkMessageConsumer);
        fragmentAssembler = new FragmentAssembler(logEntryHandler);
    }

    public int poll()
    {
        int read = 0;
        final StreamPoller[] pollers = this.pollers;
        final int size = pollers.length;
        for (int i = 0; i < size; i++)
        {
            read += pollers[i].poll(pollers, fragmentAssembler);
        }

        read += processReorderBuffer(pollers);

        compact();

        return read;
    }

    private int processReorderBuffer(final StreamPoller[] pollers)
    {
        int read = 0;
        positions.sort(TIMESTAMP_COMPARATOR);
        final Iterator<BufferedPosition> it = positions.iterator();
        while (it.hasNext())
        {
            final BufferedPosition position = it.next();
            final long timestamp = position.timestamp;
            final long minHandleTimestamp = findMinOtherTimestamp(pollers, position.owner);

            if (timestamp <= minHandleTimestamp)
            {
                logEntryHandler.onBufferedMessage(position.offset, position.length);
                read++;
                it.remove();

                updateOwnerPosition(position);
            }
            else
            {
                break;
            }
        }
        return read;
    }

    private void compact()
    {
        if (reorderBufferOffset > compactionSize)
        {
            positions.sort(OFFSET_COMPARATOR);
            int reorderBufferOffset = 0;
            for (final BufferedPosition position : positions)
            {
                final int offset = position.offset;
                // No compaction possible
                if (offset == reorderBufferOffset)
                {
                    return;
                }

                final int length = position.length;
                reorderBuffer.putBytes(reorderBufferOffset, reorderBuffer, offset, length);
                reorderBufferOffset += length;
            }
            this.reorderBufferOffset = reorderBufferOffset;
        }
    }

    public int bufferPosition()
    {
        return reorderBufferOffset;
    }

    public int bufferCapacity()
    {
        return reorderBuffer.capacity();
    }

    private void updateOwnerPosition(final BufferedPosition position)
    {
        final StreamPoller owner = position.owner;
        for (final BufferedPosition remainingPosition : positions)
        {
            if (remainingPosition.owner == owner)
            {
                owner.minBufferedTimestamp = remainingPosition.timestamp;
                return;
            }
        }

        // If there is nothing else in the queue make it the max handled position
        owner.minBufferedTimestamp = position.timestamp;
    }

    public void onClose()
    {
        final LogEntryHandler logEntryHandler = this.logEntryHandler;
        final List<BufferedPosition> positions = this.positions;
        final int size = positions.size();

        positions.sort(TIMESTAMP_COMPARATOR);

        for (int i = 0; i < size; i++)
        {
            final BufferedPosition position = positions.get(i);
            logEntryHandler.onBufferedMessage(position.offset, position.length);
        }

        positions.clear();
        reorderBufferOffset = 0;
    }

    static class BufferedPosition
    {
        final StreamPoller owner;
        final long timestamp;
        final int offset;
        final int length;

        BufferedPosition(final StreamPoller owner, final long timestamp, final int offset, final int length)
        {
            this.owner = owner;
            this.timestamp = timestamp;
            this.offset = offset;
            this.length = length;
        }

        public String toString()
        {
            return "BufferedPosition{" +
                "owner=" + owner.subscription.streamId() +
                ", timestamp=" + timestamp +
                ", offset=" + offset +
                ", length=" + length +
                '}';
        }
    }

    static class TimestampComparator implements Comparator<BufferedPosition>
    {
        public int compare(final BufferedPosition o1, final BufferedPosition o2)
        {
            return Long.compare(o1.timestamp, o2.timestamp);
        }
    }

    static class OffsetComparator implements Comparator<BufferedPosition>
    {
        public int compare(final BufferedPosition o1, final BufferedPosition o2)
        {
            return Long.compare(o1.offset, o2.offset);
        }
    }

    class StreamPoller
    {
        private final Subscription subscription;
        // initially 0, if no timestamps buffered then it was the max handled timestamp
        private long minBufferedTimestamp;

        StreamPoller(final Subscription subscription)
        {
            this.subscription = subscription;
        }

        public int poll(final StreamPoller[] pollers, final FragmentAssembler fragmentAssembler)
        {
            final long minOtherTimestamp = findMinOtherTimestamp(pollers, this);
            logEntryHandler.reset(minOtherTimestamp, this);

            final int read = subscription.poll(fragmentAssembler, 10);

            final long minBufferedTimestampByPoll = logEntryHandler.minBufferedTimestamp;
            if (minBufferedTimestampByPoll != 0)
            {
                minBufferedTimestamp = minBufferedTimestampByPoll;
            }
            return read;
        }
    }

    private static long findMinOtherTimestamp(final StreamPoller[] pollers, final StreamPoller owner)
    {
        long minOtherTimestamp = Long.MAX_VALUE;
        for (int i = 0; i < pollers.length; i++)
        {
            final StreamPoller poller = pollers[i];
            if (poller != owner)
            {
                minOtherTimestamp = min(minOtherTimestamp, poller.minBufferedTimestamp);
            }
        }
        return minOtherTimestamp;
    }

    class LogEntryHandler implements FragmentHandler
    {
        private final MessageHeaderDecoder messageHeader = new MessageHeaderDecoder();
        private final FixMessageDecoder fixMessage = new FixMessageDecoder();
        private final ILinkMessageDecoder iLinkMessage = new ILinkMessageDecoder();
        private final ReplayerTimestampDecoder replayerTimestamp = new ReplayerTimestampDecoder();

        private final FixMessageConsumer fixHandler;
        private final ILinkMessageConsumer iLinkHandler;

        StreamPoller owner;
        long maxTimestampToHandle;
        long minBufferedTimestamp;

        LogEntryHandler(final FixMessageConsumer fixHandler, final ILinkMessageConsumer iLinkHandler)
        {
            this.fixHandler = fixHandler;
            this.iLinkHandler = iLinkHandler;
        }

        public void onFragment(
            final DirectBuffer buffer, final int start, final int length, final Header header)
        {
            int offset = start;
            messageHeader.wrap(buffer, offset);
            final int templateId = messageHeader.templateId();
            final int blockLength = messageHeader.blockLength();
            final int version = messageHeader.version();

            if (templateId == FixMessageDecoder.TEMPLATE_ID)
            {
                offset += MessageHeaderDecoder.ENCODED_LENGTH;

                fixMessage.wrap(buffer, offset, blockLength, version);

                if (version >= metaDataSinceVersion())
                {
                    offset += metaDataHeaderLength() + fixMessage.metaDataLength();
                    fixMessage.skipMetaData();
                }

                final long timestamp = fixMessage.timestamp();

                // hand off the first message you see, otherwise buffer it.
                if (timestamp <= maxTimestampToHandle)
                {
                    fixHandler.onMessage(fixMessage, buffer, offset, length, header);
                }
                else
                {
                    if (minBufferedTimestamp == 0)
                    {
                        minBufferedTimestamp = timestamp;
                    }

                    reorderBuffer.putBytes(reorderBufferOffset, buffer, start, length);
                    positions.add(new BufferedPosition(owner, timestamp, reorderBufferOffset, length));
                    reorderBufferOffset += length;
                }
            }
            else if (templateId == ReplayerTimestampDecoder.TEMPLATE_ID)
            {
                offset += MessageHeaderDecoder.ENCODED_LENGTH;

                replayerTimestamp.wrap(buffer, offset, blockLength, version);
                final long timestamp = replayerTimestamp.timestamp();
                if (minBufferedTimestamp == 0)
                {
                    minBufferedTimestamp = timestamp;
                }
            }
            else if (templateId == ILinkMessageDecoder.TEMPLATE_ID)
            {
                offset += MessageHeaderDecoder.ENCODED_LENGTH;

                iLinkMessage.wrap(buffer, offset, blockLength, version);

                offset += ILinkMessageDecoder.BLOCK_LENGTH;

                final long timestamp = iLinkMessage.enqueueTime();

                if (timestamp <= maxTimestampToHandle)
                {
                    iLinkHandler.onBusinessMessage(iLinkMessage, buffer, offset, header);
                }
                else
                {
                    if (minBufferedTimestamp == 0)
                    {
                        minBufferedTimestamp = timestamp;
                    }

                    reorderBuffer.putBytes(reorderBufferOffset, buffer, start, length);
                    positions.add(new BufferedPosition(owner, timestamp, reorderBufferOffset, length));
                    reorderBufferOffset += length;
                }
            }
        }

        void reset(final long minOtherTimestamp, final StreamPoller owner)
        {
            maxTimestampToHandle = minOtherTimestamp;
            minBufferedTimestamp = 0;
            this.owner = owner;
        }

        public void onBufferedMessage(final int start, final int length)
        {
            final ExpandableArrayBuffer buffer = StreamTimestampZipper.this.reorderBuffer;
            int offset = start;
            messageHeader.wrap(buffer, offset);
            final int templateId = messageHeader.templateId();
            final int blockLength = messageHeader.blockLength();
            final int version = messageHeader.version();

            if (templateId == FixMessageDecoder.TEMPLATE_ID)
            {
                offset += MessageHeaderDecoder.ENCODED_LENGTH;

                fixMessage.wrap(buffer, offset, blockLength, version);

                if (version >= metaDataSinceVersion())
                {
                    offset += metaDataHeaderLength() + fixMessage.metaDataLength();
                    fixMessage.skipMetaData();
                }

                fixHandler.onMessage(fixMessage, buffer, offset, length, null);
            }
            else if (templateId == ILinkMessageDecoder.TEMPLATE_ID)
            {
                offset += MessageHeaderDecoder.ENCODED_LENGTH;

                iLinkMessage.wrap(buffer, offset, blockLength, version);

                offset += ILinkMessageDecoder.BLOCK_LENGTH;

                iLinkHandler.onBusinessMessage(iLinkMessage, buffer, offset, null);
            }
        }
    }
}
