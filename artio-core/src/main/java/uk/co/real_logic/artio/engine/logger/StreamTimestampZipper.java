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

import io.aeron.FragmentAssembler;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.ExpandableArrayBuffer;
import uk.co.real_logic.artio.ArtioLogHeader;
import uk.co.real_logic.artio.ilink.ILinkMessageConsumer;
import uk.co.real_logic.artio.messages.FixMessageDecoder;
import uk.co.real_logic.artio.messages.FixPMessageDecoder;
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
        final FixMessageConsumer fixMessageConsumer,
        final ILinkMessageConsumer iLinkMessageConsumer,
        final int compactionSize,
        final Poller... pollers)
    {
        this.compactionSize = compactionSize;
        this.pollers = new StreamPoller[pollers.length];
        for (int i = 0; i < pollers.length; i++)
        {
            this.pollers[i] = new StreamPoller(pollers[i]);
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
            final long timestampLowWaterMark = findMinLowWaterMark(pollers, position.owner);

            if (timestamp <= timestampLowWaterMark)
            {
                position.owner.handledTimestamp(timestamp);
                logEntryHandler.owner = position.owner;
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
                position.offset = reorderBufferOffset;
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
                // don't go through method here because this might increase the min buffered timestamp.
                owner.minBufferedTimestamp = remainingPosition.timestamp;
                return;
            }
        }

        owner.nothingBuffered();
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
            logEntryHandler.owner = position.owner;
            logEntryHandler.onBufferedMessage(position.offset, position.length);
        }

        positions.clear();
        reorderBufferOffset = 0;

        for (final StreamPoller poller : pollers)
        {
            poller.close();
        }
    }

    static class BufferedPosition
    {
        final StreamPoller owner;
        final long timestamp;
        int offset;
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
                "owner=" + owner.poller.streamId() +
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
        private static final long NOTHING_BUFFERED = -1;

        private final ArtioLogHeader header;
        private final Poller poller;
        private long minBufferedTimestamp = NOTHING_BUFFERED;
        private long maxHandledTimestamp;

        StreamPoller(final Poller poller)
        {
            this.poller = poller;
            header = new ArtioLogHeader(poller.streamId());
        }

        public int poll(final StreamPoller[] pollers, final FragmentAssembler fragmentAssembler)
        {
            final long minOtherTimestamp = findMinLowWaterMark(pollers, this);
            logEntryHandler.reset(minOtherTimestamp, this);
            return poller.poll(fragmentAssembler);
        }

        // This is the position at which it is safe for other streams to emit below.
        long timestampLowWaterMark()
        {
            return minBufferedTimestamp == NOTHING_BUFFERED ? maxHandledTimestamp : minBufferedTimestamp;
        }

        void handledTimestamp(final long timestamp)
        {
            // always >= previous timestamp.
            maxHandledTimestamp = timestamp;
        }

        void bufferedTimestamp(final long timestamp)
        {
            if (minBufferedTimestamp == NOTHING_BUFFERED)
            {
                minBufferedTimestamp = timestamp;
            }
            else
            {
                minBufferedTimestamp = min(minBufferedTimestamp, timestamp);
            }
        }

        public void nothingBuffered()
        {
            minBufferedTimestamp = NOTHING_BUFFERED;
        }

        public String toString()
        {
            return "StreamPoller{" +
                "header=" + header +
                '}';
        }

        public void close()
        {
            poller.close();
        }
    }

    private static long findMinLowWaterMark(final StreamPoller[] pollers, final StreamPoller owner)
    {
        long timestampLowWaterMark = Long.MAX_VALUE;
        for (int i = 0; i < pollers.length; i++)
        {
            final StreamPoller poller = pollers[i];
            if (poller != owner)
            {
                timestampLowWaterMark = min(timestampLowWaterMark, poller.timestampLowWaterMark());
            }
        }
        return timestampLowWaterMark;
    }

    class LogEntryHandler implements FragmentHandler
    {
        private final MessageHeaderDecoder messageHeader = new MessageHeaderDecoder();
        private final FixMessageDecoder fixMessage = new FixMessageDecoder();
        private final FixPMessageDecoder iLinkMessage = new FixPMessageDecoder();
        private final ReplayerTimestampDecoder replayerTimestamp = new ReplayerTimestampDecoder();

        private final FixMessageConsumer fixHandler;
        private final ILinkMessageConsumer iLinkHandler;

        StreamPoller owner;
        long maxTimestampToHandle;

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
                    owner.handledTimestamp(timestamp);
                    fixHandler.onMessage(fixMessage, buffer, offset, length, owner.header);
                }
                else
                {
                    putBufferedMessage(buffer, start, length, timestamp);
                }
            }
            else if (templateId == ReplayerTimestampDecoder.TEMPLATE_ID)
            {
                offset += MessageHeaderDecoder.ENCODED_LENGTH;

                replayerTimestamp.wrap(buffer, offset, blockLength, version);
                final long timestamp = replayerTimestamp.timestamp();
                owner.handledTimestamp(timestamp);
            }
            else if (templateId == FixPMessageDecoder.TEMPLATE_ID)
            {
                offset += MessageHeaderDecoder.ENCODED_LENGTH;

                iLinkMessage.wrap(buffer, offset, blockLength, version);

                offset += FixPMessageDecoder.BLOCK_LENGTH;

                final long timestamp = iLinkMessage.enqueueTime();

                if (timestamp <= maxTimestampToHandle)
                {
                    owner.handledTimestamp(timestamp);
                    iLinkHandler.onBusinessMessage(iLinkMessage, buffer, offset, owner.header);
                }
                else
                {
                    putBufferedMessage(buffer, start, length, timestamp);
                }
            }
        }

        private void putBufferedMessage(
            final DirectBuffer buffer, final int start, final int length, final long timestamp)
        {
            owner.bufferedTimestamp(timestamp);
            reorderBuffer.putBytes(reorderBufferOffset, buffer, start, length);
            positions.add(new BufferedPosition(owner, timestamp, reorderBufferOffset, length));
            reorderBufferOffset += length;
        }

        void reset(final long minOtherTimestamp, final StreamPoller owner)
        {
            maxTimestampToHandle = minOtherTimestamp;
            this.owner = owner;
        }

        public void onBufferedMessage(final int start, final int length)
        {
            int offset = start;

            final ExpandableArrayBuffer buffer = StreamTimestampZipper.this.reorderBuffer;
            final MessageHeaderDecoder messageHeader = this.messageHeader;
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

                fixHandler.onMessage(fixMessage, buffer, offset, length, owner.header);
            }
            else if (templateId == FixPMessageDecoder.TEMPLATE_ID)
            {
                offset += MessageHeaderDecoder.ENCODED_LENGTH;

                iLinkMessage.wrap(buffer, offset, blockLength, version);

                offset += FixPMessageDecoder.BLOCK_LENGTH;

                iLinkHandler.onBusinessMessage(iLinkMessage, buffer, offset, owner.header);
            }
        }
    }

    public interface Poller
    {
        int poll(FragmentAssembler fragmentAssembler);

        int streamId();

        void close();
    }
}
