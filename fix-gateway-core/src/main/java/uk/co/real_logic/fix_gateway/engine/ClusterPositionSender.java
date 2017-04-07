/*
 * Copyright 2014 Real Logic Ltd.
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
package uk.co.real_logic.fix_gateway.engine;

import io.aeron.Subscription;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import io.aeron.logbuffer.Header;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.DirectBuffer;
import org.agrona.collections.Int2IntHashMap;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.concurrent.Agent;
import uk.co.real_logic.fix_gateway.DebugLogger;
import uk.co.real_logic.fix_gateway.engine.logger.ArchiveDescriptor;
import uk.co.real_logic.fix_gateway.engine.logger.Archiver.ArchivedPositionHandler;
import uk.co.real_logic.fix_gateway.messages.*;
import uk.co.real_logic.fix_gateway.protocol.GatewayPublication;
import uk.co.real_logic.fix_gateway.replication.ClusterFragmentHandler;
import uk.co.real_logic.fix_gateway.replication.ClusterHeader;
import uk.co.real_logic.fix_gateway.replication.ClusterableSubscription;

import java.util.Arrays;
import java.util.function.IntFunction;
import java.util.stream.IntStream;

import static uk.co.real_logic.fix_gateway.LogTag.POSITION;

// TODO: identify how to improve liveness in the situation that no messages
// are replicated for a long term once a stream has been replicated.
class ClusterPositionSender implements Agent, ArchivedPositionHandler
{
    static final int DEFAULT_INTERVAL_COUNT = 4;

    private static final int HEADER_LENGTH = MessageHeaderDecoder.ENCODED_LENGTH;
    private static final int LIMIT = 10;

    private static final int MISSING = -1;

    private final MessageHeaderDecoder messageHeader = new MessageHeaderDecoder();
    private final FixMessageDecoder fixMessage = new FixMessageDecoder();
    private final DisconnectDecoder disconnect = new DisconnectDecoder();
    private final ReplicatedMessageDecoder replicatedMessage = new ReplicatedMessageDecoder();
    private final LibraryConnectDecoder libraryConnect = new LibraryConnectDecoder();
    private final ApplicationHeartbeatDecoder applicationHeartbeat = new ApplicationHeartbeatDecoder();
    private final LibraryTimeoutDecoder libraryTimeout = new LibraryTimeoutDecoder();

    private final Int2ObjectHashMap<LibraryPositions> libraryIdToPosition = new Int2ObjectHashMap<>();
    private final Long2LongHashMap aeronSessionIdToArchivedPosition = new Long2LongHashMap(MISSING);
    private final Int2IntHashMap aeronSessionIdToLibraryId = new Int2IntHashMap(MISSING);

    private final Subscription outboundLibrarySubscription;
    private final ControlledFragmentHandler onLibraryFragmentFunc = this::onLibraryFragment;
    private final ClusterableSubscription outboundClusterSubscription;
    private final ClusterFragmentHandler onClusterFragmentFunc = this::onClusterFragment;
    private final IntFunction<LibraryPositions> newLibraryPositionsFunc = LibraryPositions::new;
    private final GatewayPublication inboundLibraryPublication;
    private final String agentNamePrefix;

    ClusterPositionSender(
        final Subscription outboundLibrarySubscription,
        final ClusterableSubscription outboundClusterSubscription,
        final GatewayPublication inboundLibraryPublication,
        final String agentNamePrefix)
    {
        this.outboundLibrarySubscription = outboundLibrarySubscription;
        this.outboundClusterSubscription = outboundClusterSubscription;
        this.inboundLibraryPublication = inboundLibraryPublication;
        this.agentNamePrefix = agentNamePrefix;
    }

    public int doWork() throws Exception
    {
        return pollCommands() + checkConditions();
    }

    private int pollCommands()
    {
        return outboundLibrarySubscription.controlledPoll(onLibraryFragmentFunc, LIMIT) +
            outboundClusterSubscription.poll(onClusterFragmentFunc, LIMIT);
    }

    @SuppressWarnings("FinalParameters")
    private Action onLibraryFragment(final DirectBuffer buffer, int offset, final int length, final Header header)
    {
        messageHeader.wrap(buffer, offset);

        offset += HEADER_LENGTH;

        final int blockLength = messageHeader.blockLength();
        final int version = messageHeader.version();
        final int aeronSessionId = header.sessionId();

        switch (messageHeader.templateId())
        {
            case LibraryConnectDecoder.TEMPLATE_ID:
            {
                libraryConnect.wrap(buffer, offset, blockLength, version);
                onLibraryConnect(aeronSessionId, libraryConnect.libraryId());
                break;
            }

            case ApplicationHeartbeatDecoder.TEMPLATE_ID:
            {
                applicationHeartbeat.wrap(buffer, offset, blockLength, version);
                onApplicationHeartbeat(aeronSessionId, applicationHeartbeat.libraryId());
                break;
            }

            case LibraryTimeoutDecoder.TEMPLATE_ID:
            {
                libraryTimeout.wrap(buffer, offset, blockLength, version);
                onLibraryTimeout(aeronSessionId, libraryTimeout.libraryId());
                break;
            }
        }

        return Action.CONTINUE;
    }

    @SuppressWarnings("FinalParameters")
    private Action onClusterFragment(
        final DirectBuffer buffer, int offset, final int length, final ClusterHeader header)
    {
        offset = wrapHeader(buffer, offset);

        if (messageHeader.templateId() == ReplicatedMessageDecoder.TEMPLATE_ID)
        {
            replicatedMessage.wrap(
                buffer, offset, messageHeader.blockLength(), messageHeader.blockLength());

            final long position = replicatedMessage.position();

            offset += ReplicatedMessageDecoder.BLOCK_LENGTH;

            offset = wrapHeader(buffer, offset);

            final int version = messageHeader.version();
            final int actingBlockLength = messageHeader.blockLength();
            // the length of the original message before it was wrapped, aligned to frame boundaries.
            final int wrappedFrameLength =
                length + DataHeaderFlyweight.HEADER_LENGTH - ReplicatedMessageDecoder.BLOCK_LENGTH;

            switch (messageHeader.templateId())
            {
                case DisconnectDecoder.TEMPLATE_ID:
                {
                    disconnect.wrap(buffer, offset, actingBlockLength, version);
                    final int libraryId = disconnect.libraryId();
                    onClusteredLibraryPosition(libraryId, position, wrappedFrameLength);
                    break;
                }

                case FixMessageDecoder.TEMPLATE_ID:
                {
                    fixMessage.wrap(buffer, offset, actingBlockLength, version);
                    final int libraryId = fixMessage.libraryId();
                    onClusteredLibraryPosition(libraryId, position, wrappedFrameLength);
                    break;
                }
            }
        }

        return Action.CONTINUE;
    }

    @SuppressWarnings("FinalParameters")
    private int wrapHeader(final DirectBuffer buffer, int offset)
    {
        messageHeader.wrap(buffer, offset);
        offset += MessageHeaderDecoder.ENCODED_LENGTH;
        return offset;
    }

    int checkConditions()
    {
        int sendCount = 0;
        for (final LibraryPositions positions : libraryIdToPosition.values())
        {
            if (positions.sendUpdatedPosition())
            {
                sendCount++;
            }
        }

        return sendCount;
    }

    public String roleName()
    {
        return agentNamePrefix + "ClusterPositionSender";
    }

    void onLibraryConnect(final int aeronSessionId, final int libraryId)
    {
        aeronSessionIdToLibraryId.put(aeronSessionId, libraryId);
        // Backup path to avoid missing a position
        final long archivedPosition = aeronSessionIdToArchivedPosition.remove(aeronSessionId);
        if (archivedPosition != MISSING)
        {
            // TODO: fix the length right
            getPositions(libraryId).newPosition(archivedPosition, (int)archivedPosition);
        }
    }

    void onApplicationHeartbeat(final int aeronSessionId, final int libraryId)
    {
        aeronSessionIdToLibraryId.put(aeronSessionId, libraryId);
    }

    void onLibraryTimeout(final int aeronSessionId, final int libraryId)
    {
        libraryIdToPosition.remove(libraryId);
        aeronSessionIdToLibraryId.remove(aeronSessionId);
    }

    void onClusteredLibraryPosition(final int libraryId, final long position, final int length)
    {
        final int alignedLength = ArchiveDescriptor.alignTerm(length);
        DebugLogger.log(POSITION, "Clustered Position %d, len = %d%n", position, alignedLength);
        getPositions(libraryId).newPosition(position, alignedLength);
    }

    public void onArchivedPosition(final int aeronSessionId, final long endPosition, final int alignedLength)
    {
        DebugLogger.log(POSITION, "Archived Position %d, len = %d%n", endPosition, alignedLength);
        final int libraryId = aeronSessionIdToLibraryId.get(aeronSessionId);
        if (libraryId != MISSING)
        {
            getPositions(libraryId).newPosition(endPosition, alignedLength);
        }
        else
        {
            // Backup path in case we haven't yet seen the library connect message
            aeronSessionIdToArchivedPosition.put(aeronSessionId, endPosition);
        }
    }

    private LibraryPositions getPositions(final int libraryId)
    {
        return libraryIdToPosition.computeIfAbsent(libraryId, newLibraryPositionsFunc);
    }

    private final class LibraryPositions
    {
        private final int libraryId;
        private Interval[] intervals =
            IntStream.range(0, DEFAULT_INTERVAL_COUNT)
                .mapToObj(i -> new Interval())
                .toArray(Interval[]::new);

        private int read;
        private int write;
        private long contiguousPosition = 0;
        private boolean updatedPosition = false;

        private LibraryPositions(final int libraryId)
        {
            this.libraryId = libraryId;
        }

        @SuppressWarnings("FinalParameters")
        private void newPosition(long endPosition, final int alignedLength)
        {
            final long startPosition = endPosition - alignedLength;
            if (contiguousPosition == 0 || contiguousPosition == startPosition)
            {
                // Scan the list of intervals for contiguous intervals
                Interval interval = intervals[read];
                while (size() > 0 && endPosition == interval.startPosition)
                {
                    endPosition = interval.endPosition;
                    read = next(read);
                    interval = intervals[read];
                }

                contiguousPosition = endPosition;

                updatedPosition = true;
            }
            // endPosition <= contiguousPosition can happen because we take the first received message
            // as the start, in practice always an archived message so this is safe.
            else if (endPosition > contiguousPosition)
            {
                final int oldEnd = intervals.length - 1;
                if (size() == oldEnd)
                {
                    final int newLength = intervals.length * 2;
                    final Interval[] newIntervals = new Interval[newLength];
                    Arrays.setAll(newIntervals, i -> new Interval());

                    for (int i = 0; read != write; i++)
                    {
                        newIntervals[i] = intervals[read];
                        read = next(read);
                    }

                    intervals = newIntervals;
                    read = 0;
                    write = oldEnd;
                }

                final Interval interval = intervals[write];
                interval.startPosition = startPosition;
                interval.endPosition = endPosition;
                write = next(write);
            }
        }

        private int next(final int value)
        {
            return (value + 1) & intervals.length - 1;
        }

        private int size()
        {
            return (write - read) & intervals.length - 1;
        }

        private boolean sendUpdatedPosition()
        {
            if (updatedPosition && inboundLibraryPublication.saveNewSentPosition(libraryId, contiguousPosition) >= 0)
            {
                updatedPosition = false;
                return true;
            }

            return false;
        }

        public String toString()
        {
            return "LibraryPositions{" +
                "libraryId=" + libraryId +
                ", intervals=" + Arrays.toString(intervals) +
                ", read=" + read +
                ", write=" + write +
                ", contiguousPosition=" + contiguousPosition +
                ", updatedPosition=" + updatedPosition +
                '}';
        }
    }

    private static class Interval
    {
        private long startPosition;
        private long endPosition;

        public String toString()
        {
            return "{" + startPosition +
                " - " + endPosition +
                '}';
        }
    }
}
