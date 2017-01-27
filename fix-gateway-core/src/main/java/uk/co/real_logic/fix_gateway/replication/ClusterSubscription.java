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
package uk.co.real_logic.fix_gateway.replication;

import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import uk.co.real_logic.fix_gateway.DebugLogger;
import uk.co.real_logic.fix_gateway.replication.messages.ConsensusHeartbeatDecoder;
import uk.co.real_logic.fix_gateway.replication.messages.MessageHeaderDecoder;

import java.util.PriorityQueue;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.*;
import static java.util.Comparator.comparing;
import static uk.co.real_logic.fix_gateway.LogTag.RAFT;
import static uk.co.real_logic.fix_gateway.replication.ReservedValue.NO_FILTER;

class ClusterSubscription extends ClusterableSubscription
{
    private final MessageHeaderDecoder messageHeader = new MessageHeaderDecoder();
    private final ConsensusHeartbeatDecoder consensusHeartbeat = new ConsensusHeartbeatDecoder();
    private final ControlledFragmentHandler onControlMessage = this::onControlMessage;
    private final PriorityQueue<FutureAck> futureAcks = new PriorityQueue<>(
        comparing(FutureAck::leaderShipTerm).thenComparing(FutureAck::streamPosition));

    private final MessageFilter messageFilter;
    private final Subscription dataSubscription;
    private final Subscription controlSubscription;

    private int currentLeadershipTermId = Integer.MIN_VALUE;
    private long previousPosition;
    private Image dataImage;

    ClusterSubscription(
        final Subscription dataSubscription,
        final int clusterStreamId,
        final Subscription controlSubscription)
    {
        this.controlSubscription = controlSubscription;
        // We use clusterStreamId as a reserved value filter
        if (clusterStreamId == NO_FILTER)
        {
            throw new IllegalArgumentException("ClusterStreamId must not be 0");
        }

        this.dataSubscription = dataSubscription;
        messageFilter = new MessageFilter(clusterStreamId);
    }

    public int controlledPoll(final ControlledFragmentHandler fragmentHandler, final int fragmentLimit)
    {
        if (cannotAdvance())
        {
            if (!hasMatchingFutureAck())
            {
                final int messagesRead = controlSubscription.controlledPoll(onControlMessage, fragmentLimit);

                if (cannotAdvance())
                {
                    return messagesRead;
                }
            }
        }

        messageFilter.fragmentHandler = fragmentHandler;
        return dataImage.controlledPoll(messageFilter, fragmentLimit);
    }

    boolean hasMatchingFutureAck()
    {
        final FutureAck ack = futureAcks.peek();
        if (ack != null
            && currentLeadershipTermId == ack.previousLeadershipTermId()
            && previousPosition == ack.startPosition)
        {
            futureAcks.poll();

            switchTerms(ack.leaderShipTermId, ack.leaderSessionId, ack.streamPosition, ack.startPosition);

            return true;
        }

        return false;
    }

    private Action onControlMessage(
        final DirectBuffer buffer,
        int offset,
        final int length,
        final Header header)
    {
        messageHeader.wrap(buffer, offset);
        if (messageHeader.templateId() == ConsensusHeartbeatDecoder.TEMPLATE_ID)
        {
            offset += MessageHeaderDecoder.ENCODED_LENGTH;

            consensusHeartbeat.wrap(buffer, offset, messageHeader.blockLength(), messageHeader.version());

            final int leaderShipTerm = consensusHeartbeat.leaderShipTerm();
            final int leaderSessionId = consensusHeartbeat.leaderSessionId();
            final long position = consensusHeartbeat.position();
            final long streamStartPosition = consensusHeartbeat.streamStartPosition();
            final long streamPosition = consensusHeartbeat.streamPosition();

            return onConsensusHeartbeat(leaderShipTerm, leaderSessionId, position, streamStartPosition, streamPosition);
        }

        return CONTINUE;
    }

    Action onConsensusHeartbeat(
        final int leaderShipTermId,
        final int leaderSessionId,
        final long position,
        final long streamStartPosition,
        final long streamPosition)
    {
        DebugLogger.log(
            RAFT,
            "Subscription Heartbeat(leaderShipTerm=%d, pos=%d, sStartPos=%d, sPos=%d, leaderSessId=%d)%n",
            leaderShipTermId,
            position,
            streamStartPosition,
            streamPosition,
            leaderSessionId);

        if (leaderShipTermId == currentLeadershipTermId)
        {
            if (messageFilter.streamConsensusPosition < streamPosition)
            {
                messageFilter.streamConsensusPosition = streamPosition;
                previousPosition = position;

                return BREAK;
            }
        }
        else if (leaderShipTermId == currentLeadershipTermId + 1 || dataImage == null)
        {
            final long length = streamPosition - streamStartPosition;
            final long startPosition = position - length;
            if (startPosition != previousPosition)
            {
                save(leaderShipTermId, leaderSessionId, streamStartPosition, streamPosition);
            }
            else
            {
                switchTerms(leaderShipTermId, leaderSessionId, streamPosition, position);

                return BREAK;
            }
        }
        else if (leaderShipTermId > currentLeadershipTermId)
        {
            save(leaderShipTermId, leaderSessionId, streamStartPosition, streamPosition);
        }

        // We deliberately ignore leaderShipTerm < currentLeadershipTermId, as they would be old
        // leader messages

        return CONTINUE;
    }

    private void switchTerms(final int leaderShipTermId,
                             final int leaderSessionId,
                             final long streamPosition,
                             final long position)
    {
        dataImage = dataSubscription.imageBySessionId(leaderSessionId);
        messageFilter.streamConsensusPosition = streamPosition;
        currentLeadershipTermId = leaderShipTermId;
        previousPosition = position;
    }

    private void save(
        final int leaderShipTermId,
        final int leaderSessionId,
        final long previousPosition,
        final long streamPosition)
    {
        futureAcks.add(new FutureAck(leaderShipTermId, leaderSessionId, streamPosition, previousPosition));
    }

    private boolean cannotAdvance()
    {
        return dataImage == null || dataImage.position() >= messageFilter.streamConsensusPosition;
    }

    public void close()
    {
        CloseHelper.close(dataSubscription);
    }

    private final class MessageFilter implements ControlledFragmentHandler
    {
        private ControlledFragmentHandler fragmentHandler;
        private final int clusterStreamId;

        private long streamConsensusPosition;

        private MessageFilter(final int clusterStreamId)
        {
            this.clusterStreamId = clusterStreamId;
        }

        public Action onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
        {
            final long headerPosition = header.position();
            final int clusterStreamId = ReservedValue.clusterStreamId(header);

            DebugLogger.log(
                RAFT,
                "Subscription onFragment(hdrPos=%d, csnsPos=%d, ourStream=%d, msgStream=%d)%n",
                headerPosition,
                streamConsensusPosition,
                this.clusterStreamId,
                clusterStreamId);

            if (headerPosition > streamConsensusPosition)
            {
                return ABORT;
            }

            if (this.clusterStreamId == clusterStreamId)
            {
                messageHeader.wrap(buffer, offset);
                if (messageHeader.templateId() != ConsensusHeartbeatDecoder.TEMPLATE_ID)
                {
                    return fragmentHandler.onFragment(buffer, offset, length, header);
                }
            }

            return CONTINUE;
        }
    }

    private final class FutureAck
    {
        private final int leaderShipTermId;
        private final int leaderSessionId;
        private final long streamPosition;
        private final long startPosition;

        private FutureAck(
            final int leaderShipTermId,
            final int leaderSessionId,
            final long streamPosition,
            final long startPosition)
        {
            this.leaderShipTermId = leaderShipTermId;
            this.leaderSessionId = leaderSessionId;
            this.streamPosition = streamPosition;
            this.startPosition = startPosition;
        }

        private int leaderShipTerm()
        {
            return leaderShipTermId;
        }

        private long streamPosition()
        {
            return streamPosition;
        }

        private int previousLeadershipTermId()
        {
            return leaderShipTermId - 1;
        }
    }

    long streamPosition()
    {
        return messageFilter.streamConsensusPosition;
    }

    public long positionOf(final int aeronSessionId)
    {
        return streamPosition();
    }

    int currentLeadershipTermId()
    {
        return currentLeadershipTermId;
    }
}
