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
import uk.co.real_logic.fix_gateway.messages.ConsensusHeartbeatDecoder;
import uk.co.real_logic.sbe.ir.generated.MessageHeaderDecoder;

import java.util.PriorityQueue;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.*;
import static java.util.Comparator.comparing;
import static uk.co.real_logic.fix_gateway.replication.ReservedValue.NO_FILTER;

class ClusterSubscription extends ClusterableSubscription
{

    private final MessageHeaderDecoder messageHeader = new MessageHeaderDecoder();
    private final ConsensusHeartbeatDecoder concensusHeartbeat = new ConsensusHeartbeatDecoder();
    private final ControlledFragmentHandler onControlMessage = this::onControlMessage;
    private final PriorityQueue<FutureAck> futureAcks = new PriorityQueue<>(
        comparing(FutureAck::leaderShipTerm).thenComparing(FutureAck::position));

    private final MessageFilter messageFilter;
    private final Subscription dataSubscription;
    private final Subscription controlSubscription;

    private int currentLeadershipTermId = Integer.MIN_VALUE;
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
            && messageFilter.consensusPosition == ack.previousPosition)
        {
            futureAcks.poll();

            switchTerms(ack.leaderShipTermId, ack.leaderSessionId, ack.position);

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

            concensusHeartbeat.wrap(buffer, offset, messageHeader.blockLength(), messageHeader.version());

            final int leaderShipTerm = concensusHeartbeat.leaderShipTerm();
            final int leaderSessionId = concensusHeartbeat.leaderSessionId();
            final long position = concensusHeartbeat.position();
            final long previousPosition = concensusHeartbeat.startPosition();

            return onConsensusHeartbeat(leaderShipTerm, leaderSessionId, position, previousPosition);
        }

        return CONTINUE;
    }

    Action onConsensusHeartbeat(
        final int leaderShipTermId,
        final int leaderSessionId,
        final long position,
        final long previousPosition)
    {
        if (leaderShipTermId == currentLeadershipTermId)
        {
            if (messageFilter.consensusPosition < position)
            {
                messageFilter.consensusPosition = position;

                return BREAK;
            }
        }
        else if (leaderShipTermId == currentLeadershipTermId + 1 || dataImage == null)
        {
            if (previousPosition != messageFilter.consensusPosition)
            {
                save(leaderShipTermId, leaderSessionId, position, previousPosition);
            }
            else
            {
                switchTerms(leaderShipTermId, leaderSessionId, position);

                return BREAK;
            }
        }
        else if (leaderShipTermId > currentLeadershipTermId)
        {
            save(leaderShipTermId, leaderSessionId, position, previousPosition);
        }

        // We deliberately ignore leaderShipTerm < currentLeadershipTermId, as they would be old
        // leader messages

        return CONTINUE;
    }

    private void switchTerms(final int leaderShipTermId, final int leaderSessionId, final long position)
    {
        dataImage = dataSubscription.getImage(leaderSessionId);
        messageFilter.consensusPosition = position;
        currentLeadershipTermId = leaderShipTermId;
    }

    private void save(final int leaderShipTermId,
                      final int leaderSessionId,
                      final long position,
                      final long previousPosition)
    {
        futureAcks.add(new FutureAck(leaderShipTermId, leaderSessionId, position, previousPosition));
    }

    private boolean cannotAdvance()
    {
        return dataImage == null || dataImage.position() == messageFilter.consensusPosition;
    }

    public void close()
    {
        CloseHelper.close(dataSubscription);
    }

    private final class MessageFilter implements ControlledFragmentHandler
    {
        private ControlledFragmentHandler fragmentHandler;
        private final int clusterStreamId;

        private long consensusPosition;

        private MessageFilter(final int clusterStreamId)
        {
            this.clusterStreamId = clusterStreamId;
        }

        public Action onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
        {
            if (header.position() > consensusPosition)
            {
                return ABORT;
            }

            final int clusterStreamId = ReservedValue.clusterStreamId(header);
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
        private final long position;
        private final long previousPosition;

        private FutureAck(
            final int leaderShipTermId,
            final int leaderSessionId,
            final long position,
            final long previousPosition)
        {
            this.leaderShipTermId = leaderShipTermId;
            this.leaderSessionId = leaderSessionId;
            this.position = position;
            this.previousPosition = previousPosition;
        }

        public int leaderShipTerm()
        {
            return leaderShipTermId;
        }

        public long position()
        {
            return position;
        }

        public int previousLeadershipTermId()
        {
            return leaderSessionId - 1;
        }
    }

    long currentConsensusPosition()
    {
        return messageFilter.consensusPosition;
    }

    int currentLeadershipTermId()
    {
        return currentLeadershipTermId;
    }
}
