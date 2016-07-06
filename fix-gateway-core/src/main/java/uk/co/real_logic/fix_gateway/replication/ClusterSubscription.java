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
import uk.co.real_logic.fix_gateway.messages.ConcensusHeartbeatDecoder;
import uk.co.real_logic.sbe.ir.generated.MessageHeaderDecoder;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.*;
import static uk.co.real_logic.fix_gateway.replication.ReservedValue.NO_FILTER;

public class ClusterSubscription extends ClusterableSubscription
{

    private final MessageHeaderDecoder messageHeader = new MessageHeaderDecoder();
    private final ConcensusHeartbeatDecoder concensusHeartbeat = new ConcensusHeartbeatDecoder();
    private final ControlledFragmentHandler onControlMessage = this::onControlMessage;

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
            final int messagesRead = controlSubscription.controlledPoll(onControlMessage, fragmentLimit);

            if (cannotAdvance())
            {
                return messagesRead;
            }
        }

        messageFilter.fragmentHandler = fragmentHandler;
        return dataImage.controlledPoll(messageFilter, fragmentLimit);
    }

    private Action onControlMessage(
        final DirectBuffer buffer,
        int offset,
        final int length,
        final Header header)
    {
        messageHeader.wrap(buffer, offset);
        if (messageHeader.templateId() == ConcensusHeartbeatDecoder.TEMPLATE_ID)
        {
            offset += MessageHeaderDecoder.ENCODED_LENGTH;

            concensusHeartbeat.wrap(buffer, offset, messageHeader.blockLength(), messageHeader.version());

            final int leaderShipTerm = concensusHeartbeat.leaderShipTerm();
            final int leaderSessionId = concensusHeartbeat.leaderSessionId();
            final long position = concensusHeartbeat.position();

            return onConcensusHeartbeat(leaderShipTerm, leaderSessionId, position);
        }

        return CONTINUE;
    }

    private Action onConcensusHeartbeat(final int leaderShipTerm, final int leaderSessionId, final long position)
    {
        if (leaderShipTerm == currentLeadershipTermId)
        {
            if (messageFilter.consensusPosition < position)
            {
                messageFilter.consensusPosition = position;

                return BREAK;
            }
        }
        else if (leaderShipTerm == currentLeadershipTermId + 1 || dataImage == null)
        {
            // TODO: add previous position and check previous position.

            dataImage = dataSubscription.getImage(leaderSessionId);
            messageFilter.consensusPosition = position;
            currentLeadershipTermId = leaderShipTerm;

            return BREAK;
        }
        else if (leaderShipTerm > currentLeadershipTermId)
        {
            System.out.println("TODO: stash");
        }

        // We deliberately ignore leaderShipTerm < currentLeadershipTermId, as they would be old
        // leader messages

        return CONTINUE;
    }

    private boolean cannotAdvance()
    {
        return dataImage == null || dataImage.position() == messageFilter.consensusPosition;
    }

    public void close()
    {
        CloseHelper.close(dataSubscription);
    }

    // TODO: fix thread-safety bug, what if commitPosition refers to a different leader?
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
                if (messageHeader.templateId() != ConcensusHeartbeatDecoder.TEMPLATE_ID)
                {
                    return fragmentHandler.onFragment(buffer, offset, length, header);
                }
            }

            return CONTINUE;
        }
    }
}
