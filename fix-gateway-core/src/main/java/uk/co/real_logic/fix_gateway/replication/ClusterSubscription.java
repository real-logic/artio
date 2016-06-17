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
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import uk.co.real_logic.fix_gateway.messages.ConcensusHeartbeatDecoder;
import uk.co.real_logic.sbe.ir.generated.MessageHeaderDecoder;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.ABORT;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;
import static uk.co.real_logic.fix_gateway.replication.ReservedValue.NO_FILTER;
import static uk.co.real_logic.fix_gateway.replication.TermState.NO_LEADER;

public class ClusterSubscription extends ClusterableSubscription
{

    private final MessageFilter messageFilter;
    private final MessageHeaderDecoder messageHeader = new MessageHeaderDecoder();
    private final Subscription subscription;
    private final TermState termState;

    private Image image;

    ClusterSubscription(
        final Subscription subscription,
        final int clusterStreamId,
        final TermState termState)
    {
        this.termState = termState;
        // We use clusterStreamId as a reserved value filter
        if (clusterStreamId == NO_FILTER)
        {
            throw new IllegalArgumentException("ClusterStreamId must not be 0");
        }

        this.subscription = subscription;
        messageFilter = new MessageFilter(clusterStreamId);
    }

    public int controlledPoll(final ControlledFragmentHandler fragmentHandler, final int fragmentLimit)
    {
        final LeaderPosition leaderPosition = termState.leaderPosition();
        if (imageNeedsUpdate(leaderPosition))
        {
            onLeaderChanged(leaderPosition);

            if (image == null)
            {
                return 0;
            }
        }

        messageFilter.fragmentHandler = fragmentHandler;
        return image.controlledPoll(messageFilter, fragmentLimit);
    }

    public void close()
    {
        CloseHelper.close(subscription);
    }

    public void forEachPosition(final PositionHandler handler)
    {
        // TODO: remove this method.
    }

    private boolean imageNeedsUpdate(final LeaderPosition leaderPosition)
    {
        final Image image = this.image;
        return image == null || leaderPosition.sessionId() != image.sessionId();
    }

    private void onLeaderChanged(final LeaderPosition leaderPosition)
    {
        final int leaderSessionId = leaderPosition.sessionId();
        if (leaderSessionId != NO_LEADER)
        {
            image = subscription.getImage(leaderSessionId);
            messageFilter.leaderPosition = leaderPosition;
        }
    }

    private final class MessageFilter implements ControlledFragmentHandler
    {
        private ControlledFragmentHandler fragmentHandler;
        private final int clusterStreamId;
        private LeaderPosition leaderPosition;

        private MessageFilter(final int clusterStreamId)
        {
            this.clusterStreamId = clusterStreamId;
        }

        public Action onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
        {
            if (header.position() > leaderPosition.consensusPosition())
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
