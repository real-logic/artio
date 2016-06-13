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

import java.util.concurrent.atomic.AtomicLong;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.ABORT;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;

public class ClusterSubscription2 extends ClusterableSubscription
{
    private final MessageFilter messageFilter;
    private final MessageHeaderDecoder messageHeader = new MessageHeaderDecoder();
    private final Subscription subscription;

    private Image image;

    public ClusterSubscription2(
        final Subscription subscription,
        final int clusterStreamId,
        final AtomicLong position)
    {
        this.subscription = subscription;
        messageFilter = new MessageFilter(clusterStreamId, position);
    }

    public int controlledPoll(final ControlledFragmentHandler fragmentHandler, final int fragmentLimit)
    {
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

    // TODO: update all subscriptions once per duty cycle
    public void onNewLeader(final int leaderSessionId)
    {
        this.image = subscription.getImage(leaderSessionId);
    }

    private final class MessageFilter implements ControlledFragmentHandler
    {
        private ControlledFragmentHandler fragmentHandler;
        private final int clusterStreamId;
        private final AtomicLong position;

        private MessageFilter(final int clusterStreamId, final AtomicLong position)
        {
            this.clusterStreamId = clusterStreamId;
            this.position = position;
        }

        public Action onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
        {
            if (header.position() > position.get())
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
