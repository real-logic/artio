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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.ABORT;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;
import static uk.co.real_logic.fix_gateway.replication.TermState.NO_LEADER;
import static uk.co.real_logic.fix_gateway.replication.ReservedValue.NO_FILTER;

public class ClusterSubscription extends ClusterableSubscription
{

    private final MessageFilter messageFilter;
    private final MessageHeaderDecoder messageHeader = new MessageHeaderDecoder();
    private final Subscription subscription;
    private final AtomicInteger leaderSessionId;

    private Image image;

    public ClusterSubscription(
        final Subscription subscription,
        final int clusterStreamId,
        final AtomicLong consensusPosition,
        final AtomicInteger leaderSessionId)
    {
        this.leaderSessionId = leaderSessionId;
        // We use clusterStreamId as a reserved value filter
        if (clusterStreamId == NO_FILTER)
        {
            throw new IllegalArgumentException("ClusterStreamId must not be 0");
        }

        this.subscription = subscription;
        messageFilter = new MessageFilter(clusterStreamId, consensusPosition);
    }

    public int controlledPoll(final ControlledFragmentHandler fragmentHandler, final int fragmentLimit)
    {
        if (imageNeedsUpdate())
        {
            updateImage();

            if (imageNeedsUpdate())
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

    private boolean imageNeedsUpdate()
    {
        final Image image = this.image;
        return image == null || leaderSessionId.get() != image.sessionId();
    }

    private void updateImage()
    {
        final int leaderSessionId = this.leaderSessionId.get();
        if (leaderSessionId != NO_LEADER)
        {
            image = subscription.getImage(leaderSessionId);
        }
    }

    // TODO: fix thread-safety bug, what if commitPosition refers to a different leader?
    private final class MessageFilter implements ControlledFragmentHandler
    {
        private ControlledFragmentHandler fragmentHandler;
        private final int clusterStreamId;
        private final AtomicLong consensusPosition;

        private MessageFilter(final int clusterStreamId, final AtomicLong consensusPosition)
        {
            this.clusterStreamId = clusterStreamId;
            this.consensusPosition = consensusPosition;
        }

        public Action onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
        {
            if (header.position() > consensusPosition.get())
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
