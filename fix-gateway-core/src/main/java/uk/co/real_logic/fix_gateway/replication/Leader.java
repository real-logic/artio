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
package uk.co.real_logic.fix_gateway.replication;

import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.logbuffer.FragmentHandler;
import uk.co.real_logic.aeron.logbuffer.Header;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.collections.IntHashSet;
import uk.co.real_logic.agrona.collections.Long2LongHashMap;

public class Leader implements Role, ControlHandler, FragmentHandler
{
    public static final int NO_SESSION_ID = -1;

    private final TermAcknowledgementStrategy termAcknowledgementStrategy;
    private final ControlSubscriber controlSubscriber = new ControlSubscriber(this);
    private final Subscription controlSubscription;

    // Counts of how many acknowledgements
    private final Long2LongHashMap nodeToAckedPosition = new Long2LongHashMap(NO_SESSION_ID);
    private long acknowledgedTerm = 0;

    public Leader(
        final TermAcknowledgementStrategy termAcknowledgementStrategy,
        final IntHashSet followers,
        final Subscription controlSubscription)
    {
        this.termAcknowledgementStrategy = termAcknowledgementStrategy;
        this.controlSubscription = controlSubscription;
        followers.forEach(follower -> nodeToAckedPosition.put(follower, 0));
    }

    public int poll(int fragmentLimit, final long timeInMs)
    {
        return controlSubscription.poll(controlSubscriber, fragmentLimit);
    }

    public void onMessageAcknowledgement(final long newAckedPosition, final short nodeId)
    {
        final long lastAckedPosition = nodeToAckedPosition.get(nodeId);
        if (lastAckedPosition != NO_SESSION_ID)
        {
            if (newAckedPosition > lastAckedPosition)
            {
                nodeToAckedPosition.put(nodeId, newAckedPosition);

                final long newAcknowledgedTerm = termAcknowledgementStrategy.findAckedTerm(nodeToAckedPosition);
                if (newAcknowledgedTerm > acknowledgedTerm)
                {
                    //subscription.pollToPosition(newAckedPosition);
                    acknowledgedTerm = newAcknowledgedTerm;
                }
            }
        }
        else
        {
            // TODO: error case
        }
    }

    public void onRequestVote(final short candidateId, final long lastAckedPosition)
    {
        // They're rebelling
    }

    public void onConcensusHeartbeat(final short nodeId)
    {
        // Update heartbeat time
    }

    @Override
    public void onFragment(DirectBuffer buffer, int offset, int length, Header header)
    {

    }
}
