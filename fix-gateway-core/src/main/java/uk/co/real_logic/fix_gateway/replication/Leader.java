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
import uk.co.real_logic.aeron.logbuffer.BlockHandler;
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
    private final Subscription dataSubscription;
    private final BlockHandler handler;
    private final long heartbeatIntervalInMs;

    // Counts of how many acknowledgements
    private final Long2LongHashMap nodeToPosition = new Long2LongHashMap(NO_SESSION_ID);

    private long lastPosition = 0;
    private long nextHeartbeatTimeInMs;

    public Leader(
        final TermAcknowledgementStrategy termAcknowledgementStrategy,
        final IntHashSet followers,
        final Subscription controlSubscription,
        final Subscription dataSubscription,
        final BlockHandler handler,
        final int timeInMs,
        final long heartbeatIntervalInMs)
    {
        this.termAcknowledgementStrategy = termAcknowledgementStrategy;
        this.controlSubscription = controlSubscription;
        this.dataSubscription = dataSubscription;
        this.handler = handler;
        this.heartbeatIntervalInMs = heartbeatIntervalInMs;
        followers.forEach(follower -> nodeToPosition.put(follower, 0));
        this.nextHeartbeatTimeInMs = timeInMs + nextHeartbeatTimeInMs;
    }

    public int poll(int fragmentLimit, final long timeInMs)
    {
        final int read = controlSubscription.poll(controlSubscriber, fragmentLimit);

        if (read > 0)
        {
            final long newPosition = termAcknowledgementStrategy.findAckedTerm(nodeToPosition);
            final int delta = (int) (newPosition - lastPosition);
            if (delta > 0)
            {
                lastPosition = dataSubscription.blockPoll(handler, delta);
                nextHeartbeatTimeInMs = timeInMs + heartbeatIntervalInMs;
            }
        }

        /*if (timeInMs > nextHeartbeatTimeInMs)
        {
            // TODO: heartbeat
            ;
        }*/

        return read;
    }

    public void onMessageAcknowledgement(final long newAckedPosition, final short nodeId)
    {
        nodeToPosition.put(nodeId, newAckedPosition);
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
