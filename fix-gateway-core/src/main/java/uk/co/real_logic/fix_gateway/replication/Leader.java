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
import uk.co.real_logic.agrona.collections.IntHashSet;
import uk.co.real_logic.agrona.collections.Long2LongHashMap;
import uk.co.real_logic.fix_gateway.messages.AcknowledgementStatus;
import uk.co.real_logic.fix_gateway.messages.Vote;

import static uk.co.real_logic.fix_gateway.messages.AcknowledgementStatus.OK;

public class Leader implements Role, ControlHandler
{
    public static final int NO_SESSION_ID = -1;

    private final short nodeId;
    private final TermAcknowledgementStrategy termAcknowledgementStrategy;
    private final ControlSubscriber controlSubscriber = new ControlSubscriber(this);
    private final ControlPublication controlPublication;
    private final Subscription controlSubscription;
    private final Subscription dataSubscription;
    private final Replicator replicator;
    private final BlockHandler handler;
    private final long heartbeatIntervalInMs;

    // Counts of how many acknowledgements
    private final Long2LongHashMap nodeToPosition = new Long2LongHashMap(NO_SESSION_ID);

    private long lastPosition = 0;
    private long nextHeartbeatTimeInMs;
    private int term;

    public Leader(
        final short nodeId,
        final TermAcknowledgementStrategy termAcknowledgementStrategy,
        final IntHashSet followers,
        final ControlPublication controlPublication,
        final Subscription controlSubscription,
        final Subscription dataSubscription,
        final Replicator replicator,
        final BlockHandler handler,
        final long timeInMs,
        final long heartbeatIntervalInMs)
    {
        this.nodeId = nodeId;
        this.termAcknowledgementStrategy = termAcknowledgementStrategy;
        this.controlPublication = controlPublication;
        this.controlSubscription = controlSubscription;
        this.dataSubscription = dataSubscription;
        this.replicator = replicator;
        this.handler = handler;
        this.heartbeatIntervalInMs = heartbeatIntervalInMs;
        followers.forEach(follower -> nodeToPosition.put(follower, 0));
        updateHeartbeatInterval(timeInMs);
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
                updateHeartbeatInterval(timeInMs);
            }
        }

        if (timeInMs > nextHeartbeatTimeInMs)
        {
            controlPublication.saveConcensusHeartbeat(nodeId, term, lastPosition);
            updateHeartbeatInterval(timeInMs);
        }

        return read;
    }

    public void updateHeartbeatInterval(final long timeInMs)
    {
        this.nextHeartbeatTimeInMs = timeInMs + nextHeartbeatTimeInMs;
    }

    public void onMessageAcknowledgement(
        final long newAckedPosition, final short nodeId, final AcknowledgementStatus status)
    {
        if (status == OK)
        {
            nodeToPosition.put(nodeId, newAckedPosition);
        }
    }

    public void onRequestVote(final short candidateId, final int term, final long lastAckedPosition)
    {
        // We've possibly timed out
    }

    public void onReplyVote(final short candidateId, final int term, final Vote vote)
    {
        // We've possibly timed out
    }

    public void onConcensusHeartbeat(final short nodeId, final int term, final long position)
    {
        if (nodeId != this.nodeId && term > this.term)
        {
            // Should not receive this unless someone else is the leader
            replicator.becomeFollower(term, position);
        }
    }

    public Leader getsElected(final long timeInMs, final int term)
    {
        this.term = term;
        updateHeartbeatInterval(timeInMs);
        return this;
    }
}
