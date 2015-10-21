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

    private final TermState termState;
    private final short nodeId;
    private final AcknowledgementStrategy acknowledgementStrategy;
    private final ControlSubscriber acknowledgementSubscriber = new ControlSubscriber(this);
    private final RaftNode raftNode;
    private final BlockHandler blockHandler;
    private final long heartbeatIntervalInMs;

    // Counts of how many acknowledgements
    private final Long2LongHashMap nodeToPosition = new Long2LongHashMap(NO_SESSION_ID);

    private ControlPublication controlPublication;
    private Subscription acknowledgementSubscription;
    private Subscription dataSubscription;
    private long commitPosition = 0;
    private long nextHeartbeatTimeInMs;
    private int leaderShipTerm;
    private long timeInMs;

    public Leader(
        final short nodeId,
        final AcknowledgementStrategy acknowledgementStrategy,
        final IntHashSet followers,
        final RaftNode raftNode,
        final ReplicationHandler handler,
        final long timeInMs,
        final long heartbeatIntervalInMs,
        final TermState termState)
    {
        this.nodeId = nodeId;
        this.acknowledgementStrategy = acknowledgementStrategy;
        this.raftNode = raftNode;
        this.termState = termState;
        this.blockHandler = (buffer, offset, length, sessionId, termId) -> handler.onBlock(buffer, offset, length);
        this.heartbeatIntervalInMs = heartbeatIntervalInMs;
        followers.forEach(follower -> nodeToPosition.put(follower, 0));
        updateHeartbeatInterval(timeInMs);
    }

    public int poll(int fragmentLimit, final long timeInMs)
    {
        this.timeInMs = timeInMs;
        final int read = acknowledgementSubscription.poll(acknowledgementSubscriber, fragmentLimit);

        if (read > 0)
        {
            final long newPosition = acknowledgementStrategy.findAckedTerm(nodeToPosition);
            final int delta = (int) (newPosition - commitPosition);
            if (delta > 0)
            {
                commitPosition = dataSubscription.blockPoll(blockHandler, delta);
                heartbeat();
            }
        }

        if (timeInMs > nextHeartbeatTimeInMs)
        {
            heartbeat();
        }

        return read;
    }

    public void closeStreams()
    {
        controlPublication.close();
        acknowledgementSubscription.close();
        dataSubscription.close();
    }

    private void heartbeat()
    {
        controlPublication.saveConcensusHeartbeat(nodeId, leaderShipTerm, commitPosition);
        updateHeartbeatInterval(timeInMs);
    }

    public void updateHeartbeatInterval(final long timeInMs)
    {
        // this.nextHeartbeatTimeInMs = timeInMs + nextHeartbeatTimeInMs;
        this.nextHeartbeatTimeInMs = timeInMs + heartbeatIntervalInMs;
    }

    public void onMessageAcknowledgement(
        final long newAckedPosition, final short nodeId, final AcknowledgementStatus status)
    {
        if (status == OK)
        {
            nodeToPosition.put(nodeId, newAckedPosition);
        }
    }

    public void onRequestVote(final short candidateId, final int leaderShipTerm, final long lastAckedPosition)
    {
        // We've possibly timed out
    }

    public void onReplyVote(
        final short senderNodeId, final short candidateId, final int leaderShipTerm, final Vote vote)
    {
        // We've possibly timed out
    }

    public void onConcensusHeartbeat(final short nodeId, final int leaderShipTerm, final long position)
    {
        if (nodeId != this.nodeId && leaderShipTerm > this.leaderShipTerm)
        {
            // Should not receive this unless someone else is the leader
            termState.leadershipTerm(leaderShipTerm).position(position);
            raftNode.transitionToFollower(this, timeInMs);
        }
    }

    public Leader getsElected(final long timeInMs)
    {
        leaderShipTerm = termState.leadershipTerm();
        updateHeartbeatInterval(timeInMs);
        return this;
    }

    public Leader acknowledgementSubscription(final Subscription acknowledgementSubscription)
    {
        this.acknowledgementSubscription = acknowledgementSubscription;
        return this;
    }

    public Leader dataSubscription(final Subscription dataSubscription)
    {
        this.dataSubscription = dataSubscription;
        return this;
    }

    public Leader controlPublication(final ControlPublication controlPublication)
    {
        this.controlPublication = controlPublication;
        return this;
    }
}
