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
import uk.co.real_logic.agrona.collections.IntHashSet;
import uk.co.real_logic.fix_gateway.DebugLogger;
import uk.co.real_logic.fix_gateway.messages.AcknowledgementStatus;
import uk.co.real_logic.fix_gateway.messages.Vote;

import java.util.concurrent.ThreadLocalRandom;

import static uk.co.real_logic.fix_gateway.messages.Vote.FOR;

public class Candidate implements Role, RaftHandler
{
    private final RaftSubscriber raftSubscriber = new RaftSubscriber(this);

    private final ThreadLocalRandom random = ThreadLocalRandom.current();

    private final TermState termState;
    private final short id;
    private final RaftNode raftNode;
    private final int clusterSize;
    private final long voteTimeout;
    private final IntHashSet votesFor;

    private RaftPublication controlPublication;
    private Subscription controlSubscription;
    private long currentVoteTimeout;
    private int leaderShipTerm;
    private long position;
    private long timeInMs;

    public Candidate(final short id,
                     final RaftNode raftNode,
                     final int clusterSize,
                     final long voteTimeout,
                     final TermState termState)
    {
        this.id = id;
        this.raftNode = raftNode;
        this.clusterSize = clusterSize;
        this.voteTimeout = voteTimeout;
        this.termState = termState;
        votesFor = new IntHashSet(2 * clusterSize, -1);
    }

    public int poll(int fragmentLimit, final long timeInMs)
    {
        this.timeInMs = timeInMs;
        int work = controlSubscription.poll(raftSubscriber, fragmentLimit);

        if (timeInMs > currentVoteTimeout)
        {
            //System.out.println("Timeout: " + timeInMs + " : " + currentVoteTimeout);
            startElection(timeInMs);
            work++;
        }

        return work;
    }

    public void closeStreams()
    {
        controlPublication.close();
        controlSubscription.close();
    }

    public void onMessageAcknowledgement(
        final long newAckedPosition, final short nodeId, final AcknowledgementStatus status)
    {

    }

    public void onRequestVote(short candidateId, final int leaderShipTerm, long lastAckedPosition)
    {
        followIfNextTerm(candidateId, leaderShipTerm, position,
            leaderShipTerm > this.leaderShipTerm);
    }

    public void onReplyVote(
        final short senderNodeId, final short candidateId, final int leaderShipTerm, final Vote vote)
    {
        if (candidateId == id && leaderShipTerm == this.leaderShipTerm && vote == FOR && votesFor.add(senderNodeId))
        {
            if (votesFor.size() > clusterSize / 2)
            {
                termState.leadershipTerm(leaderShipTerm);
                raftNode.transitionToLeader(timeInMs);
            }
        }
    }

    public void onConcensusHeartbeat(short nodeId,
                                     final int leaderShipTerm,
                                     final long position,
                                     final int dataSessionId)
    {
        // System.out.println("YES @ " + this.id);
        followIfNextTerm(nodeId, leaderShipTerm, position, true);
    }

    public Candidate startNewElection(final long timeInMs)
    {
        this.position = termState.position();
        this.leaderShipTerm = termState.leadershipTerm();
        startElection(timeInMs);
        return this;
    }

    private void followIfNextTerm(
        final short nodeId, final int leaderShipTerm, final long position, final boolean leaderShipTermOk)
    {
        if (nodeId != id && position >= this.position && leaderShipTermOk)
        {
            votesFor.clear();
            termState
                .position(position)
                .leadershipTerm(leaderShipTerm);
            raftNode.transitionToFollower(this, timeInMs);
            stopTimeout();
        }
    }

    private void startElection(final long timeInMs)
    {
        DebugLogger.log("%d: startElection @ %d in %d\n", id, timeInMs, leaderShipTerm);

        resetTimeout(timeInMs);
        leaderShipTerm++;
        votesFor.add(id); // Vote for yourself
        controlPublication.saveRequestVote(id, position, leaderShipTerm);
    }

    private void stopTimeout()
    {
        currentVoteTimeout = Long.MAX_VALUE;
    }

    private void resetTimeout(final long timeInMs)
    {
        currentVoteTimeout = timeInMs + random.nextLong(voteTimeout / 2, voteTimeout);
    }

    public Candidate controlPublication(final RaftPublication controlPublication)
    {
        this.controlPublication = controlPublication;
        return this;
    }

    public Candidate controlSubscription(final Subscription controlSubscription)
    {
        this.controlSubscription = controlSubscription;
        return this;
    }
}
