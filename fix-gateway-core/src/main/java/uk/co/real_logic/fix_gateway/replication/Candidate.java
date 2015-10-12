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
import uk.co.real_logic.fix_gateway.DebugLogger;
import uk.co.real_logic.fix_gateway.messages.AcknowledgementStatus;
import uk.co.real_logic.fix_gateway.messages.Vote;

import java.util.concurrent.ThreadLocalRandom;

import static uk.co.real_logic.fix_gateway.messages.Vote.FOR;

public class Candidate implements Role, ControlHandler
{
    private final ControlSubscriber controlSubscriber = new ControlSubscriber(this);

    private final ThreadLocalRandom random = ThreadLocalRandom.current();

    private final short id;
    private final ControlPublication controlPublication;
    private final Subscription controlSubscription;
    private final Replicator replicator;
    private final int clusterSize;
    private final long voteTimeout;

    private long currentVoteTimeout;
    private int votesFor;
    private int leaderShipTerm;
    private long position;
    private long timeInMs;

    public Candidate(final short id,
                     final ControlPublication controlPublication,
                     final Subscription controlSubscription,
                     final Replicator replicator,
                     final int clusterSize,
                     final long voteTimeout)
    {
        this.id = id;
        this.controlPublication = controlPublication;
        this.controlSubscription = controlSubscription;
        this.replicator = replicator;
        this.clusterSize = clusterSize;
        this.voteTimeout = voteTimeout;
    }

    public int poll(int fragmentLimit, final long timeInMs)
    {
        this.timeInMs = timeInMs;
        int work = controlSubscription.poll(controlSubscriber, fragmentLimit);

        if (timeInMs > currentVoteTimeout)
        {
            //System.out.println("Timeout: " + timeInMs + " : " + currentVoteTimeout);
            startElection(timeInMs);
            work++;
        }

        return work;
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

    public void onReplyVote(final short candidateId, final int leaderShipTerm, final Vote vote)
    {
        if (candidateId == id && leaderShipTerm == this.leaderShipTerm && vote == FOR)
        {
            votesFor++;

            if (votesFor > clusterSize / 2)
            {
                replicator.becomeLeader(timeInMs, leaderShipTerm);

                controlPublication.saveConcensusHeartbeat(id, leaderShipTerm, position);
            }
        }
    }

    public void onConcensusHeartbeat(short nodeId, final int leaderShipTerm, final long position)
    {
        // System.out.println("YES @ " + this.id);
        followIfNextTerm(nodeId, leaderShipTerm, position, true);
    }

    public void startNewElection(final long timeInMs, final int oldLeaderShipTerm, final long position)
    {
        this.position = position;
        this.leaderShipTerm = oldLeaderShipTerm;
        startElection(timeInMs);
    }

    private void followIfNextTerm(
        final short nodeId, final int leaderShipTerm, final long position, final boolean leaderShipTermOk)
    {
        if (nodeId != id && position >= this.position && leaderShipTermOk)
        {
            replicator.becomeFollower(timeInMs, leaderShipTerm, position);
            stopTimeout();
        }
    }

    private void startElection(final long timeInMs)
    {
        DebugLogger.log("%d: startElection @ %d in %d\n", id, timeInMs, leaderShipTerm);

        resetTimeout(timeInMs);
        leaderShipTerm++;
        votesFor = 1; // Vote for yourself
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
}
