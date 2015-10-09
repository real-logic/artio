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
    private int term;
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

    public void onRequestVote(short candidateId, final int term, long lastAckedPosition)
    {
        followIfNextTerm(candidateId, term, position,
            term > this.term);
    }

    public void onReplyVote(final short candidateId, final int term, final Vote vote)
    {
        if (candidateId == id && term == this.term && vote == FOR)
        {
            votesFor++;

            if (votesFor > clusterSize / 2)
            {
                replicator.becomeLeader(timeInMs, term);

                controlPublication.saveConcensusHeartbeat(id, term, position);
            }
        }
    }

    public void onConcensusHeartbeat(short nodeId, final int term, final long position)
    {
        // System.out.println("YES @ " + this.id);
        followIfNextTerm(nodeId, term, position, true);
    }

    public void startNewElection(final long timeInMs, final int oldTerm, final long position)
    {
        this.position = position;
        this.term = oldTerm;
        startElection(timeInMs);
    }

    private void followIfNextTerm(final short nodeId, final int term, final long position, final boolean termOk)
    {
        if (nodeId != id && position >= this.position && termOk)
        {
            replicator.becomeFollower(timeInMs, term, position);
            stopTimeout();
        }
    }

    private void startElection(final long timeInMs)
    {
        DebugLogger.log("%d: startElection @ %d in %d\n", id, timeInMs, term);

        resetTimeout(timeInMs);
        term++;
        votesFor = 1; // Vote for yourself
        controlPublication.saveRequestVote(id, position, term);
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
