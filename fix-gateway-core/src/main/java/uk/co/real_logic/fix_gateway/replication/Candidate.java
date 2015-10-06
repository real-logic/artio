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
import uk.co.real_logic.fix_gateway.messages.AcknowledgementStatus;
import uk.co.real_logic.fix_gateway.messages.Vote;

import static uk.co.real_logic.fix_gateway.messages.Vote.FOR;

public class Candidate implements Role, ControlHandler
{
    private final ControlSubscriber controlSubscriber = new ControlSubscriber(this);

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
        if (nodeId != id && term >= this.term)
        {
            replicator.becomeFollower(timeInMs, term, position);
        }
    }

    public void startNewElection(final long timeInMs, final int oldTerm, final long position)
    {
        this.position = position;
        term = oldTerm + 1;
        startElection(timeInMs);
    }

    private void startElection(final long timeInMs)
    {
        resetTimeout(timeInMs);
        votesFor = 1; // Vote for yourself
        controlPublication.saveRequestVote(id, position, term);
    }

    private void resetTimeout(final long timeInMs)
    {
        currentVoteTimeout = timeInMs + voteTimeout;
    }
}
