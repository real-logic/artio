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

import uk.co.real_logic.fix_gateway.messages.AcknowledgementStatus;
import uk.co.real_logic.fix_gateway.messages.Vote;

import static uk.co.real_logic.fix_gateway.messages.Vote.FOR;

public class Candidate implements Role, ControlHandler
{
    private final short id;
    private final ControlPublication controlPublication;
    private final Replicator replicator;
    private final int clusterSize;

    private int votesFor;
    private int term;

    public Candidate(final short id,
                     final ControlPublication controlPublication,
                     final Replicator replicator,
                     final int clusterSize)
    {
        this.id = id;
        this.controlPublication = controlPublication;
        this.replicator = replicator;
        this.clusterSize = clusterSize;
    }

    public int poll(int fragmentLimit, final long timeInMs)
    {
        return 0;
    }

    public void onMessageAcknowledgement(
        final long newAckedPosition, final short nodeId, final AcknowledgementStatus status)
    {

    }

    public void onRequestVote(short candidateId, long lastAckedPosition)
    {

    }

    public void onReplyVote(final short candidateId, final int term, final Vote vote)
    {
        if (candidateId == id && term == this.term && vote == FOR)
        {
            votesFor++;

            if (votesFor > clusterSize / 2)
            {
                controlPublication.saveConcensusHeartbeat(id);
                replicator.becomeLeader();
            }
        }
    }

    public void onConcensusHeartbeat(short nodeId)
    {
        if (nodeId != id)
        {
            replicator.becomeFollower();
        }
    }

    public void startElection(final int oldTerm, final long position)
    {
        votesFor = 1; // Vote for yourself
        term = oldTerm + 1;
        controlPublication.saveRequestVote(id, position, term);
    }
}
