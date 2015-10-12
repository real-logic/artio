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

import org.junit.Test;
import uk.co.real_logic.aeron.Subscription;

import static org.mockito.Mockito.*;
import static uk.co.real_logic.fix_gateway.messages.Vote.AGAINST;
import static uk.co.real_logic.fix_gateway.messages.Vote.FOR;
import static uk.co.real_logic.fix_gateway.replication.AbstractReplicationTest.*;

public class CandidateTest
{
    private static final long POSITION = 40;
    private static final long VOTE_TIMEOUT = 100;
    private static final int OLD_LEADERSHIP_TERM = 1;
    private static final int NEW_LEADERSHIP_TERM = OLD_LEADERSHIP_TERM + 1;
    private static final int CLUSTER_SIZE = 3;
    private static final short ID = 3;

    private ControlPublication controlPublication = mock(ControlPublication.class);
    private Subscription controlSubscription = mock(Subscription.class);
    private Replicator replicator = mock(Replicator.class);

    private Candidate candidate = new Candidate(
        ID, controlPublication, controlSubscription, replicator, CLUSTER_SIZE, VOTE_TIMEOUT);

    @Test
    public void shouldVoteForSelfWhenStartingElection()
    {
        startElection();

        requestsVote(NEW_LEADERSHIP_TERM);
    }

    @Test
    public void shouldBecomeLeaderUponReceiptOfEnoughVotes()
    {
        startElection();

        candidate.onReplyVote(ID, NEW_LEADERSHIP_TERM, FOR);

        becomesLeader(replicator);
    }

    @Test
    public void shouldNotBecomeLeaderUponReceiptOfVotesForWrongTerm()
    {
        startElection();

        candidate.onReplyVote(ID, OLD_LEADERSHIP_TERM, FOR);

        neverBecomesLeader(replicator);
    }

    @Test
    public void shouldNotBecomeLeaderUponReceiptOfVotesAgainst()
    {
        startElection();

        candidate.onReplyVote(ID, NEW_LEADERSHIP_TERM, AGAINST);

        neverBecomesLeader(replicator);
    }

    @Test
    public void shouldNotBecomeLeaderUponReceiptOfVotesForOtherCandidates()
    {
        final short otherCandidate = (short) 2;

        startElection();

        candidate.onReplyVote(otherCandidate, NEW_LEADERSHIP_TERM, FOR);

        neverBecomesLeader(replicator);
    }

    @Test
    public void shouldBecomeFollowerUponReceiptOfHeartbeat()
    {
        final short otherCandidate = (short) 2;

        startElection();

        candidate.onConcensusHeartbeat(otherCandidate, NEW_LEADERSHIP_TERM, POSITION);

        becomesFollower(replicator);
    }

    @Test
    public void shouldNotBecomeFollowerUponReceiptOfOwnHeartbeat()
    {
        startElection();

        candidate.onConcensusHeartbeat(ID, NEW_LEADERSHIP_TERM, POSITION);

        neverBecomesFollower(replicator);
    }

    @Test
    public void shouldRestartElectionIfTimeoutElapses()
    {
        startElection();

        candidate.poll(1, VOTE_TIMEOUT * 2 + 1);

        requestsVote(NEW_LEADERSHIP_TERM);
        requestsVote(NEW_LEADERSHIP_TERM + 1);

        neverBecomesLeader(replicator);
        neverBecomesFollower(replicator);
    }

    private void requestsVote(final int term)
    {
        verify(controlPublication, times(1)).saveRequestVote(ID, POSITION, term);
    }

    private void startElection()
    {
        candidate.startNewElection(0L, OLD_LEADERSHIP_TERM, POSITION);
    }
}
