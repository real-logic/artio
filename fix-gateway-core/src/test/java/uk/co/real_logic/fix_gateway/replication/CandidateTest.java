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

import org.junit.Before;
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
    private static final int CLUSTER_SIZE = 5;

    private static final short ID = 3;
    private static final short ID_4 = 4;
    private static final short ID_5 = 5;

    private RaftPublication controlPublication = mock(RaftPublication.class);
    private Subscription controlSubscription = mock(Subscription.class);
    private RaftNode raftNode = mock(RaftNode.class);
    private TermState termState = new TermState();

    private Candidate candidate = new Candidate(
        ID, raftNode, CLUSTER_SIZE, VOTE_TIMEOUT, termState);

    @Before
    public void setUp()
    {
        candidate
            .controlPublication(controlPublication)
            .controlSubscription(controlSubscription);
    }

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

        candidate.onReplyVote(ID_4, ID, NEW_LEADERSHIP_TERM, FOR);
        candidate.onReplyVote(ID_5, ID, NEW_LEADERSHIP_TERM, FOR);

        transitionsToLeader(raftNode);
    }

    @Test
    public void shouldNotCountVotesForWrongTerm()
    {
        startElection();

        candidate.onReplyVote(ID_4, ID, OLD_LEADERSHIP_TERM, FOR);
        candidate.onReplyVote(ID_5, ID, OLD_LEADERSHIP_TERM, FOR);

        neverTransitionsToLeader(raftNode);
    }

    @Test
    public void shouldNotCountVotesAgainst()
    {
        startElection();

        candidate.onReplyVote(ID_4, ID, NEW_LEADERSHIP_TERM, AGAINST);

        neverTransitionsToLeader(raftNode);
    }

    @Test
    public void shouldNotCountVotesForOtherCandidates()
    {
        final short otherCandidate = (short) 2;

        startElection();

        candidate.onReplyVote(ID_4, otherCandidate, NEW_LEADERSHIP_TERM, FOR);
        candidate.onReplyVote(ID_5, otherCandidate, NEW_LEADERSHIP_TERM, FOR);

        neverTransitionsToLeader(raftNode);
    }

    @Test
    public void shouldNotDoubleCountVotes()
    {
        startElection();

        candidate.onReplyVote(ID_4, ID, NEW_LEADERSHIP_TERM, FOR);
        candidate.onReplyVote(ID_4, ID, NEW_LEADERSHIP_TERM, FOR);

        neverTransitionsToLeader(raftNode);
    }

    @Test
    public void shouldBecomeFollowerUponReceiptOfHeartbeat()
    {
        final short otherCandidate = (short) 2;

        startElection();

        candidate.onConcensusHeartbeat(otherCandidate, NEW_LEADERSHIP_TERM, POSITION);

        transitionsToFollower(raftNode);
    }

    @Test
    public void shouldNotBecomeFollowerUponReceiptOfOwnHeartbeat()
    {
        startElection();

        candidate.onConcensusHeartbeat(ID, NEW_LEADERSHIP_TERM, POSITION);

        candidateNeverBecomesLeader();
    }

    @Test
    public void shouldRestartElectionIfTimeoutElapses()
    {
        startElection();

        candidate.poll(1, VOTE_TIMEOUT * 2 + 1);

        requestsVote(NEW_LEADERSHIP_TERM);
        requestsVote(NEW_LEADERSHIP_TERM + 1);

        neverTransitionsToLeader(raftNode);
        candidateNeverBecomesLeader();
    }

    private void candidateNeverBecomesLeader()
    {
        verify(raftNode, never()).transitionToFollower(any(Candidate.class), anyLong());
    }

    private void requestsVote(final int term)
    {
        verify(controlPublication, times(1)).saveRequestVote(ID, POSITION, term);
    }

    private void startElection()
    {
        termState.leadershipTerm(OLD_LEADERSHIP_TERM).position(POSITION);
        candidate.startNewElection(0L);
    }
}
