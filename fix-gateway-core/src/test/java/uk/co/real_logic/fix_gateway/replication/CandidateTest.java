/*
 * Copyright 2015-2017 Real Logic Ltd.
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

import io.aeron.Subscription;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Before;
import org.junit.Test;
import org.mockito.verification.VerificationMode;

import static io.aeron.Publication.BACK_PRESSURED;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.fix_gateway.replication.ReplicationAsserts.neverTransitionsToFollower;
import static uk.co.real_logic.fix_gateway.replication.ReplicationAsserts.neverTransitionsToLeader;
import static uk.co.real_logic.fix_gateway.replication.messages.Vote.AGAINST;
import static uk.co.real_logic.fix_gateway.replication.messages.Vote.FOR;

public class CandidateTest
{
    private static final long POSITION = 40;
    private static final long VOTE_TIMEOUT = 100;
    private static final int OLD_LEADERSHIP_TERM = 1;
    private static final int NEW_LEADERSHIP_TERM = OLD_LEADERSHIP_TERM + 1;
    private static final int DATA_SESSION_ID = 42;
    private static final int CLUSTER_SIZE = 5;
    private static final DirectBuffer NODE_STATE_BUFFER = new UnsafeBuffer(new byte[1]);
    private static final int NODE_STATE_LENGTH = 1;
    private static final int SESSION_ID = 0;

    private static final short ID = 3;
    private static final short ID_4 = 4;
    private static final short ID_5 = 5;

    private RaftPublication controlPublication = mock(RaftPublication.class);
    private Subscription controlSubscription = mock(Subscription.class);
    private ClusterAgent clusterAgent = mock(ClusterAgent.class);
    private TermState termState = new TermState();
    private NodeStateHandler nodeStateHandler = mock(NodeStateHandler.class);

    private Candidate candidate = new Candidate(
        ID, DATA_SESSION_ID, clusterAgent, CLUSTER_SIZE, VOTE_TIMEOUT,
        termState, new QuorumAcknowledgementStrategy(),
        NODE_STATE_BUFFER, nodeStateHandler);

    @Before
    public void setUp()
    {
        candidate
            .controlPublication(controlPublication)
            .controlSubscription(controlSubscription);
    }

    @Test
    public void shouldNotCountVotesForWrongTerm()
    {
        startElection();

        candidate.onReplyVote(
            ID_4, ID, OLD_LEADERSHIP_TERM, FOR, NODE_STATE_BUFFER, NODE_STATE_LENGTH, SESSION_ID);
        candidate.onReplyVote(
            ID_5, ID, OLD_LEADERSHIP_TERM, FOR, NODE_STATE_BUFFER, NODE_STATE_LENGTH, SESSION_ID);

        neverTransitionsToLeader(clusterAgent);
    }

    @Test
    public void shouldNotCountVotesAgainst()
    {
        startElection();

        candidate.onReplyVote(
            ID_4, ID, NEW_LEADERSHIP_TERM, AGAINST, NODE_STATE_BUFFER, NODE_STATE_LENGTH, SESSION_ID);
        candidate.onReplyVote(
            ID_5, ID, NEW_LEADERSHIP_TERM, AGAINST, NODE_STATE_BUFFER, NODE_STATE_LENGTH, SESSION_ID);

        neverTransitionsToLeader(clusterAgent);
    }

    @Test
    public void shouldNotVoteAgainstYourself()
    {
        startElection();

        requestsVote(NEW_LEADERSHIP_TERM, times(1));

        // Candidate will receive own vote request
        candidate.onRequestVote(ID, DATA_SESSION_ID, NEW_LEADERSHIP_TERM, POSITION);

        verifyNoMoreInteractions(controlPublication);
        neverTransitionsToLeader(clusterAgent);
        neverTransitionsToFollower(clusterAgent);
    }

    @Test
    public void shouldNotCountVotesForOtherCandidates()
    {
        final short otherCandidate = (short)2;

        startElection();

        candidate.onReplyVote(
            ID_4, otherCandidate, NEW_LEADERSHIP_TERM, FOR, NODE_STATE_BUFFER, NODE_STATE_LENGTH, SESSION_ID);
        candidate.onReplyVote(
            ID_5, otherCandidate, NEW_LEADERSHIP_TERM, FOR, NODE_STATE_BUFFER, NODE_STATE_LENGTH, SESSION_ID);

        neverTransitionsToLeader(clusterAgent);
    }

    @Test
    public void shouldNotDoubleCountVotes()
    {
        startElection();

        candidate.onReplyVote(ID_4, ID, NEW_LEADERSHIP_TERM, FOR, NODE_STATE_BUFFER, NODE_STATE_LENGTH, SESSION_ID);
        candidate.onReplyVote(ID_4, ID, NEW_LEADERSHIP_TERM, FOR, NODE_STATE_BUFFER, NODE_STATE_LENGTH, SESSION_ID);

        neverTransitionsToLeader(clusterAgent);
    }

    @Test
    public void shouldRestartElectionIfTimeoutElapses()
    {
        startElection();

        candidate.poll(1, VOTE_TIMEOUT * 2 + 1);

        requestsVote(NEW_LEADERSHIP_TERM, times(1));
        requestsVote(NEW_LEADERSHIP_TERM + 1, times(1));

        neverTransitionsToLeader(clusterAgent);
        neverTransitionsToFollower(clusterAgent);
    }

    @Test
    public void shouldResentRequestVoteIfBackPressured()
    {
        when(controlPublication.saveRequestVote(anyShort(), anyInt(), anyLong(), anyInt()))
            .thenReturn(BACK_PRESSURED, 100L);

        startElection();

        candidate.poll(1, 0);

        candidate.poll(1, 0);

        candidate.poll(1, 0);

        requestsVote(NEW_LEADERSHIP_TERM, times(2));
    }

    private void requestsVote(final int term, final VerificationMode mode)
    {
        verify(controlPublication, mode).saveRequestVote(ID, DATA_SESSION_ID, POSITION, term);
    }

    private void startElection()
    {
        termState.leadershipTerm(OLD_LEADERSHIP_TERM).consensusPosition(POSITION);
        candidate.startNewElection(0L);
    }
}
