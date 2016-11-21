/*
 * Copyright 2015-2016 Real Logic Ltd.
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

import static uk.co.real_logic.fix_gateway.Timing.assertEventuallyTrue;
import static uk.co.real_logic.fix_gateway.replication.ReplicationAsserts.*;

/**
 * Test candidate instances in an election
 */
public class TwoCandidateElectionTest extends AbstractReplicationTest
{
    private static final long LOW_POSITION = 32;
    private static final long HIGH_POSITION = 40;

    private Candidate node1;
    private Candidate node2;
    private Follower node3;

    @Before
    public void setUp()
    {
        termState3.leaderSessionId(DATA_SESSION_ID);

        node1 = candidate((short) 1, clusterNode1, termState1);
        node2 = candidate((short) 2, clusterNode2, termState2);
        node3 = follower((short) 3, clusterNode3, termState3);
    }

    @Test
    public void shouldElectCandidateWithAtLeastQuorumPosition()
    {
        termState3.allPositions(HIGH_POSITION);
        node3.follow(TIME);

        termState1.leadershipTerm(1).allPositions(32);
        termState2.leadershipTerm(1).allPositions(40);
        node1.startNewElection(TIME);
        node2.startNewElection(TIME);

        runElection();

        electionResultsAre(clusterNode2, clusterNode1);
    }

    @Test
    public void shouldElectCandidateWithCorrectTerm()
    {
        termState3.leadershipTerm(2).consensusPosition(32);
        node3.follow(TIME);

        electCandidateWithCorrectTerm();
    }

    private void electCandidateWithCorrectTerm()
    {
        termState1.leadershipTerm(1).allPositions(40);
        termState2.leadershipTerm(2).allPositions(32);
        node1.startNewElection(TIME);
        node2.startNewElection(TIME);

        runElection();

        electionResultsAre(clusterNode2, clusterNode1);
    }

    @Test
    public void shouldResolveCandidatesWithEqualPositions()
    {
        node3.follow(TIME);

        termState1.leadershipTerm(1).allPositions(HIGH_POSITION);
        termState2.leadershipTerm(1).allPositions(HIGH_POSITION);
        node1.startNewElection(TIME);
        node2.startNewElection(TIME);

        runElection();

        electionResultsAre(clusterNode1, clusterNode2);
    }

    @Test
    public void shouldBeAbleToSwitchLeadersUponSecondElection()
    {
        shouldElectCandidateWithAtLeastQuorumPosition();

        electCandidateWithCorrectTerm();
    }

    private void electionResultsAre(final ClusterAgent leader, final ClusterAgent follower)
    {
        transitionsToLeader(leader);
        staysLeader(leader);

        staysFollower(follower);

        staysFollower(clusterNode3);
    }

    private void runElection()
    {
        poll1(node1);
        poll1(node2);
        poll1(node3);

        assertEventuallyTrue(
            "Timed out awaiting the end of the election",
            () -> poll(node1) + poll(node2) + poll(node3) == 0);
    }

    private Candidate candidate(final short id, final ClusterAgent clusterNode, final TermState termState)
    {
        final QuorumAcknowledgementStrategy ackStrategy = new QuorumAcknowledgementStrategy();
        return new Candidate(
            id, DATA_SESSION_ID, clusterNode, CLUSTER_SIZE, TIMEOUT, termState, ackStrategy,
            NODE_STATE_BUFFER, nodeStateHandler)
                    .controlSubscription(controlSubscription())
                    .controlPublication(raftPublication(ClusterNodeConfiguration.DEFAULT_CONTROL_STREAM_ID));
    }
}
