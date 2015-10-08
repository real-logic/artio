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
import uk.co.real_logic.aeron.logbuffer.BlockHandler;

import static org.mockito.Mockito.mock;

/**
 * Test candidate instances in an election
 */
public class ElectionTest extends AbstractReplicationTest
{

    private Candidate node1;
    private Candidate node2;
    private Follower node3;

    @Before
    public void setUp()
    {
        node1 = candidate((short) 1, replicator1);
        node2 = candidate((short) 2, replicator2);
        node3 = follower((short) 3, replicator2, mock(BlockHandler.class));
    }

    @Test
    public void shouldElectCandidateWithAtLeastQuorumPosition()
    {
        node3.follow(TIME, 1, 40);

        node1.startNewElection(TIME, 1, 32);
        node2.startNewElection(TIME, 1, 40);

        runElection();

        electionResultsAre(replicator2, replicator1);
    }

    @Test
    public void shouldElectCandidateWithCorrectTerm()
    {
        node3.follow(TIME, 2, 32);

        electCandidateWithCorrectTerm();
    }

    private void electCandidateWithCorrectTerm()
    {
        node1.startNewElection(TIME, 1, 40);
        node2.startNewElection(TIME, 2, 32);

        runElection();

        electionResultsAre(replicator2, replicator1);
    }

    @Test
    public void shouldResolveCandidatesWithEqualPositions()
    {
        node3.follow(TIME, 1, 40);

        node1.startNewElection(TIME, 1, 40);
        node2.startNewElection(TIME, 1, 40);

        runElection();

        electionResultsAre(replicator1, replicator2);
    }

    @Test
    public void shouldBeAbleToSwitchLeadersUponSecondElection()
    {
        shouldElectCandidateWithAtLeastQuorumPosition();

        electCandidateWithCorrectTerm();
    }

    private void electionResultsAre(final Replicator leader, final Replicator follower)
    {
        becomesLeader(leader);
        staysLeader(leader);

        becomesFollower(follower);
        staysFollower(follower);

        staysFollower(replicator3);
    }

    private void runElection()
    {
        run(node1, node2, node3);
    }

    private Candidate candidate(final short id, final Replicator replicator)
    {
        return new Candidate(
            id, controlPublication(), controlSubscription(), replicator, CLUSTER_SIZE, TIMEOUT);
    }
}
