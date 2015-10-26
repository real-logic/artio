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
import uk.co.real_logic.agrona.collections.IntHashSet;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static uk.co.real_logic.fix_gateway.replication.AbstractReplicationTest.neverTransitionsToFollower;

public class LeaderTest
{
    private static final short ID = 2;
    private static final int LEADERSHIP_TERM = 1;
    private static final int DATA_SESSION_ID = 42;
    private static final long TIME = 10L;
    private static final long POSITION = 40L;
    private static final int HEARTBEAT_INTERVAL_IN_MS = 10;

    private RaftPublication controlPublication = mock(RaftPublication.class);
    private RaftNode raftNode = mock(RaftNode.class);
    private Subscription acknowledgementSubscription = mock(Subscription.class);
    private Subscription dataSubscription = mock(Subscription.class);
    private TermState termState = new TermState()
        .leadershipTerm(LEADERSHIP_TERM)
        .position(POSITION);

    private Leader leader = new Leader(
        ID,
        new EntireClusterAcknowledgementStrategy(),
        new IntHashSet(40, -1),
        raftNode,
        mock(ReplicationHandler.class),
        0,
        HEARTBEAT_INTERVAL_IN_MS,
        termState,
        DATA_SESSION_ID)
        .controlPublication(controlPublication)
        .acknowledgementSubscription(acknowledgementSubscription)
        .dataSubscription(dataSubscription)
        .getsElected(TIME);

    @Test
    public void shouldNotifyOtherNodesThatItIsTheLeader()
    {
        verify(controlPublication).saveConcensusHeartbeat(ID, LEADERSHIP_TERM, POSITION, DATA_SESSION_ID);
    }

    @Test
    public void shouldBecomeFollowerUponOtherLeaderHeartbeating()
    {
        final short newLeaderId = 3;

        receivesHeartbeat(newLeaderId, LEADERSHIP_TERM + 1);

        verify(raftNode, atLeastOnce()).transitionToFollower(any(Leader.class), anyLong());
    }

    @Test
    public void shouldNotBecomeFollowerFromOldTermHeartbeating()
    {
        final short newLeaderId = 3;

        receivesHeartbeat(newLeaderId, LEADERSHIP_TERM);

        neverTransitionsToFollower(raftNode);
    }

    @Test
    public void shouldNotBecomeFollowerFromOwnHeartbeats()
    {
        receivesHeartbeat(ID, LEADERSHIP_TERM);

        neverTransitionsToFollower(raftNode);
    }

    private void receivesHeartbeat(final short leaderId, final int leaderShipTerm)
    {
        leader.onConcensusHeartbeat(leaderId, leaderShipTerm, POSITION, DATA_SESSION_ID);
    }

}
