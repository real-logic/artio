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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.util.List;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Stream;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static uk.co.real_logic.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static uk.co.real_logic.fix_gateway.replication.AbstractReplicationTest.poll;
import static uk.co.real_logic.fix_gateway.replication.AbstractReplicationTest.run;

/**
 * Test simulated cluster.
 */
public class ClusterReplicationTest
{

    public static final int BUFFER_SIZE = 16;
    public static final int POSITION_AFTER_MESSAGE = BUFFER_SIZE + HEADER_LENGTH;

    private UnsafeBuffer buffer = new UnsafeBuffer(new byte[BUFFER_SIZE]);

    private NodeRunner node1 = new NodeRunner(1, 2, 3);
    private NodeRunner node2 = new NodeRunner(2, 1, 3);
    private NodeRunner node3 = new NodeRunner(3, 1, 2);

    @Before
    public void awaitClusterJoin()
    {
        // TODO: decide upon a better way to do this
        LockSupport.parkNanos(MILLISECONDS.toNanos(100));
    }

    @Test(timeout = 3000)
    public void shouldEstablishCluster()
    {
        while (!foundLeader())
        {
            pollAll();
        }
    }

    @Test(timeout = 3000)
    public void shouldReplicateMessage()
    {
        shouldEstablishCluster();

        final NodeRunner leader = leader();

        sendMessageTo(leader.replicator());

        assertMessageReceived();
    }

    @Test(timeout = 3000)
    public void shouldReformClusterAfterLeaderNetsplit()
    {
        shouldEstablishCluster();

        final NodeRunner leader = leader();
        final List<NodeRunner> followers = followers();

        leader.dropFrames(true);

        assertElectsNewLeader(followers);

        leader.dropFrames(false);

        assertBecomesFollower(leader);
    }

    @Test(timeout = 3000)
    public void shouldRejoinClusterAfterFollowerNetsplit()
    {
        shouldEstablishCluster();

        final NodeRunner follower = followers().get(0);

        follower.dropFrames(true);

        assertBecomesCandidate(follower);

        follower.dropFrames(false);

        assertBecomesFollower(follower);
    }

    private void assertBecomesCandidate(final NodeRunner follower)
    {
        final Replicator replicator = follower.replicator();
        assertFalse(replicator.isCandidate());
        while (!replicator.isCandidate())
        {
            pollAll();
        }
        assertTrue(replicator.isCandidate());
    }

    private void assertBecomesFollower(final NodeRunner leader)
    {
        final Replicator replicator = leader.replicator();
        assertFalse(replicator.isFollower());
        while (!replicator.isFollower())
        {
            pollAll();
        }
        assertTrue(replicator.isFollower());
    }

    private void assertElectsNewLeader(final List<NodeRunner> followers)
    {
        while (!foundLeader(followers))
        {
            pollAll();
        }
    }

    private void assertMessageReceived()
    {
        while (hasSeenMessage(node1)
            && hasSeenMessage(node2)
            && hasSeenMessage(node3))
        {
            pollAll();
        }
    }

    private boolean hasSeenMessage(final NodeRunner leader)
    {
        return leader.replicatedPosition() < POSITION_AFTER_MESSAGE;
    }

    private void sendMessageTo(final Replicator leader)
    {
        while (leader.offer(buffer, 0, BUFFER_SIZE) < 0)
        {
            pause();
        }
    }

    private void pause()
    {
        LockSupport.parkNanos(1000);
    }

    private void pollAll()
    {
        poll(node1);
        poll(node2);
        poll(node3);
        advanceAllClocks(10);
        LockSupport.parkNanos(MILLISECONDS.toNanos(1));
    }

    private boolean foundLeader()
    {
        return node1.isLeader() || node2.isLeader() || node3.isLeader();
    }

    private boolean foundLeader(List<NodeRunner> nodes)
    {
        return nodes.stream().anyMatch(NodeRunner::isLeader);
    }

    private NodeRunner leader()
    {
        return nodes()
            .filter(NodeRunner::isLeader)
            .findFirst()
            .get(); // Just error the test if there's not a leader
    }

    private List<NodeRunner> followers()
    {
        return nodes().filter(node -> !node.isLeader()).collect(toList());
    }

    private Stream<NodeRunner> nodes()
    {
        return Stream
            .of(node1, node2, node3);
    }

    private void runCluster()
    {
        run(node1, node2, node3);
    }

    private void advanceAllClocks(final long delta)
    {
        node1.advanceClock(delta);
        node2.advanceClock(delta);
        node3.advanceClock(delta);
    }

    @After
    public void shutdown()
    {
        node1.close();
        node2.close();
        node3.close();
    }

}
