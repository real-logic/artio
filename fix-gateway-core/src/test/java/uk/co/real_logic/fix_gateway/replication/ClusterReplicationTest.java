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
import org.junit.Ignore;
import org.junit.Test;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import java.util.concurrent.locks.LockSupport;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static uk.co.real_logic.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static uk.co.real_logic.fix_gateway.replication.AbstractReplicationTest.poll;

/**
 * Test simulated cluster.
 */
@Ignore
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
        // TODO: decide whether this is needed at all
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
        final NodeRunner[] followers = followers();

        leader.dropFrames(true);

        assertElectsNewLeader(followers);

        leader.dropFrames(false);

        assertBecomesFollower(leader);
    }

    @Test(timeout = 3000)
    public void shouldRejoinClusterAfterFollowerNetsplit()
    {
        shouldEstablishCluster();

        final NodeRunner follower = aFollower();

        follower.dropFrames(true);

        assertBecomesCandidate(follower);

        follower.dropFrames(false);

        assertBecomesFollower(follower);
    }

    @Test(timeout = 3000)
    public void shouldNotReplicateMessageUntilClusterReformed()
    {
        shouldEstablishCluster();

        final NodeRunner leader = leader();
        final NodeRunner follower = aFollower();

        follower.dropFrames(true);

        sendMessageTo(leader.replicator());

        assertBecomesCandidate(follower);

        assertTrue(notAllNodesReceivedMessage());

        follower.dropFrames(false);

        assertBecomesFollower(follower);

        assertMessageReceived();
    }

    @Test(timeout = 3000)
    public void shouldReformClusterAfterFollowerNetsplit()
    {
        shouldEstablishCluster();

        final NodeRunner[] followers = followers();

        nodes().forEach(nodeRunner -> nodeRunner.dropFrames(true));

        assertBecomesCandidate(followers);

        nodes().forEach(nodeRunner -> nodeRunner.dropFrames(false));

        assertBecomesFollower(followers);

        assertTrue(foundLeader());
    }

    private NodeRunner aFollower()
    {
        return followers()[0];
    }

    private void assertBecomesCandidate(final NodeRunner ... nodes)
    {
        assertBecomes(RaftNode::roleIsCandidate, nodes);
    }

    private void assertBecomesFollower(final NodeRunner ... nodes)
    {
        assertBecomes(RaftNode::roleIsFollower, nodes);
    }

    private void assertBecomes(final Predicate<RaftNode> predicate, final NodeRunner... nodes)
    {
        final RaftNode[] raftNodes = getReplicators(nodes);
        assertFalse(allMatch(raftNodes, predicate));
        while (!allMatch(raftNodes, predicate))
        {
            pollAll();
        }
        assertTrue(allMatch(raftNodes, predicate));
    }

    private RaftNode[] getReplicators(final NodeRunner[] nodes)
    {
        return Stream.of(nodes).map(NodeRunner::replicator).toArray(RaftNode[]::new);
    }

    private static <T> boolean allMatch(final T[] values, final Predicate<T> predicate)
    {
        return Stream.of(values).allMatch(predicate);
    }

    private void assertElectsNewLeader(final NodeRunner ... followers)
    {
        while (!foundLeader(followers))
        {
            pollAll();
        }
    }

    private void assertMessageReceived()
    {
        while (notAllNodesReceivedMessage())
        {
            pollAll();
        }
    }

    private boolean notAllNodesReceivedMessage()
    {
        return notReceivedMessage(node1) && notReceivedMessage(node2) && notReceivedMessage(node3);
    }

    private boolean notReceivedMessage(final NodeRunner node)
    {
        return node.replicatedPosition() < POSITION_AFTER_MESSAGE;
    }

    private void sendMessageTo(final RaftNode leader)
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
        return foundLeader(node1, node2, node3);
    }

    private boolean foundLeader(NodeRunner ... nodes)
    {
        final long leaderCount = Stream.of(nodes).filter(NodeRunner::isLeader).count();
        return leaderCount == 1;
    }

    private NodeRunner leader()
    {
        return nodes()
            .filter(NodeRunner::isLeader)
            .findFirst()
            .get(); // Just error the test if there's not a leader
    }

    private NodeRunner[] followers()
    {
        return nodes().filter(node -> !node.isLeader()).toArray(NodeRunner[]::new);
    }

    private Stream<NodeRunner> nodes()
    {
        return Stream.of(node1, node2, node3);
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
