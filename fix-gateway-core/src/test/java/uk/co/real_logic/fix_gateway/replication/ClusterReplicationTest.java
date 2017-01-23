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

import io.aeron.logbuffer.BufferClaim;
import org.agrona.collections.Int2IntHashMap;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import uk.co.real_logic.fix_gateway.DebugLogger;
import uk.co.real_logic.fix_gateway.Timing;

import java.util.Optional;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static org.hamcrest.Matchers.hasEntry;
import static org.junit.Assert.*;
import static uk.co.real_logic.fix_gateway.LogTag.RAFT;
import static uk.co.real_logic.fix_gateway.Timing.DEFAULT_TIMEOUT_IN_MS;
import static uk.co.real_logic.fix_gateway.Timing.withTimeout;

/**
 * Test simulated cluster.
 */
public class ClusterReplicationTest
{
    private static final int BUFFER_SIZE = 1337;
    private static final int POSITION_AFTER_MESSAGE = BUFFER_SIZE + HEADER_LENGTH;
    private static final int TEST_TIMEOUT = 10_000;

    private BufferClaim bufferClaim = new BufferClaim();
    private UnsafeBuffer buffer = new UnsafeBuffer(new byte[BUFFER_SIZE]);

    private final NodeRunner node1 = new NodeRunner(1, 2, 3);
    private final NodeRunner node2 = new NodeRunner(2, 1, 3);
    private final NodeRunner node3 = new NodeRunner(3, 1, 2);
    private final NodeRunner[] allNodes = { node1, node2, node3 };

    @Before
    public void hasElectedLeader()
    {
        assertEventuallyFindsLeaderIn(allNodes);

        final NodeRunner leader = leader();
        DebugLogger.log(RAFT, "Leader elected: %d\n\n", leader.clusterAgent().nodeId());
    }

    @After
    public void shutdown()
    {
        for (final NodeRunner nodeRunner : allNodes)
        {
            nodeRunner.close();
        }
    }

    @Test(timeout = TEST_TIMEOUT)
    public void shouldEstablishCluster()
    {
        checkClusterStable();

        assertNodeStateReplicated();
    }

    @Test(timeout = TEST_TIMEOUT)
    public void shouldReplicateMessage()
    {
        final NodeRunner leader = leader();

        DebugLogger.log(RAFT, "Leader is %s\n", leader.clusterAgent().nodeId());

        final long position = sendMessageTo(leader);

        DebugLogger.log(RAFT, "Leader @ %s\n", position);

        assertMessageReceived();
    }

    @Test(timeout = TEST_TIMEOUT)
    public void shouldReformClusterAfterLeaderPause()
    {
        awaitLeadershipConsensus();

        final NodeRunner leader = leader();
        final NodeRunner[] followers = followers();

        assertEventuallyTrue(
            "Failed to find leader",
            () ->
            {
                poll(followers);
                return foundLeader(followers);
            });

        assertBecomesFollower(leader);
    }

    @Test(timeout = TEST_TIMEOUT)
    public void shouldReformClusterAfterLeaderNetsplit()
    {
        leaderNetSplitScenario(true, true);
    }

    @Test(timeout = TEST_TIMEOUT)
    public void shouldReformClusterAfterPartialLeaderNetsplit()
    {
        // NB: under other partial failure, the leader would never stop being a leader
        leaderNetSplitScenario(false, true);
    }

    private void leaderNetSplitScenario(final boolean dropInboundFrames, final boolean dropOutboundFrames)
    {
        final NodeRunner leader = leader();
        final NodeRunner[] followers = followers();

        leader.dropFrames(dropInboundFrames, dropOutboundFrames);

        assertElectsNewLeader(followers);

        leader.dropFrames(false);

        assertBecomesFollower(leader);
    }

    @Test
    public void shouldRejoinClusterAfterFollowerNetsplit()
    {
        // NB: under other partial failure, the follower would never stop being a follower
        followerNetSplitScenario(true, true);
    }

    @Test(timeout = TEST_TIMEOUT)
    public void shouldRejoinClusterAfterPartialFollowerNetsplit()
    {
        followerNetSplitScenario(true, false);
    }

    private void followerNetSplitScenario(final boolean dropInboundFrames, final boolean dropOutboundFrames)
    {
        final NodeRunner follower = aFollower();

        follower.dropFrames(dropInboundFrames, dropOutboundFrames);

        assertBecomesCandidate(follower);

        follower.dropFrames(false);

        eventuallyOneLeaderAndTwoFollowers();
    }

    @Test(timeout = TEST_TIMEOUT)
    public void shouldReformClusterAfterFollowerNetsplit()
    {
        clusterNetSplitScenario(true, true);
    }

    @Test(timeout = TEST_TIMEOUT)
    public void shouldReformClusterAfterPartialFollowerNetsplit()
    {
        clusterNetSplitScenario(true, false);
    }

    private void clusterNetSplitScenario(final boolean dropInboundFrames, final boolean dropOutboundFrames)
    {
        final NodeRunner[] followers = followers();

        nodes().forEach((nodeRunner) -> nodeRunner.dropFrames(dropInboundFrames, dropOutboundFrames));

        assertBecomesCandidate(followers);

        nodes().forEach((nodeRunner) -> nodeRunner.dropFrames(false));

        assertBecomesFollower(followers);

        eventuallyOneLeaderAndTwoFollowers();
    }

    @Ignore
    @Test(timeout = TEST_TIMEOUT)
    public void shouldNotReplicateMessageUntilClusterReformed()
    {
        final NodeRunner leader = leader();
        final NodeRunner follower = aFollower();

        follower.dropFrames(true);

        assertBecomesCandidate(follower);

        sendMessageTo(leader);

        assertTrue("nodes received message when one was supposedly netsplit", noNodesReceivedMessage());

        follower.dropFrames(false);

        assertBecomesFollower(follower);

        assertMessageReceived();
    }

    private void assertNodeStateReplicated()
    {
        final int[] nodeIds = { 1, 2, 3 };
        final int followerCount = nodeIds.length - 1;
        final NodeRunner leader = leader();
        final Int2IntHashMap nodeIdToId = leader.nodeIdToId();
        assertEventuallyTrue(
            "Never replicates node state",
            () ->
            {
                pollAll();
                return nodeIdToId.size() >= followerCount;
            });

        final short leaderId = leader.clusterAgent().nodeId();

        for (final int id : nodeIds)
        {
            if (id != leaderId)
            {
                assertThat(nodeIdToId, hasEntry(id, id));
            }
        }
    }

    private NodeRunner aFollower()
    {
        return followers()[0];
    }

    private void assertBecomesCandidate(final NodeRunner... nodes)
    {
        assertBecomes("isCandidate", ClusterAgent::isCandidate, allNodes, nodes);
    }

    private void assertBecomesFollower(final NodeRunner... nodes)
    {
        assertBecomes("isFollower", ClusterAgent::isFollower, allNodes, nodes);
    }

    private void assertBecomes(
        final String message,
        final Predicate<ClusterAgent> predicate,
        final NodeRunner[] toPoll,
        final NodeRunner... nodes)
    {
        final ClusterAgent[] clusterNodes = getRaftNodes(nodes);
        assertEventuallyTrue(
            message + " never true",
            () ->
            {
                poll(toPoll);
                return allMatch(clusterNodes, predicate);
            });
    }

    private ClusterAgent[] getRaftNodes(final NodeRunner[] nodes)
    {
        return Stream.of(nodes).map(NodeRunner::clusterAgent).toArray(ClusterAgent[]::new);
    }

    private static <T> boolean allMatch(final T[] values, final Predicate<T> predicate)
    {
        return Stream.of(values).allMatch(predicate);
    }

    private void assertElectsNewLeader(final NodeRunner... followers)
    {
        assertEventuallyFindsLeaderIn(followers);
    }

    private void assertMessageReceived()
    {
        assertEventuallyTrue(
            "Message not received",
            () ->
            {
                pollAll();
                return !noNodesReceivedMessage();
            });
    }

    private boolean noNodesReceivedMessage()
    {
        return notReceivedMessage(node1) && notReceivedMessage(node2) && notReceivedMessage(node3);
    }

    private void checkClusterStable()
    {
        for (int i = 0; i < 100; i++)
        {
            pollAll();
        }

        eventuallyOneLeaderAndTwoFollowers(this::nodesAgreeOnLeader);

        assertAllNodesSeeSameLeader();

        DebugLogger.log(RAFT, "Cluster Stable");
    }

    private void awaitLeadershipConsensus()
    {
        assertEventuallyTrue(
            "Nodes don't agree on the leader",
            () ->
            {
                pollAll();
                final int leaderOfNode1 = node1.leaderSessionId();
                return leaderOfNode1 == node2.leaderSessionId()
                    && leaderOfNode1 == node3.leaderSessionId();
            });
    }

    private void assertAllNodesSeeSameLeader()
    {
        final int leaderSessionId = node1.leaderSessionId();
        assertEquals("1 and 2 disagree on leader" + clusterInfo(), leaderSessionId, node2.leaderSessionId());
        assertEquals("1 and 3 disagree on leader" + clusterInfo(), leaderSessionId, node3.leaderSessionId());
    }

    private boolean notReceivedMessage(final NodeRunner node)
    {
        return node.replicatedPosition() < POSITION_AFTER_MESSAGE;
    }

    private long sendMessageTo(final NodeRunner leader)
    {
        final ClusterablePublication publication = leader.clusterAgent().clusterStreams().publication(1);

        return withTimeout(
            "Failed to send message",
            () ->
            {
                final long position = publication.tryClaim(BUFFER_SIZE, bufferClaim);
                if (position > 0)
                {
                    bufferClaim.buffer().putBytes(bufferClaim.offset(), buffer, 0, BUFFER_SIZE);
                    bufferClaim.commit();
                    return Optional.of(position);
                }

                pollAll();
                return Optional.empty();
            },
            5_000L);
    }

    private void pollAll()
    {
        poll(allNodes);
    }

    private void poll(final NodeRunner... nodes)
    {
        final int fragmentLimit = 10;
        for (final NodeRunner node : nodes)
        {
            node.poll(fragmentLimit);
        }
    }

    private void assertEventuallyFindsLeaderIn(final NodeRunner... nodes)
    {
        assertEventuallyTrue(
            "Never finds leader",
            () ->
            {
                pollAll();
                return foundLeader(nodes);
            });
    }

    private boolean foundLeader(NodeRunner... nodes)
    {
        final long leaderCount = Stream.of(nodes).filter(NodeRunner::isLeader).count();
        return leaderCount == 1;
    }

    private void eventuallyOneLeaderAndTwoFollowers()
    {
        eventuallyOneLeaderAndTwoFollowers(() -> true);
    }

    private void eventuallyOneLeaderAndTwoFollowers(final BooleanSupplier predicate)
    {
        assertEventuallyTrue(
            "failed to find one leader with two followers",
            () ->
            {
                pollAll();
                return oneLeaderAndTwoFollowers() && predicate.getAsBoolean();
            });
    }

    private boolean nodesAgreeOnLeader()
    {
        final NodeRunner[] allNodes = this.allNodes;
        final int leaderSessionId = allNodes[0].leaderSessionId();
        for (int i = 1; i < allNodes.length; i++)
        {
            if (allNodes[i].leaderSessionId() != leaderSessionId)
            {
                return false;
            }
        }
        return true;
    }

    private boolean oneLeaderAndTwoFollowers()
    {
        int leaderCount = 0;
        int followerCount = 0;

        for (final NodeRunner node : allNodes)
        {
            if (node.isLeader())
            {
                leaderCount++;
            }
            else if (node.clusterAgent().isFollower())
            {
                followerCount++;
            }
        }

        return leaderCount == 1 && followerCount == 2;
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
        return nodes().filter((node) -> !node.isLeader()).toArray(NodeRunner[]::new);
    }

    private void assertEventuallyTrue(
        final String message,
        final BooleanSupplier test)
    {
        Timing.assertEventuallyTrue(
            () -> message + clusterInfo(),
            test,
            DEFAULT_TIMEOUT_IN_MS,
            () ->
            {
            }
        );
    }

    private String clusterInfo()
    {
        return nodes()
            .map(NodeRunner::clusterAgent)
            .map(
                (agent) ->
                {
                    final TermState termState = agent.termState();
                    final int leaderSessionId = termState.leaderSessionId().get();
                    final int leadershipTerm = termState.leadershipTerm();
                    return String.format(
                        "%s %d: leader=%d, term=%d", state(agent), agent.nodeId(), leaderSessionId, leadershipTerm);
                })
            .collect(Collectors.joining("\n", "\n", "\n"));
    }

    private String state(final ClusterAgent agent)
    {
        if (agent.isLeader())
        {
            return "leader   ";
        }

        if (agent.isFollower())
        {
            return "follower ";
        }

        if (agent.isCandidate())
        {
            return "candidate";
        }

        throw new IllegalStateException("Unknown state");
    }

    private Stream<NodeRunner> nodes()
    {
        return Stream.of(node1, node2, node3);
    }
}
