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
import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.driver.MediaDriver;
import uk.co.real_logic.agrona.concurrent.AtomicCounter;
import uk.co.real_logic.agrona.concurrent.NoOpIdleStrategy;
import uk.co.real_logic.fix_gateway.TestFixtures;
import uk.co.real_logic.fix_gateway.engine.framer.ReliefValve;

import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.agrona.CloseHelper.close;

public class AbstractReplicationTest
{
    protected static final String IPC = "aeron:ipc";
    protected static final int CONTROL = 1;
    protected static final int DATA = 2;
    protected static final int ACKNOWLEDGEMENT = 3;
    protected static final int FRAGMENT_LIMIT = 1;
    protected static final long TIMEOUT = 100;
    protected static final long HEARTBEAT_INTERVAL = TIMEOUT / 2;
    protected static final int CLUSTER_SIZE = 3;
    protected static final long TIME = 0L;

    protected RaftNode raftNode1 = mock(RaftNode.class);
    protected RaftNode raftNode2 = mock(RaftNode.class);
    protected RaftNode raftNode3 = mock(RaftNode.class);

    protected TermState termState1 = new TermState();
    protected TermState termState2 = new TermState();
    protected TermState termState3 = new TermState();

    protected MediaDriver mediaDriver;
    protected Aeron aeron;

    protected Subscription controlSubscription()
    {
        return aeron.addSubscription(IPC, CONTROL);
    }

    protected Subscription acknowledgementSubscription()
    {
        return aeron.addSubscription(IPC, ACKNOWLEDGEMENT);
    }

    protected Subscription dataSubscription()
    {
        return aeron.addSubscription(IPC, DATA);
    }

    protected RaftPublication raftPublication(final int streamId)
    {
        return new RaftPublication(
            100,
            new NoOpIdleStrategy(),
            mock(AtomicCounter.class),
            mock(ReliefValve.class),
            aeron.addPublication(IPC, streamId));
    }

    protected Publication dataPublication()
    {
        return aeron.addPublication(IPC, DATA);
    }

    @Before
    public void setupAeron()
    {
        mediaDriver = TestFixtures.launchMediaDriver();
        aeron = Aeron.connect(new Aeron.Context());
    }

    @After
    public void teardownAeron()
    {
        close(aeron);
        close(mediaDriver);
    }

    protected static int poll(final Role role)
    {
        return role.poll(FRAGMENT_LIMIT, 0);
    }

    protected static void poll1(final Role role)
    {
        while (role.poll(FRAGMENT_LIMIT, 0) == 0)
        {

        }
    }

    protected static void transitionsToCandidate(final RaftNode raftNode)
    {
        verify(raftNode).transitionToCandidate(anyLong());
    }

    protected static void neverTransitionsToCandidate(final RaftNode raftNode)
    {
        verify(raftNode, never()).transitionToCandidate(anyLong());
    }

    protected static void transitionsToFollower(final RaftNode raftNode)
    {
        verify(raftNode, atLeastOnce()).transitionToFollower(any(Candidate.class), anyLong());
    }

    protected static void neverTransitionsToFollower(final RaftNode raftNode)
    {
        verify(raftNode, never()).transitionToFollower(any(Leader.class), anyLong());
    }

    protected static void transitionsToLeader(final RaftNode raftNode)
    {
        verify(raftNode).transitionToLeader(anyLong());
    }

    protected static void neverTransitionsToLeader(final RaftNode raftNode)
    {
        verify(raftNode, never()).transitionToLeader(anyLong());
    }

    protected static void staysFollower(final RaftNode raftNode)
    {
        neverTransitionsToCandidate(raftNode);
        neverTransitionsToLeader(raftNode);
    }

    protected static void staysLeader(final RaftNode raftNode)
    {
        neverTransitionsToCandidate(raftNode);
        neverTransitionsToFollower(raftNode);
    }

    protected Follower follower(
        final short id, final RaftNode raftNode, final ReplicationHandler handler, final TermState termState)
    {
        return new Follower(
            id,
            handler,
            raftNode,
            0,
            TIMEOUT,
            1024 * 1024,
            termState)
            .dataSubscription(dataSubscription())
            .controlSubscription(controlSubscription())
            .acknowledgementPublication(raftPublication(ACKNOWLEDGEMENT))
            .controlPublication(raftPublication(CONTROL));
    }

    protected static void run(final Role node1, final Role node2, final Role node3)
    {
        poll1(node1);
        poll1(node2);
        poll1(node3);

        //noinspection StatementWithEmptyBody
        while (poll(node1) + poll(node2) + poll(node3) > 0)
        {
        }
    }
}
