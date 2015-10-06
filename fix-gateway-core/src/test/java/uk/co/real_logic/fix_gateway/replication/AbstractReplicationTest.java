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
import uk.co.real_logic.aeron.logbuffer.FragmentHandler;
import uk.co.real_logic.agrona.concurrent.AtomicCounter;
import uk.co.real_logic.agrona.concurrent.NoOpIdleStrategy;
import uk.co.real_logic.fix_gateway.TestFixtures;
import uk.co.real_logic.fix_gateway.engine.framer.ReliefValve;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static uk.co.real_logic.agrona.CloseHelper.close;

public class AbstractReplicationTest
{
    protected static final String IPC = "aeron:ipc";
    protected static final int CONTROL = 1;
    protected static final int DATA = 2;
    protected static final int FRAGMENT_LIMIT = 10;
    protected static final long TIMEOUT = 100;
    protected static final long HEARTBEAT_INTERVAL = TIMEOUT / 2;
    protected static final int CLUSTER_SIZE = 3;
    protected static final long TIME = 0L;


    protected Replicator replicator1 = mock(Replicator.class);
    protected Replicator replicator2 = mock(Replicator.class);
    protected Replicator replicator3 = mock(Replicator.class);

    protected MediaDriver mediaDriver;
    protected Aeron aeron;

    protected Subscription controlSubscription()
    {
        return aeron.addSubscription(IPC, CONTROL);
    }

    protected Subscription dataSubscription()
    {
        return aeron.addSubscription(IPC, DATA);
    }

    protected ControlPublication controlPublication()
    {
        return new ControlPublication(
            100,
            new NoOpIdleStrategy(),
            mock(AtomicCounter.class),
            mock(ReliefValve.class),
            aeron.addPublication(IPC, CONTROL));
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

    protected int poll(final Role role)
    {
        return role.poll(FRAGMENT_LIMIT, 0);
    }

    protected void poll1(final Role role)
    {
        while (role.poll(FRAGMENT_LIMIT, 0) == 0)
        {

        }
    }

    protected static void becomesCandidate(final Replicator replicator)
    {
        verify(replicator).becomeCandidate();
    }

    protected static void becomesFollower(final Replicator replicator)
    {
        verify(replicator).becomeFollower();
    }

    protected static void neverBecomesCandidate(final Replicator replicator)
    {
        verify(replicator, never()).becomeCandidate();
    }

    protected static void neverBecomesFollower(final Replicator replicator)
    {
        verify(replicator, never()).becomeFollower();
    }

    protected static void neverBecomesLeader(final Replicator replicator)
    {
        verify(replicator, never()).becomeLeader();
    }

    protected static void becomesLeader(final Replicator replicator)
    {
        verify(replicator).becomeLeader();
    }

    protected static void staysFollower(final Replicator replicator)
    {
        neverBecomesCandidate(replicator);
        neverBecomesLeader(replicator);
    }

    protected static void staysLeader(final Replicator replicator)
    {
        neverBecomesCandidate(replicator);
        neverBecomesFollower(replicator);
    }

    protected Follower follower(final short id, final Replicator replicator, final FragmentHandler handler)
    {
        return new Follower(
            id,
            controlPublication(),
            handler,
            dataSubscription(),
            controlSubscription(),
            replicator,
            0,
            TIMEOUT);
    }
}
