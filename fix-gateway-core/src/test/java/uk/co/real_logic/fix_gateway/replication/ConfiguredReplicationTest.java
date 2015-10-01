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
import org.mockito.ArgumentCaptor;
import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.driver.MediaDriver;
import uk.co.real_logic.aeron.logbuffer.BlockHandler;
import uk.co.real_logic.aeron.logbuffer.FragmentHandler;
import uk.co.real_logic.aeron.protocol.DataHeaderFlyweight;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.collections.IntHashSet;
import uk.co.real_logic.agrona.concurrent.AtomicBuffer;
import uk.co.real_logic.agrona.concurrent.AtomicCounter;
import uk.co.real_logic.agrona.concurrent.NoOpIdleStrategy;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.TestFixtures;
import uk.co.real_logic.fix_gateway.engine.framer.ReliefValve;

import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.agrona.CloseHelper.close;

public class ConfiguredReplicationTest
{

    private static final String IPC = "aeron:ipc";
    private static final int CONTROL = 1;
    private static final int DATA = 2;
    private static final int VALUE = 42;
    private static final int OFFSET = 42;
    private static final int FRAGMENT_LIMIT = 10;
    private static final long REPLY_TIMEOUT = 100;
    private static final long HEARTBEAT_INTERVAL = REPLY_TIMEOUT / 2;

    private AtomicBuffer buffer = new UnsafeBuffer(new byte[1024]);

    private BlockHandler handler = mock(BlockHandler.class);
    private Replicator leaderReplicator = mock(Replicator.class);
    private Replicator follower1Replicator = mock(Replicator.class);
    private Replicator follower2Replicator = mock(Replicator.class);

    private MediaDriver mediaDriver;
    private Aeron aeron;
    private Leader leader;
    private Follower follower1;
    private Follower follower2;
    private Publication dataPublication;

    @Before
    public void setUp()
    {
        buffer.putInt(OFFSET, VALUE);

        mediaDriver = TestFixtures.launchMediaDriver();
        aeron = Aeron.connect();

        final IntHashSet followers = new IntHashSet(10, -1);
        followers.add(2);
        followers.add(3);

        leader = new Leader(
            new EntireClusterTermAcknowledgementStrategy(),
            followers,
            controlSubscription(),
            dataSubscription(),
            handler,
            0,
            HEARTBEAT_INTERVAL);

        follower1 = follower((short) 2, follower1Replicator);
        follower2 = follower((short) 3, follower2Replicator);

        dataPublication = dataPublication();
    }

    @Test
    public void shouldNotProcessDataUntilAcknowledged()
    {
        offerBuffer();

        pollLeader(0);
        noDataAcknowledged();
    }

    @Test
    public void shouldProcessDataWhenAcknowledged()
    {
        final long position = offerBuffer();

        poll(follower1);
        poll(follower2);

        pollLeader(2);
        dataAcknowledged((int) position, 0);
    }

    @Test
    public void shouldRequireQuorumToProcess()
    {
        offerBuffer();

        poll(follower1);

        pollLeader(1);
        noDataAcknowledged();
    }

    @Test
    public void shouldSupportAcknowledgementLagging()
    {
        final long position = offerBuffer();

        poll(follower1);

        pollLeader(1);
        noDataAcknowledged();

        poll(follower2);
        pollLeader(1);

        dataAcknowledged((int) position, 0);
    }

    @Test
    public void shouldTimeoutLeader()
    {
        follower1.poll(FRAGMENT_LIMIT, REPLY_TIMEOUT + 1);

        verifyBecomeCandidate();
    }

    @Test
    public void shouldNotTimeoutLeaderIfMessagesReceived()
    {
        offerBuffer();

        follower1.poll(FRAGMENT_LIMIT, HEARTBEAT_INTERVAL);

        follower1.poll(FRAGMENT_LIMIT, REPLY_TIMEOUT + 1);

        verifyStayFollower();
    }

    @Test
    public void shouldNotTimeoutLeaderIfHeartbeat()
    {
        leader.poll(FRAGMENT_LIMIT, HEARTBEAT_INTERVAL + 1);

        follower1.poll(FRAGMENT_LIMIT, REPLY_TIMEOUT + 1);

        verifyStayFollower();
    }

    private void verifyBecomeCandidate()
    {
        verify(follower1Replicator).becomeCandidate();
    }

    private void verifyStayFollower()
    {
        verify(follower1Replicator, never()).becomeCandidate();
        verify(follower1Replicator, never()).becomeLeader();
    }

    private void pollLeader(final int read)
    {
        assertEquals(read, poll(leader));
    }

    private long offerBuffer()
    {
        final long position = dataPublication.offer(buffer);
        assertThat(position, greaterThan(0L));
        return position;
    }

    private void dataAcknowledged(final int length, final int offset)
    {
        final ArgumentCaptor<DirectBuffer> bufferCaptor = ArgumentCaptor.forClass(DirectBuffer.class);
        verify(handler).onBlock(bufferCaptor.capture(), eq(offset), eq(length), anyInt(), anyInt());
        final DirectBuffer buffer = bufferCaptor.getValue();
        assertEquals(VALUE, buffer.getInt(OFFSET + DataHeaderFlyweight.HEADER_LENGTH));
    }

    private void noDataAcknowledged()
    {
        verify(handler, never()).onBlock(any(), anyInt(), anyInt(), anyInt(), anyInt());
    }

    private int poll(final Role role)
    {
        return role.poll(FRAGMENT_LIMIT, 0);
    }

    private Follower follower(final short id, final Replicator replicator)
    {
        return new Follower(
            id,
            controlPublication(),
            mock(FragmentHandler.class),
            dataSubscription(),
            replicator,
            0,
            REPLY_TIMEOUT);
    }

    private Subscription controlSubscription()
    {
        return aeron.addSubscription(IPC, CONTROL);
    }

    private Subscription dataSubscription()
    {
        return aeron.addSubscription(IPC, DATA);
    }

    private ControlPublication controlPublication()
    {
        return new ControlPublication(
            100,
            new NoOpIdleStrategy(),
            mock(AtomicCounter.class),
            mock(ReliefValve.class),
            aeron.addPublication(IPC, CONTROL));
    }

    private Publication dataPublication()
    {
        return aeron.addPublication(IPC, DATA);
    }

    @After
    public void teardown()
    {
        close(aeron);
        close(mediaDriver);
    }
}
