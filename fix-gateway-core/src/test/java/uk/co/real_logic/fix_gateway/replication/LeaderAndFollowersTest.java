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
import org.mockito.ArgumentCaptor;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.logbuffer.BlockHandler;
import uk.co.real_logic.aeron.logbuffer.FragmentHandler;
import uk.co.real_logic.aeron.protocol.DataHeaderFlyweight;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.collections.IntHashSet;
import uk.co.real_logic.agrona.concurrent.AtomicBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.*;

/**
 * Test an isolated set of leaders and followers
 */
public class LeaderAndFollowersTest extends AbstractReplicationTest
{

    private static final int VALUE = 42;
    private static final int OFFSET = 42;
    private static final short LEADER_ID = (short) 1;
    private static final short FOLLOWER_1_ID = (short) 2;
    private static final short FOLLOWER_2_ID = (short) 3;

    private AtomicBuffer buffer = new UnsafeBuffer(new byte[1024]);

    private BlockHandler leaderHandler = mock(BlockHandler.class);
    private FragmentHandler follower1Handler = mock(FragmentHandler.class);

    private Leader leader;
    private Follower follower1;
    private Follower follower2;
    private Publication dataPublication;

    @Before
    public void setUp()
    {
        buffer.putInt(OFFSET, VALUE);

        final IntHashSet followers = new IntHashSet(10, -1);
        followers.add(2);
        followers.add(3);

        leader = new Leader(
            LEADER_ID,
            new EntireClusterTermAcknowledgementStrategy(),
            followers,
            controlPublication(),
            controlSubscription(),
            dataSubscription(),
            replicator1,
            leaderHandler,
            0,
            HEARTBEAT_INTERVAL);

        follower1 = follower(FOLLOWER_1_ID, replicator2, follower1Handler);
        follower2 = follower(FOLLOWER_2_ID, replicator3, mock(FragmentHandler.class));

        dataPublication = dataPublication();
    }

    @Test
    public void shouldNotProcessDataUntilAcknowledged()
    {
        offerBuffer();

        pollLeader(0);
        leaderNeverCommitted();
    }

    @Test
    public void shouldProcessDataWhenAcknowledged()
    {
        final int position = roundtripABuffer();

        leaderCommitted(0, position);
    }

    @Test
    public void shouldProcessSuccessiveChunks()
    {
        final int position1 = roundtripABuffer();
        leaderCommitted(0, position1);

        final int position2 = roundtripABuffer();

        leaderCommitted(position1, position2 - position1);
    }

    @Test
    public void shouldRequireContiguousMessages()
    {
        final int position1 = roundtripABuffer();
        leaderCommitted(0, position1);

        follower1.follow(0, 0, 1);

        final int position2 = roundtripABuffer();

        leaderNotCommitted(position1, position2 - position1);
    }

    @Test
    public void shouldRequireQuorumToProcess()
    {
        offerBuffer();

        poll(follower1);

        pollLeader(1);
        leaderNeverCommitted();
    }

    @Test
    public void shouldSupportAcknowledgementLagging()
    {
        final int position = offerBuffer();

        poll(follower1);

        pollLeader(1);
        leaderNeverCommitted();

        poll(follower2);
        pollLeader(1);

        leaderCommitted(0, position);
    }

    @Test
    public void shouldTimeoutLeader()
    {
        follower1.poll(FRAGMENT_LIMIT, TIMEOUT + 1);

        becomesCandidate(replicator2);
    }

    @Test
    public void shouldNotTimeoutLeaderIfMessagesReceived()
    {
        offerBuffer();

        follower1.poll(FRAGMENT_LIMIT, HEARTBEAT_INTERVAL);

        follower1.poll(FRAGMENT_LIMIT, TIMEOUT + 1);

        staysFollower(replicator2);
    }

    @Test
    public void shouldNotTimeoutLeaderUponHeartbeatReceipt()
    {
        leader.poll(FRAGMENT_LIMIT, HEARTBEAT_INTERVAL + 1);

        follower1.poll(FRAGMENT_LIMIT, TIMEOUT + 1);

        staysFollower(replicator2);
    }

    @Test
    public void shouldNotHeartbeatIfMessageRecentlySent()
    {
        leader.updateHeartbeatInterval(HEARTBEAT_INTERVAL / 2);

        leader.poll(FRAGMENT_LIMIT, HEARTBEAT_INTERVAL + 1);

        final ControlHandler controlHandler = mock(ControlHandler.class);

        final int readMessages = controlSubscription().poll(new ControlSubscriber(controlHandler), 10);
        assertEquals(0, readMessages);
        verify(controlHandler, never()).onConcensusHeartbeat(anyShort(), anyInt(), anyLong());
    }

    // TODO: test gapfill scenario

    private int roundtripABuffer()
    {
        final int position = offerBuffer();

        poll(follower1);
        poll(follower2);

        pollLeader(2);
        return position;
    }

    private void pollLeader(final int read)
    {
        assertEquals(read, poll(leader));
    }

    private int offerBuffer()
    {
        final long position = dataPublication.offer(buffer);
        assertThat(position, greaterThan(0L));
        return (int) position;
    }

    private void leaderCommitted(final int offset, final int length)
    {
        final ArgumentCaptor<DirectBuffer> bufferCaptor = ArgumentCaptor.forClass(DirectBuffer.class);
        verify(leaderHandler).onBlock(bufferCaptor.capture(), eq(offset), eq(length), anyInt(), anyInt());
        final DirectBuffer buffer = bufferCaptor.getValue();
        assertEquals(VALUE, buffer.getInt(OFFSET + DataHeaderFlyweight.HEADER_LENGTH));
    }

    private void leaderNeverCommitted()
    {
        verify(leaderHandler, never()).onBlock(any(), anyInt(), anyInt(), anyInt(), anyInt());
    }

    private void leaderNotCommitted(final int offset, final int length)
    {
        verify(leaderHandler, never()).onBlock(any(), eq(offset), eq(length), anyInt(), anyInt());
    }

}
