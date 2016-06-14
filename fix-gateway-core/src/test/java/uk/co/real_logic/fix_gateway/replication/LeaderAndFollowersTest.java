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

import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.ControlledFragmentHandler;
import org.agrona.DirectBuffer;
import org.agrona.collections.IntHashSet;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import uk.co.real_logic.fix_gateway.engine.logger.ArchiveMetaData;
import uk.co.real_logic.fix_gateway.engine.logger.ArchiveReader;
import uk.co.real_logic.fix_gateway.engine.logger.Archiver;

import java.util.concurrent.atomic.AtomicInteger;

import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.fix_gateway.engine.EngineConfiguration.DEFAULT_LOGGER_CACHE_NUM_SETS;
import static uk.co.real_logic.fix_gateway.engine.EngineConfiguration.DEFAULT_LOGGER_CACHE_SET_SIZE;
import static uk.co.real_logic.fix_gateway.replication.RandomTimeout.MAX_TO_MIN_TIMEOUT;

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
    private static final int CLUSTER_STREAM_ID = 1;

    private AtomicBuffer buffer = new UnsafeBuffer(new byte[1024]);

    private ControlledFragmentHandler leaderHandler = mock(ControlledFragmentHandler.class);
    private ControlledFragmentHandler follower1Handler = mock(ControlledFragmentHandler.class);

    private int leaderSessionId;
    private ClusterPublication publication;
    private Leader leader;
    private ClusterSubscription leaderSubscription;
    private Follower follower1;
    private ClusterSubscription follower1Subscription;
    private Follower follower2;

    @Before
    public void setUp()
    {
        buffer.putInt(OFFSET, VALUE);

        final IntHashSet followers = new IntHashSet(10, -1);
        followers.add(2);
        followers.add(3);

        final Publication dataPublication = dataPublication();
        final ClusterNode leaderNode = mock(ClusterNode.class);
        when(leaderNode.isPublishable()).thenReturn(true);
        publication = new ClusterPublication(dataPublication, leaderNode, 1);
        leaderSessionId = dataPublication.sessionId();

        termState1.leaderSessionId(leaderSessionId);
        termState2.leaderSessionId(leaderSessionId);
        termState3.leaderSessionId(leaderSessionId);

        final ArchiveMetaData metaData = archiveMetaData(LEADER_ID);
        final Subscription subscription = dataSubscription();
        final StreamIdentifier streamId = new StreamIdentifier(subscription);
        final ArchiveReader archiveReader = new ArchiveReader(
            metaData, DEFAULT_LOGGER_CACHE_NUM_SETS, DEFAULT_LOGGER_CACHE_SET_SIZE, streamId);
        final Archiver archiver = new Archiver(
            metaData,
            DEFAULT_LOGGER_CACHE_NUM_SETS,
            DEFAULT_LOGGER_CACHE_SET_SIZE,
            streamId)
            .subscription(subscription);

        leader = new Leader(
            LEADER_ID,
            new EntireClusterAcknowledgementStrategy(),
            followers,
            clusterNode1,
            0,
            HEARTBEAT_INTERVAL,
            termState1,
            leaderSessionId,
            archiveReader,
            archiver
        )
            .controlPublication(raftPublication(ClusterNodeConfiguration.DEFAULT_CONTROL_STREAM_ID))
            .controlSubscription(controlSubscription())
            .acknowledgementSubscription(acknowledgementSubscription())
            .dataSubscription(dataSubscription());

        follower1 = follower(FOLLOWER_1_ID, clusterNode2, termState2);
        follower2 = follower(FOLLOWER_2_ID, clusterNode3, termState3);

        leaderSubscription = new ClusterSubscription(
            dataSubscription(),
            CLUSTER_STREAM_ID,
            termState1.consensusPosition(),
            new AtomicInteger(leaderSessionId));

        follower1Subscription = new ClusterSubscription(
            dataSubscription(),
            CLUSTER_STREAM_ID,
            termState2.consensusPosition(),
            new AtomicInteger(leaderSessionId));
    }

    @Test
    public void shouldNotProcessDataUntilAcknowledged()
    {
        offerBuffer();

        poll(leader);

        pollLeaderSubscription();

        leaderNeverCommitted();
    }

    @Test
    public void shouldProcessDataWhenAcknowledged()
    {
        final int position = roundtripABuffer();

        leaderCommitted(0, position);
    }

    @Test
    public void shouldCommitOnFollowers()
    {
        final int position = roundtripABuffer();

        poll(follower1, 1);
        pollFollower1();

        verify(follower1Handler).onFragment(any(), eq(HEADER_LENGTH), eq(position - HEADER_LENGTH), any());
    }

    @Test
    public void shouldProcessSuccessiveChunks()
    {
        final int position1 = roundtripABuffer();
        leaderCommitted(0, position1);

        final int secondValue = VALUE + 1;
        buffer.putInt(OFFSET, secondValue);

        final int position2 = roundtripABuffer();
        pollLeader(1);
        pollLeaderSubscription();
        leaderCommitted(position1, position2 - position1, secondValue);
    }

    @Test
    public void shouldRequireContiguousMessages()
    {
        final int position1 = roundtripABuffer();
        leaderCommitted(0, position1);

        follower1.follow(0);

        final int position2 = roundtripABuffer();

        leaderNotCommitted(position1, position2 - position1);
    }

    @Test
    public void shouldRequireQuorumToProcess()
    {
        offerBuffer();

        pollFollower1();

        pollLeader(1);
        leaderNeverCommitted();
    }

    @Test
    public void shouldSupportAcknowledgementLagging()
    {
        final int position = offerBuffer();

        pollFollower1();

        pollLeader(1);
        leaderNeverCommitted();

        poll(follower2);
        pollLeader(1);
        pollLeaderSubscription();

        leaderCommitted(0, position);
    }

    @Test
    public void shouldTimeoutLeader()
    {
        final long afterTimeout = MAX_TO_MIN_TIMEOUT * TIMEOUT + 1;
        follower1.poll(FRAGMENT_LIMIT, afterTimeout);
        follower2.poll(FRAGMENT_LIMIT, afterTimeout);

        ReplicationAsserts.transitionsToCandidate(clusterNode2);
        ReplicationAsserts.transitionsToCandidate(clusterNode3);
    }

    @Test
    public void shouldNotTimeoutLeaderIfMessagesReceived()
    {
        offerBuffer();

        follower1.poll(FRAGMENT_LIMIT, HEARTBEAT_INTERVAL);

        follower1.poll(FRAGMENT_LIMIT, TIMEOUT + 1);

        ReplicationAsserts.staysFollower(clusterNode2);
    }

    @Test
    public void shouldNotTimeoutLeaderUponHeartbeatReceipt()
    {
        leader.poll(FRAGMENT_LIMIT, HEARTBEAT_INTERVAL + 1);

        follower1.poll(FRAGMENT_LIMIT, TIMEOUT + 1);

        ReplicationAsserts.staysFollower(clusterNode2);
    }

    @Test
    public void shouldNotHeartbeatIfMessageRecentlySent()
    {
        leader.updateHeartbeatInterval(HEARTBEAT_INTERVAL / 2);

        leader.poll(FRAGMENT_LIMIT, HEARTBEAT_INTERVAL + 1);

        final RaftHandler raftHandler = mock(RaftHandler.class);

        final int readMessages = controlSubscription().controlledPoll(new RaftSubscription(raftHandler), 10);
        assertEquals(0, readMessages);
        verify(raftHandler, never())
            .onConsensusHeartbeat(anyShort(), anyInt(), anyLong(), eq(leaderSessionId));
    }

    private int roundtripABuffer()
    {
        final int position = offerBuffer();

        pollFollower1();
        poll(follower2);

        pollLeader(2);
        pollLeaderSubscription();
        return position;
    }

    private void pollFollower1()
    {
        poll(follower1);
        follower1Subscription.controlledPoll(follower1Handler, FRAGMENT_LIMIT);
    }

    private void pollLeader(int toRead)
    {
        poll(leader, toRead);
    }

    private int pollLeaderSubscription()
    {
        return leaderSubscription.controlledPoll(leaderHandler, FRAGMENT_LIMIT);
    }

    private void poll(final Role role, int toRead)
    {
        while (toRead > 0)
        {
            toRead -= poll(role);
        }
    }

    private int offerBuffer()
    {
        final BufferClaim claim = new BufferClaim();
        final long position = publication.tryClaim(buffer.capacity(), claim);
        assertThat(position, greaterThan(0L));
        claim.buffer().putBytes(claim.offset(), buffer, 0, buffer.capacity());
        claim.commit();

        return (int) position;
    }

    private void leaderCommitted(int offset, int length)
    {
        leaderCommitted(offset, length, VALUE);
    }

    private void leaderCommitted(int offset, int length, final int value)
    {
        offset += HEADER_LENGTH;
        length -= HEADER_LENGTH;
        final ArgumentCaptor<DirectBuffer> bufferCaptor = ArgumentCaptor.forClass(DirectBuffer.class);
        verify(leaderHandler, atLeastOnce())
            .onFragment(bufferCaptor.capture(), eq(offset), eq(length), any());
        final DirectBuffer buffer = bufferCaptor.getValue();
        assertEquals(value, buffer.getInt(offset + OFFSET));
    }

    private void leaderNeverCommitted()
    {
        verify(leaderHandler, never()).onFragment(any(), anyInt(), anyInt(), any());
    }

    private void leaderNotCommitted(final int offset, final int length)
    {
        verify(leaderHandler, never()).onFragment(any(), eq(offset), eq(length), any());
    }

}
