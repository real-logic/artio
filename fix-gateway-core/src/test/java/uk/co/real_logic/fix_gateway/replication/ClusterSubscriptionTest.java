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

import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Before;
import org.junit.Test;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.fix_gateway.util.CustomMatchers.hasResult;

/**
 * Test technically breaches encapsulation of ClusterSubscription
 * deliberate tradeoff to avoid additional indirection and test complexity.
 */
public class ClusterSubscriptionTest
{
    private static final int CLUSTER_STREAM_ID = 1;
    private static final int LEADER = 1;
    private static final int OTHER_LEADER = 2;

    private Subscription dataSubscription = mock(Subscription.class);
    private Subscription controlSubscription = mock(Subscription.class);
    private Image dataImage = mock(Image.class);
    private ControlledFragmentHandler handler = mock(ControlledFragmentHandler.class);

    private ClusterSubscription clusterSubscription = new ClusterSubscription(
        dataSubscription, CLUSTER_STREAM_ID, controlSubscription);

    @Before
    public void setUp()
    {
        when(dataSubscription.imageBySessionId(anyInt())).thenReturn(dataImage);
    }

    @Test
    public void shouldUpdatePositionWhenAcknowledged()
    {
        onConsensusHeartbeatPoll(1, LEADER, 1, 0, 1);

        onConsensusHeartbeatPoll(1, LEADER, 2, 1, 2);

        assertState(1, LEADER, 2);
    }

    @Test
    public void shouldStashUpdatesWithGap()
    {
        onConsensusHeartbeatPoll(1, LEADER, 1, 0, 1);

        onConsensusHeartbeatPoll(2, OTHER_LEADER, 4, 2, 4);

        assertState(1, LEADER, 1);
    }

    @Test
    public void shouldApplyUpdatesWhenGapFilled()
    {
        shouldStashUpdatesWithGap();

        onConsensusHeartbeatPoll(1, LEADER, 2, 1, 2);

        assertState(1, LEADER, 2);

        clusterSubscription.hasMatchingFutureAck();

        assertState(2, OTHER_LEADER, 4);
    }

    @Test
    public void shouldStashUpdatesFromFutureLeadershipTerm()
    {
        onConsensusHeartbeatPoll(1, LEADER, 1, 0, 1);

        onConsensusHeartbeatPoll(3, OTHER_LEADER, 4, 2, 4);

        assertState(1, LEADER, 1);
    }

    @Test
    public void shouldApplyUpdatesFromFutureLeadershipTerm()
    {
        shouldStashUpdatesFromFutureLeadershipTerm();

        onConsensusHeartbeatPoll(2, OTHER_LEADER, 2, 1, 2);

        assertState(2, OTHER_LEADER, 2);

        clusterSubscription.hasMatchingFutureAck();

        assertState(3, OTHER_LEADER, 4);
    }

    @Test
    public void replicateClusterReplicationTestBug()
    {
        final int leaderShipTermId = 1;
        final int leaderSessionId = 432774274;
        final int newPosition = 1376;

        // Subscription Heartbeat(leaderShipTerm=1, startPos=0, pos=0, leaderSessId=432774274)
        onConsensusHeartbeatPoll(leaderShipTermId, leaderSessionId, 0, 0, 0);
        // Subscription Heartbeat(leaderShipTerm=1, startPos=0, pos=1376, leaderSessId=432774274)
        onConsensusHeartbeatPoll(leaderShipTermId, leaderSessionId, newPosition, 0, newPosition);

        // Subscription onFragment(headerPosition=1376, consensusPosition=1376
        when(handler.onFragment(any(), anyInt(), anyInt(), any())).thenReturn(CONTINUE);
        when(dataImage.controlledPoll(any(), anyInt())).thenAnswer(
            (inv) ->
            {
                final ControlledFragmentHandler handler = (ControlledFragmentHandler) inv.getArguments()[0];

                final Header header = mock(Header.class);
                when(header.position()).thenReturn((long) newPosition);
                when(header.reservedValue()).thenReturn(ReservedValue.ofClusterStreamId(CLUSTER_STREAM_ID));

                final UnsafeBuffer buffer = new UnsafeBuffer(new byte[newPosition]);
                handler.onFragment(buffer, 0, newPosition, header);
                return null;
            });

        final int fragmentLimit = 10;
        clusterSubscription.controlledPoll(handler, fragmentLimit);

        verify(dataImage).controlledPoll(any(), eq(fragmentLimit));
        verify(handler).onFragment(any(), eq(0), eq(newPosition), any());
    }

    private void onConsensusHeartbeatPoll(
        final int leaderShipTermId,
        final int leaderSessionId,
        final long position,
        final long streamStartPosition,
        final long streamPosition)
    {
        clusterSubscription.hasMatchingFutureAck();
        clusterSubscription.onConsensusHeartbeat(
            leaderShipTermId, leaderSessionId, position, streamStartPosition, streamPosition);
    }

    private void assertState(
        final int currentLeadershipTermId,
        final Integer leadershipSessionId,
        final long currentConsensusPosition)
    {
        assertThat(clusterSubscription,
            hasResult(
                "currentLeadershipTermId",
                ClusterSubscription::currentLeadershipTermId,
                equalTo(currentLeadershipTermId)));

        verify(dataSubscription, atLeastOnce()).imageBySessionId(eq(leadershipSessionId));

        assertThat(clusterSubscription,
            hasResult(
                "currentConsensusPosition",
                ClusterSubscription::currentConsensusPosition,
                equalTo(currentConsensusPosition)));
    }
}
