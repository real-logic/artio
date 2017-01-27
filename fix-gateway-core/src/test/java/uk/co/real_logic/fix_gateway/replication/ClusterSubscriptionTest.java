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
import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import io.aeron.logbuffer.Header;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Before;
import org.junit.Test;
import org.mockito.verification.VerificationMode;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
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
    private static final int THIRD_LEADER = 3;

    private Subscription dataSubscription = mock(Subscription.class);
    private Subscription controlSubscription = mock(Subscription.class);
    private Header header = mock(Header.class);
    private Image leaderDataImage = mock(Image.class);
    private Image otherLeaderDataImage = mock(Image.class);
    private Image thirdLeaderDataImage = mock(Image.class);
    private ControlledFragmentHandler handler = mock(ControlledFragmentHandler.class);

    private ClusterSubscription clusterSubscription = new ClusterSubscription(
        dataSubscription, CLUSTER_STREAM_ID, controlSubscription);

    @Before
    public void setUp()
    {
        when(dataSubscription.imageBySessionId(LEADER)).thenReturn(leaderDataImage);
        when(dataSubscription.imageBySessionId(OTHER_LEADER)).thenReturn(otherLeaderDataImage);
        when(dataSubscription.imageBySessionId(THIRD_LEADER)).thenReturn(thirdLeaderDataImage);

        when(handler.onFragment(any(), anyInt(), anyInt(), any())).thenReturn(CONTINUE);

        when(header.reservedValue()).thenReturn(ReservedValue.ofClusterStreamId(CLUSTER_STREAM_ID));
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
    public void shouldTransitionBetweenLeadersWithDifferentPositionDeltas()
    {
        final int leaderStreamPosition = 128;
        onConsensusHeartbeatPoll(1, LEADER, 128, 0, leaderStreamPosition);
        pollsMessageFragment(leaderDataImage, leaderStreamPosition, CONTINUE);

        final int otherLeaderStreamPosition = 128;
        onConsensusHeartbeatPoll(2, OTHER_LEADER, 256, 0, otherLeaderStreamPosition);
        pollsMessageFragment(otherLeaderDataImage, otherLeaderStreamPosition, CONTINUE);

        assertState(2, OTHER_LEADER, otherLeaderStreamPosition);
        verifyReceivesFragment(leaderStreamPosition, times(2));
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

        onConsensusHeartbeatPoll(3, THIRD_LEADER, 4, 2, 4);

        assertState(1, LEADER, 1);
    }

    @Test
    public void shouldUpdatePositionFromFutureLeadershipTerm()
    {
        shouldStashUpdatesFromFutureLeadershipTerm();

        onConsensusHeartbeatPoll(2, OTHER_LEADER, 2, 1, 2);

        assertState(2, OTHER_LEADER, 2);

        clusterSubscription.hasMatchingFutureAck();

        assertState(3, THIRD_LEADER, 4);
    }

    @Test
    public void shouldCommitUpdatesFromFutureLeadershipTermWithDifferentPositionDeltas()
    {
        // NB: uses different lengths to identify which leader was being polled in the handler verify
        final int firstTermLen = 128;
        final int secondTermLen = 256;
        final int thirdTermLen = 384;
        final int firstTermEnd = firstTermLen;
        final int secondTermEnd = firstTermEnd + secondTermLen;
        final int thirdTermEnd = secondTermEnd + thirdTermLen;

        onConsensusHeartbeatPoll(1, LEADER, firstTermEnd, 0, firstTermLen);
        pollsMessageFragment(leaderDataImage, firstTermEnd, CONTINUE);

        onConsensusHeartbeatPoll(3, THIRD_LEADER, thirdTermEnd, 0, thirdTermLen);

        onConsensusHeartbeatPoll(2, OTHER_LEADER, secondTermEnd, 0, secondTermLen);
        pollsMessageFragment(otherLeaderDataImage, secondTermLen, CONTINUE);

        clusterSubscription.hasMatchingFutureAck();
        pollsMessageFragment(thirdLeaderDataImage, thirdTermLen, CONTINUE);

        verifyReceivesFragment(firstTermLen);
        verifyReceivesFragment(secondTermLen);
        verifyReceivesFragment(thirdTermLen);
    }

    @Test
    public void replicateClusterReplicationTestBug()
    {
        final int leaderShipTermId = 1;
        final int newPosition = 1376;

        // Subscription Heartbeat(leaderShipTerm=1, startPos=0, pos=0, leaderSessId=432774274)
        onConsensusHeartbeatPoll(leaderShipTermId, LEADER, 0, 0, 0);
        // Subscription Heartbeat(leaderShipTerm=1, startPos=0, pos=1376, leaderSessId=432774274)
        onConsensusHeartbeatPoll(leaderShipTermId, LEADER, newPosition, 0, newPosition);

        // Subscription onFragment(headerPosition=1376, consensusPosition=1376
        pollsMessageFragment(leaderDataImage, newPosition, CONTINUE);

        verify(leaderDataImage).controlledPoll(any(), eq(1));
        verifyReceivesFragment(newPosition);
    }

    private void verifyReceivesFragment(final int newStreamPosition)
    {
        verifyReceivesFragment(newStreamPosition, times(1));
    }

    private void verifyReceivesFragment(final int newStreamPosition, final VerificationMode times)
    {
        verify(handler, times).onFragment(any(UnsafeBuffer.class), eq(0), eq(newStreamPosition), eq(header));
    }

    private void pollsMessageFragment(
        final Image dataImage,
        final int newStreamPosition,
        final Action expectedAction)
    {
        when(dataImage.controlledPoll(any(), anyInt())).thenAnswer(
            (inv) ->
            {
                final ControlledFragmentHandler handler = (ControlledFragmentHandler) inv.getArguments()[0];

                when(header.position()).thenReturn((long) newStreamPosition);

                final UnsafeBuffer buffer = new UnsafeBuffer(new byte[newStreamPosition]);
                final Action action = handler.onFragment(buffer, 0, newStreamPosition, header);
                assertEquals(expectedAction, action);
                return null;
            }).then(inv -> null);

        poll();
    }

    private void poll()
    {
        clusterSubscription.controlledPoll(handler, 1);
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
        final long streamPosition)
    {
        assertThat(clusterSubscription,
            hasResult(
                "currentLeadershipTermId",
                ClusterSubscription::currentLeadershipTermId,
                equalTo(currentLeadershipTermId)));

        verify(dataSubscription, atLeastOnce()).imageBySessionId(eq(leadershipSessionId));

        assertThat(clusterSubscription,
            hasResult(
                "streamPosition",
                ClusterSubscription::streamPosition,
                equalTo(streamPosition)));
    }
}
