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
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.verification.VerificationMode;
import uk.co.real_logic.fix_gateway.engine.logger.ArchiveReader;
import uk.co.real_logic.fix_gateway.engine.logger.ArchiveReader.SessionReader;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.ABORT;
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

    // Standard position points for tests
    // NB: uses different lengths to identify which leader was being polled in the handler verify

    private static final int FIRST_TERM_LENGTH = 128;
    private static final int SECOND_TERM_LENGTH = 256;
    private static final int THIRD_TERM_LENGTH = 384;
    private static final int FIRST_TERM_END = FIRST_TERM_LENGTH;
    private static final int SECOND_TERM_END = FIRST_TERM_END + SECOND_TERM_LENGTH;
    private static final int THIRD_TERM_END = SECOND_TERM_END + THIRD_TERM_LENGTH;
    private static final int THIRD_TERM_STREAM_END = SECOND_TERM_LENGTH + THIRD_TERM_LENGTH;
    private static final long THIRD_TERM_STREAM_START = SECOND_TERM_LENGTH;

    private Subscription dataSubscription = mock(Subscription.class);
    private Subscription controlSubscription = mock(Subscription.class);
    private Header header = mock(Header.class);
    private Image leaderDataImage = mock(Image.class);
    private Image otherLeaderDataImage = mock(Image.class);
    private Image thirdLeaderDataImage = mock(Image.class);
    private ClusterFragmentHandler handler = mock(ClusterFragmentHandler.class);

    private ArchiveReader archiveReader = mock(ArchiveReader.class);
    private SessionReader otherLeaderArchiveReader = mock(SessionReader.class);

    private ClusterSubscription clusterSubscription = new ClusterSubscription(
        dataSubscription, CLUSTER_STREAM_ID, controlSubscription, archiveReader);

    @Before
    public void setUp()
    {
        leaderImageAvailable();
        otherLeaderImageAvailable();
        imageAvailable(thirdLeaderDataImage, THIRD_LEADER);

        when(handler.onFragment(any(), anyInt(), anyInt(), any())).thenReturn(CONTINUE);

        when(header.reservedValue()).thenReturn(ReservedValue.ofClusterStreamId(CLUSTER_STREAM_ID));

        archiveReaderAvailable();
    }

    @Test
    public void shouldUpdatePositionWhenAcknowledged()
    {
        onConsensusHeartbeatPoll(1, LEADER, 1, 0, 1);

        onConsensusHeartbeatPoll(1, LEADER, 2, 1, 2);

        assertState(1, LEADER, 2);
    }

    @Test
    public void shouldTransitionBetweenLeadersWithDifferentPositionDeltas()
    {
        final int firstTermStreamEnd = FIRST_TERM_LENGTH;
        final int secondTermStreamEnd = SECOND_TERM_LENGTH;

        onConsensusHeartbeatPoll(1, LEADER, FIRST_TERM_END, 0, firstTermStreamEnd);
        pollsMessageFragment(leaderDataImage, firstTermStreamEnd, CONTINUE);

        onConsensusHeartbeatPoll(2, OTHER_LEADER, SECOND_TERM_END, 0, secondTermStreamEnd);
        pollsMessageFragment(otherLeaderDataImage, secondTermStreamEnd, CONTINUE);

        assertState(2, OTHER_LEADER, secondTermStreamEnd);
        verifyReceivesFragment(FIRST_TERM_LENGTH);
        verifyReceivesFragment(SECOND_TERM_LENGTH);
        verifyNoOtherFragmentsReceived();
    }

    @Test
    public void shouldTransitionBetweenLeadersWithDifferentPositionDeltasWhenDataLagsControl()
    {
        imageNotAvailable(LEADER);
        imageNotAvailable(OTHER_LEADER);

        final int firstTermLength = 128;
        final int firstTermPosition = firstTermLength;
        final int firstTermStreamPosition = firstTermLength;
        final int secondTermLength = 256;
        final int secondTermStreamPosition = secondTermLength;
        final int secondTermPosition = firstTermPosition + secondTermLength;

        onConsensusHeartbeatPoll(1, LEADER, firstTermPosition, 0, firstTermStreamPosition);
        leaderImageAvailable();
        onConsensusHeartbeatPoll(1, LEADER, firstTermPosition, 0, firstTermStreamPosition);

        pollsMessageFragment(leaderDataImage, firstTermStreamPosition, CONTINUE);

        onConsensusHeartbeatPoll(2, OTHER_LEADER, secondTermPosition, 0, secondTermStreamPosition);
        otherLeaderImageAvailable();
        onConsensusHeartbeatPoll(2, OTHER_LEADER, secondTermPosition, 0, secondTermStreamPosition);

        pollsMessageFragment(otherLeaderDataImage, secondTermStreamPosition, CONTINUE);

        assertState(2, OTHER_LEADER, secondTermStreamPosition);
        verifyReceivesFragment(firstTermLength);
        verifyReceivesFragment(secondTermLength);
        verifyNoOtherFragmentsReceived();
    }

    @Test
    public void shouldStashUpdatesWithGap()
    {
        onConsensusHeartbeatPoll(1, LEADER, FIRST_TERM_END, 0, FIRST_TERM_END);

        onConsensusHeartbeatPoll(2, OTHER_LEADER, THIRD_TERM_END, 0, THIRD_TERM_LENGTH);

        assertState(1, LEADER, FIRST_TERM_END);
    }

    @Test
    public void shouldApplyUpdatesWhenGapFilled()
    {
        shouldStashUpdatesWithGap();

        onConsensusHeartbeatPoll(1, LEADER, SECOND_TERM_END, FIRST_TERM_END, SECOND_TERM_END);

        assertState(1, LEADER, SECOND_TERM_END);

        clusterSubscription.hasMatchingFutureAck();

        assertState(2, OTHER_LEADER, THIRD_TERM_LENGTH);
    }

    @Test
    public void shouldStashUpdatesFromFutureLeadershipTerm()
    {
        onConsensusHeartbeatPoll(1, LEADER, FIRST_TERM_END, 0, FIRST_TERM_LENGTH);

        onConsensusHeartbeatPoll(3, THIRD_LEADER, THIRD_TERM_END, 0, THIRD_TERM_LENGTH);

        assertState(1, LEADER, FIRST_TERM_END);
    }

    @Test
    public void shouldUpdatePositionFromFutureLeadershipTerm()
    {
        shouldStashUpdatesFromFutureLeadershipTerm();

        onConsensusHeartbeatPoll(2, OTHER_LEADER, SECOND_TERM_END, 0, SECOND_TERM_LENGTH);

        assertState(2, OTHER_LEADER, SECOND_TERM_LENGTH);

        clusterSubscription.hasMatchingFutureAck();

        assertState(3, THIRD_LEADER, THIRD_TERM_LENGTH);
    }

    @Test
    public void shouldCommitUpdatesFromFutureLeadershipTermWithDifferentPositionDeltas()
    {
        willReceiveConsensusHeartbeat(1, LEADER, FIRST_TERM_END, 0, FIRST_TERM_LENGTH);
        pollsMessageFragment(leaderDataImage, FIRST_TERM_END, CONTINUE);

        onConsensusHeartbeatPoll(3, THIRD_LEADER, THIRD_TERM_END, 0, THIRD_TERM_LENGTH);

        willReceiveConsensusHeartbeat(2, OTHER_LEADER, SECOND_TERM_END, 0, SECOND_TERM_LENGTH);
        pollsMessageFragment(otherLeaderDataImage, SECOND_TERM_LENGTH, CONTINUE);

        pollsMessageFragment(thirdLeaderDataImage, THIRD_TERM_LENGTH, CONTINUE);

        verifyReceivesFragment(FIRST_TERM_LENGTH);
        verifyReceivesFragment(SECOND_TERM_LENGTH);
        verifyReceivesFragment(THIRD_TERM_LENGTH);
        verifyNoOtherFragmentsReceived();
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

        verify(leaderDataImage).controlledPeek(anyLong(), any(), anyLong());
        verifyReceivesFragment(newPosition);
    }

    // Scenario for resend tests:
    // A leader has committed data to a quorum of nodes excluding you, then it dies.
    // Your only way of receiving that data is through resend on the control stream
    // You may have missed some concensus messages as well.

    @Test
    public void shouldCommitResendDataIfNextThingInStream()
    {
        onConsensusHeartbeatPoll(1, LEADER, FIRST_TERM_END, 0, FIRST_TERM_LENGTH);
        pollsMessageFragment(leaderDataImage, FIRST_TERM_END, CONTINUE);

        onResend(0, FIRST_TERM_END, SECOND_TERM_LENGTH);
        onResend(SECOND_TERM_LENGTH, SECOND_TERM_END, THIRD_TERM_LENGTH);

        verifyReceivesFragment(FIRST_TERM_LENGTH);
        verifyReceivesFragment(SECOND_TERM_LENGTH);
        verifyReceivesFragment(THIRD_TERM_LENGTH);
        verifyNoOtherFragmentsReceived();
    }

    @Test
    public void shouldNotReceiveResendDataTwiceResendFirst()
    {
        onConsensusHeartbeatPoll(1, LEADER, FIRST_TERM_END, 0, FIRST_TERM_LENGTH);
        pollsMessageFragment(leaderDataImage, FIRST_TERM_LENGTH, CONTINUE);

        onResend(0, FIRST_TERM_END, SECOND_TERM_LENGTH);

        onConsensusHeartbeatPoll(2, OTHER_LEADER, SECOND_TERM_END, 0, SECOND_TERM_LENGTH);
        pollsMessageFragment(otherLeaderDataImage, SECOND_TERM_LENGTH, CONTINUE);

        onResend(SECOND_TERM_LENGTH, SECOND_TERM_END, THIRD_TERM_LENGTH);

        verifyReceivesFragment(FIRST_TERM_LENGTH);
        verifyReceivesFragment(SECOND_TERM_LENGTH);
        verifyReceivesFragment(THIRD_TERM_LENGTH);
        verifyNoOtherFragmentsReceived();
    }

    @Test
    public void shouldNotReceiveResendDataTwiceHeartbeatFirst()
    {
        onConsensusHeartbeatPoll(1, LEADER, FIRST_TERM_END, 0, FIRST_TERM_LENGTH);
        pollsMessageFragment(leaderDataImage, FIRST_TERM_LENGTH, CONTINUE);

        onConsensusHeartbeatPoll(2, OTHER_LEADER, SECOND_TERM_END, 0, SECOND_TERM_LENGTH);
        pollsMessageFragment(otherLeaderDataImage, SECOND_TERM_LENGTH, CONTINUE);

        onResend(0, FIRST_TERM_END, SECOND_TERM_LENGTH);

        onResend(SECOND_TERM_LENGTH, SECOND_TERM_END, THIRD_TERM_LENGTH);

        verifyReceivesFragment(FIRST_TERM_LENGTH);
        verifyReceivesFragment(SECOND_TERM_LENGTH);
        verifyReceivesFragment(THIRD_TERM_LENGTH);
        verifyNoOtherFragmentsReceived();
    }

    @Test
    public void shouldContinueToReceiveNormalDataAfterAResend()
    {
        onConsensusHeartbeatPoll(1, LEADER, FIRST_TERM_END, 0, FIRST_TERM_LENGTH);
        pollsMessageFragment(leaderDataImage, FIRST_TERM_LENGTH, CONTINUE);

        onResend(0, FIRST_TERM_END, SECOND_TERM_LENGTH);

        onConsensusHeartbeatPoll(2, OTHER_LEADER, THIRD_TERM_END, SECOND_TERM_LENGTH, THIRD_TERM_STREAM_END);
        pollsMessageFragment(otherLeaderDataImage, THIRD_TERM_STREAM_END, THIRD_TERM_LENGTH, CONTINUE);

        verifyReceivesFragment(FIRST_TERM_LENGTH);
        verifyReceivesFragment(SECOND_TERM_LENGTH);
        verifyReceivesFragment(THIRD_TERM_LENGTH);
        verifyNoOtherFragmentsReceived();
    }

    @Ignore
    @Test
    public void shouldCommitFromLocalLogIfGapInSubscription()
    {
        // You might receive resends out of order, using the raft leader-probing mechanism for resends.

        onConsensusHeartbeatPoll(1, LEADER, FIRST_TERM_END, 0, FIRST_TERM_LENGTH);
        pollsMessageFragment(leaderDataImage, FIRST_TERM_LENGTH, CONTINUE);

        // You got netsplit when the data was sent out on the main data channel
        when(otherLeaderDataImage.position()).thenReturn((long) THIRD_TERM_STREAM_END);

        // But the data has been resend and archived by the follower.
        onResend(SECOND_TERM_LENGTH, SECOND_TERM_END, THIRD_TERM_LENGTH);
        dataWasArchived(THIRD_TERM_STREAM_START, THIRD_TERM_STREAM_END, CONTINUE);

        onResend(0, FIRST_TERM_END, SECOND_TERM_LENGTH);

        poll();

        verifyReceivesFragment(FIRST_TERM_LENGTH);
        verifyReceivesFragment(SECOND_TERM_LENGTH);
        verifyReceivesFragment(THIRD_TERM_LENGTH);
        verifyNoOtherFragmentsReceived();
    }

    @Ignore
    @Test
    public void shouldCommitFromLocalLogWhenArchiverLags()
    {
        archiveReaderUnavailable();

        onConsensusHeartbeatPoll(1, LEADER, FIRST_TERM_END, 0, FIRST_TERM_LENGTH);
        pollsMessageFragment(leaderDataImage, FIRST_TERM_LENGTH, CONTINUE);

        // You got netsplit when the data was sent out on the main data channel
        when(otherLeaderDataImage.position()).thenReturn((long) THIRD_TERM_STREAM_END);

        // But the data has been resend and archived by the follower.
        onResend(SECOND_TERM_LENGTH, SECOND_TERM_END, THIRD_TERM_LENGTH);
        dataWasArchived(THIRD_TERM_STREAM_START, THIRD_TERM_STREAM_END, CONTINUE);

        onResend(0, FIRST_TERM_END, SECOND_TERM_LENGTH);

        poll();

        archiveReaderAvailable();
        poll();

        verifyReceivesFragment(FIRST_TERM_LENGTH);
        verifyReceivesFragment(SECOND_TERM_LENGTH);
        verifyReceivesFragment(THIRD_TERM_LENGTH);
        verifyNoOtherFragmentsReceived();
    }

    @Test
    public void shouldCommitResendDataAtStart()
    {
        // You might receive resends out of order, using the raft leader-probing mechanism for resends.
        poll();
        when(otherLeaderDataImage.position()).thenReturn(0L);
        onResend(1, 0, 0, FIRST_TERM_LENGTH, CONTINUE);

        onConsensusHeartbeatPoll(2, LEADER, SECOND_TERM_END, 0, SECOND_TERM_LENGTH);
        pollsMessageFragment(leaderDataImage, SECOND_TERM_LENGTH, CONTINUE);

        verifyReceivesFragment(FIRST_TERM_LENGTH);
        verifyReceivesFragment(SECOND_TERM_LENGTH);
        verifyNoOtherFragmentsReceived();
    }

    // Back Pressure Tests

    @Test
    public void shouldPollDataWhenBackPressured()
    {
        final int firstTermStreamEnd = FIRST_TERM_LENGTH;

        backPressureNextCommit();

        willReceiveConsensusHeartbeat(
            1, LEADER, FIRST_TERM_END, 0, firstTermStreamEnd);
        pollsMessageFragment(leaderDataImage, FIRST_TERM_LENGTH, ABORT);
        pollsMessageFragment(leaderDataImage, FIRST_TERM_LENGTH, CONTINUE);

        verifyReceivesFragment(FIRST_TERM_LENGTH, times(2));
        verifyNoOtherFragmentsReceived();
    }

    @Test
    public void shouldCommitResendDataIfNextThingInStreamWhenBackPressured()
    {
        onConsensusHeartbeatPoll(1, LEADER, FIRST_TERM_END, 0, FIRST_TERM_LENGTH);
        pollsMessageFragment(leaderDataImage, FIRST_TERM_END, CONTINUE);

        backPressureNextCommit();
        onResend(2, (long)0, FIRST_TERM_END, SECOND_TERM_LENGTH, ABORT);
        onResend(0, FIRST_TERM_END, SECOND_TERM_LENGTH);
        onResend(SECOND_TERM_LENGTH, SECOND_TERM_END, THIRD_TERM_LENGTH);

        verifyReceivesFragment(FIRST_TERM_LENGTH);
        verifyReceivesFragment(SECOND_TERM_LENGTH, times(2));
        verifyReceivesFragment(THIRD_TERM_LENGTH);
        verifyNoOtherFragmentsReceived();
    }

    @Ignore
    @Test
    public void shouldCommitFromLocalLogIfGapInSubscriptionWhenBackPressured()
    {
        // You might receive resends out of order, using the raft leader-probing mechanism for resends.
        onConsensusHeartbeatPoll(1, LEADER, FIRST_TERM_END, 0, FIRST_TERM_LENGTH);
        pollsMessageFragment(leaderDataImage, FIRST_TERM_LENGTH, CONTINUE);

        // You got netsplit when the data was sent out on the main data channel
        when(otherLeaderDataImage.position()).thenReturn((long) THIRD_TERM_STREAM_END);

        // But the data has been resend and archived by the follower.
        onResend(SECOND_TERM_LENGTH, SECOND_TERM_END, THIRD_TERM_LENGTH);

        onResend(0, FIRST_TERM_END, SECOND_TERM_LENGTH);

        backPressureNextCommit();

        dataWasArchived(THIRD_TERM_STREAM_START, THIRD_TERM_STREAM_END, ABORT);
        poll();

        dataWasArchived(THIRD_TERM_STREAM_START, THIRD_TERM_STREAM_END, CONTINUE);
        poll();

        verifyReceivesFragment(FIRST_TERM_LENGTH);
        verifyReceivesFragment(SECOND_TERM_LENGTH);
        verifyReceivesFragment(THIRD_TERM_LENGTH, times(2));
        verifyNoOtherFragmentsReceived();
    }

    private void backPressureNextCommit()
    {
        when(handler.onFragment(any(), anyInt(), anyInt(), any())).thenReturn(ABORT, CONTINUE);
    }

    private void dataWasArchived(
        final long streamStart, final long streamEnd, final Action expectedAction)
    {
        when(otherLeaderArchiveReader.readUpTo(
            eq(streamStart + DataHeaderFlyweight.HEADER_LENGTH), eq(streamEnd), any()))
            .then(
                (inv) ->
                {
                    callHandler(
                        streamEnd,
                        (int)(streamEnd - streamStart),
                        expectedAction,
                        inv,
                        2);
                    return expectedAction == ABORT ? streamStart : streamEnd;
                });
    }

    private void onResend(final long streamStartPosition, final int startPosition, final int resendLen)
    {
        onResend(2, streamStartPosition, startPosition, resendLen, CONTINUE);
    }

    private void onResend(
        final int leaderShipTerm,
        final long streamStartPosition,
        final int startPosition,
        final int resendLen,
        final Action expectedAction)
    {
        final UnsafeBuffer resendBuffer = new UnsafeBuffer(new byte[resendLen]);
        clusterSubscription.hasMatchingFutureAck();
        final Action action = clusterSubscription.onResend(
            OTHER_LEADER, leaderShipTerm, startPosition, streamStartPosition, resendBuffer, 0, resendLen);
        assertEquals(expectedAction, action);
    }

    private void verifyReceivesFragment(final int newStreamPosition)
    {
        verifyReceivesFragment(newStreamPosition, times(1));
    }

    private void verifyReceivesFragment(final int newStreamPosition, final VerificationMode times)
    {
        verify(handler, times).onFragment(any(UnsafeBuffer.class), eq(0), eq(newStreamPosition), any());
    }

    private void pollsMessageFragment(
        final Image dataImage,
        final int streamPosition,
        final Action expectedAction)
    {
        pollsMessageFragment(dataImage, streamPosition, streamPosition, expectedAction);
    }

    private void pollsMessageFragment(
        final Image dataImage,
        final int streamPosition,
        final int length,
        final Action expectedAction)
    {
        when(dataImage.controlledPeek(anyLong(), any(), anyLong())).thenAnswer(
            (inv) ->
            {
                final long initialPosition = inv.getArgument(0);
                final long limitPosition = inv.getArgument(2);

                if (initialPosition < limitPosition)
                {
                    callHandler(streamPosition, length, expectedAction, inv, 1);
                    if (expectedAction != ABORT)
                    {
                        when(dataImage.position()).thenReturn((long)streamPosition);
                        return (long) streamPosition;
                    }
                }

                return initialPosition;
            }).then(inv -> 0L);

        poll();
    }

    private void callHandler(
        final long streamPosition,
        final int length,
        final Action expectedAction,
        final InvocationOnMock inv,
        final int handlerArgumentIndex)
    {
        final ControlledFragmentHandler handler = (ControlledFragmentHandler)inv.getArguments()[handlerArgumentIndex];

        when(header.position()).thenReturn(streamPosition);

        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[length]);
        final Action action = handler.onFragment(buffer, 0, length, header);
        assertEquals(expectedAction, action);
    }

    private void poll()
    {
        clusterSubscription.poll(handler, 1);
    }

    private void onConsensusHeartbeatPoll(
        final int leaderShipTerm,
        final int leaderSessionId,
        final long position,
        final long streamStartPosition,
        final long streamPosition)
    {
        clusterSubscription.hasMatchingFutureAck();
        clusterSubscription.onConsensusHeartbeat(
            leaderShipTerm, leaderSessionId, position, streamStartPosition, streamPosition);
    }

    private void willReceiveConsensusHeartbeat(
        final int leaderShipTerm,
        final int leader,
        final long position,
        final long streamStartPosition,
        final long streamPosition)
    {
        when(controlSubscription.controlledPoll(any(), anyInt())).then(
            (inv) ->
            {
                clusterSubscription.onConsensusHeartbeat(
                    leaderShipTerm, leader, position, streamStartPosition, streamPosition);
                return 1;
            }).thenReturn(0);
    }

    private void assertState(
        final int currentLeadershipTerm,
        final Integer leadershipSessionId,
        final long transportPosition)
    {
        assertThat(clusterSubscription,
            hasResult(
                "currentLeadershipTerm",
                ClusterSubscription::currentLeadershipTerm,
                equalTo(currentLeadershipTerm)));

        verify(dataSubscription, atLeastOnce()).imageBySessionId(eq(leadershipSessionId));

        assertThat(clusterSubscription,
            hasResult(
                "transportPosition",
                ClusterSubscription::transportPosition,
                equalTo(transportPosition)));
    }

    private void verifyNoOtherFragmentsReceived()
    {
        verifyNoMoreInteractions(handler);
    }

    private void archiveReaderAvailable()
    {
        archiveReader(otherLeaderArchiveReader);
    }

    private void archiveReaderUnavailable()
    {
        archiveReader(null);
    }

    private void archiveReader(final SessionReader archiveReader)
    {
        when(this.archiveReader.session(OTHER_LEADER)).thenReturn(archiveReader);
    }

    private void otherLeaderImageAvailable()
    {
        imageAvailable(otherLeaderDataImage, OTHER_LEADER);
    }

    private void leaderImageAvailable()
    {
        imageAvailable(leaderDataImage, LEADER);
    }

    private void imageAvailable(final Image image, final int aeronSessionId)
    {
        when(dataSubscription.imageBySessionId(aeronSessionId)).thenReturn(image);
        if (image != null)
        {
            when(image.sessionId()).thenReturn(aeronSessionId);
        }
    }

    private void imageNotAvailable(final int aeronSessionId)
    {
        imageAvailable(null, aeronSessionId);
    }

}
