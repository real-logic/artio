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

import io.aeron.Subscription;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.OngoingStubbing;
import org.mockito.verification.VerificationMode;
import uk.co.real_logic.fix_gateway.engine.logger.Archiver;
import uk.co.real_logic.fix_gateway.engine.logger.Archiver.SessionArchiver;

import static io.aeron.Publication.BACK_PRESSURED;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.anyShort;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.fix_gateway.messages.AcknowledgementStatus.MISSING_LOG_ENTRIES;
import static uk.co.real_logic.fix_gateway.messages.AcknowledgementStatus.OK;
import static uk.co.real_logic.fix_gateway.messages.Vote.FOR;

public class FollowerTest
{
    private static final long POSITION = 40;
    private static final int LENGTH = 100;
    private static final long VOTE_TIMEOUT = 100;
    private static final int OLD_LEADERSHIP_TERM = 1;
    private static final int NEW_LEADERSHIP_TERM = OLD_LEADERSHIP_TERM + 1;
    private static final DirectBuffer NODE_STATE_BUFFER = new UnsafeBuffer(new byte[1]);

    private static final short ID = 3;
    private static final short ID_4 = 4;
    private static final int SESSION_ID_4 = 42;
    private static final short ID_5 = 5;
    private static final int SESSION_ID_5 = 43;

    private AtomicBuffer buffer = new UnsafeBuffer(new byte[8 * 1024]);
    private RaftPublication acknowledgementPublication = mock(RaftPublication.class);
    private RaftPublication controlPublication = mock(RaftPublication.class);
    private SessionArchiver leaderArchiver = mock(SessionArchiver.class);
    private Subscription controlSubscription = mock(Subscription.class);
    private ClusterAgent clusterNode = mock(ClusterAgent.class);
    private Archiver archiver = mock(Archiver.class);
    private NodeStateHandler nodeStateHandler = mock(NodeStateHandler.class);

    private final TermState termState = new TermState()
        .allPositions(POSITION)
        .leadershipTerm(OLD_LEADERSHIP_TERM)
        .leaderSessionId(SESSION_ID_4);

    private Follower follower = new Follower(
        ID,
        clusterNode,
        0,
        VOTE_TIMEOUT,
        termState,
        new RaftArchiver(termState.leaderSessionId(), archiver),
        NODE_STATE_BUFFER,
        nodeStateHandler);

    @Before
    public void setUp()
    {
        follower
            .controlPublication(controlPublication)
            .acknowledgementPublication(acknowledgementPublication)
            .controlSubscription(controlSubscription);

        when(archiver.session(SESSION_ID_4)).thenReturn(leaderArchiver);

        follower.follow(0);
    }

    @Test
    public void shouldOnlyVoteForOneCandidateDuringTerm()
    {
        follower.onRequestVote(ID_4, SESSION_ID_4, NEW_LEADERSHIP_TERM, POSITION);

        verify(controlPublication).saveReplyVote(eq(ID), eq(ID_4), anyInt(), eq(FOR), eq(NODE_STATE_BUFFER));

        onHeartbeat();

        follower.onRequestVote(ID_5, SESSION_ID_5, NEW_LEADERSHIP_TERM, POSITION);

        verify(controlPublication, never()).saveReplyVote(eq(ID), eq(ID_5), anyInt(), eq(FOR), eq(NODE_STATE_BUFFER));
    }

    @Test
    public void shouldRecogniseNewLeader()
    {
        termState.noLeader();
        follower.follow(0);
        reset(archiver);

        onHeartbeat();

        verify(archiver).session(SESSION_ID_4);
    }

    @Test
    public void shouldNotifyMissingLogEntries()
    {
        dataToBeCommitted(POSITION + LENGTH);

        poll();

        notifyMissingLogEntries(times(1));
    }

    @Test
    public void shouldAcknowledgeResentLogEntries()
    {
        receivesHeartbeat();

        poll();

        receivesResend();

        poll();

        acknowledgeLogEntries(times(1));
    }

    @Test
    public void shouldResendAcknowledgeLogEntriesIfBackPressured()
    {
        when(leaderArchiver.poll()).thenReturn(100, 0);
        backpressureFirstAcknowledgement();

        poll();

        poll();

        acknowledgeLogEntries(times(2));
    }

    private void backpressureFirstAcknowledgement()
    {
        when(acknowledgementPublication.saveMessageAcknowledgement(anyLong(), anyShort(), any()))
            .thenReturn(BACK_PRESSURED, 100L);
    }

    @Test
    public void shouldOnceSendMissingLogEntriesOnce()
    {
        dataToBeCommitted(POSITION + LENGTH);

        poll();

        poll();

        notifyMissingLogEntries(times(1));
    }

    @Test
    public void shouldResendMissingLogEntriesIfFailed()
    {
        backpressureFirstAcknowledgement();

        dataToBeCommitted(POSITION + LENGTH);

        poll();

        poll();

        notifyMissingLogEntries(times(2));
    }

    @Test
    public void shouldSendSeparateMissingLogEntriesUponChangeOfLeader()
    {
        dataToBeCommitted(POSITION + LENGTH);

        poll();

        onHeartbeat();

        dataToBeCommitted(POSITION + LENGTH);

        poll();

        notifyMissingLogEntries(times(2));
    }

    private void onHeartbeat()
    {
        follower.onConsensusHeartbeat(ID_4, NEW_LEADERSHIP_TERM, POSITION, SESSION_ID_4);
    }

    private void notifyMissingLogEntries(final VerificationMode mode)
    {
        verify(acknowledgementPublication, mode)
            .saveMessageAcknowledgement(POSITION, ID, MISSING_LOG_ENTRIES);
    }

    private void receivesHeartbeat()
    {
        receivesHeartbeat(POSITION + LENGTH);
    }

    private void receivesHeartbeat(final long position)
    {
        whenControlPolled().then(
            (inv) ->
            {
                follower.onConsensusHeartbeat(ID_4, NEW_LEADERSHIP_TERM, position, SESSION_ID_4);

                return 1;
            });
    }

    private void receivesResend()
    {
        receivesResendFrom(POSITION, SESSION_ID_4, NEW_LEADERSHIP_TERM);
    }

    private void acknowledgeLogEntries(final VerificationMode mode)
    {
        verify(acknowledgementPublication, mode)
            .saveMessageAcknowledgement(POSITION + LENGTH, ID, OK);
    }

    private void receivesResendFrom(final long position, final int leaderSessionId, final int leaderShipTerm)
    {
        whenControlPolled().then(
            (inv) ->
            {
                follower.onResend(leaderSessionId, leaderShipTerm, position, buffer, 0, LENGTH);

                return 1;
            });
    }

    private OngoingStubbing<Integer> whenControlPolled()
    {
        return when(controlSubscription.controlledPoll(any(), anyInt()));
    }

    private void dataToBeCommitted(final long position)
    {
        when(leaderArchiver.poll()).thenReturn(LENGTH);

        when(leaderArchiver.archivedPosition()).thenReturn(position);
    }

    private void poll()
    {
        follower.poll(10, 0);
    }
}
