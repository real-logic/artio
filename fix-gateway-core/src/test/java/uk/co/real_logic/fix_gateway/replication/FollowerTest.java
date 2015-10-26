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
import uk.co.real_logic.aeron.Image;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.agrona.concurrent.AtomicBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.fix_gateway.messages.AcknowledgementStatus.MISSING_LOG_ENTRIES;
import static uk.co.real_logic.fix_gateway.messages.Vote.FOR;

public class FollowerTest
{
    private static final long POSITION = 40;
    private static final int OFFSET = (int) POSITION;
    private static final int LENGTH = 100;
    private static final long VOTE_TIMEOUT = 100;
    private static final int OLD_LEADERSHIP_TERM = 1;
    private static final int NEW_LEADERSHIP_TERM = OLD_LEADERSHIP_TERM + 1;
    private static final int LEADER_SESSION_ID = 42;
    private static final int OTHER_SESSION_ID = LEADER_SESSION_ID + 1;

    private static final short ID = 3;
    private static final short ID_4 = 4;
    private static final short ID_5 = 5;

    private AtomicBuffer buffer = new UnsafeBuffer(new byte[8 * 1024]);
    private RaftPublication acknowledgementPublication = mock(RaftPublication.class);
    private RaftPublication controlPublication = mock(RaftPublication.class);
    private ReplicationHandler handler = mock(ReplicationHandler.class);
    private Subscription dataSubscription = mock(Subscription.class);
    private Image leaderDataImage = mock(Image.class);
    private Subscription controlSubscription = mock(Subscription.class);
    private RaftNode raftNode = mock(RaftNode.class);

    private final TermState termState = new TermState()
        .allPositions(POSITION)
        .leadershipTerm(OLD_LEADERSHIP_TERM)
        .leaderSessionId(LEADER_SESSION_ID);

    private Follower follower = new Follower(
        ID,
        handler,
        raftNode,
        0,
        VOTE_TIMEOUT,
        8 * 1024 * 1024,
        termState);

    @Before
    public void setUp()
    {
        follower
            .controlPublication(controlPublication)
            .acknowledgementPublication(acknowledgementPublication)
            .dataSubscription(dataSubscription)
            .controlSubscription(controlSubscription);

        when(dataSubscription.getImage(LEADER_SESSION_ID)).thenReturn(leaderDataImage);

        follower.follow(0);
    }

    @Test
    public void shouldOnlyVoteForOneCandidateDuringTerm()
    {
        follower.onRequestVote(ID_4, NEW_LEADERSHIP_TERM, POSITION);

        verify(controlPublication).saveReplyVote(eq(ID), eq(ID_4), anyInt(), eq(FOR));

        follower.onConcensusHeartbeat(ID_4, NEW_LEADERSHIP_TERM, POSITION, LEADER_SESSION_ID);

        follower.onRequestVote(ID_5, NEW_LEADERSHIP_TERM, POSITION);

        verify(controlPublication, never()).saveReplyVote(eq(ID), eq(ID_5), anyInt(), eq(FOR));
    }

    @Test
    public void shouldCommitDataWithAck()
    {
        dataToBeCommitted();

        receivesHeartbeat();

        poll();

        dataCommitted();
    }

    @Test
    public void shouldNotCommitDataWithoutAck()
    {
        dataToBeCommitted();

        poll();

        noDataCommitted();
    }

    @Test
    public void shouldNotCommitDataWithoutData()
    {
        receivesHeartbeat();

        poll();

        noDataCommitted();
    }

    @Test
    public void shouldCommitDataReceivedAfterAck()
    {
        receivesHeartbeat();

        dataToBeCommitted();

        poll();

        dataCommitted();
    }

    @Test
    public void shouldNotifyMissingLogEntries()
    {
        dataToBeCommitted(POSITION + LENGTH);

        poll();

        notifyMissingLogEntries();
    }

    // TODO: commits resend
    // TODO: connect, resend, update ensure backfilled

    private void notifyMissingLogEntries()
    {
        verify(acknowledgementPublication)
            .saveMessageAcknowledgement(POSITION, ID, MISSING_LOG_ENTRIES);
    }

    private void receivesHeartbeat()
    {
        when(controlSubscription.poll(any(), anyInt())).then(inv ->
        {
            follower.onConcensusHeartbeat(ID_4, NEW_LEADERSHIP_TERM, POSITION + LENGTH, LEADER_SESSION_ID);

            return 1;
        });
    }

    private void dataCommitted()
    {
        verify(handler).onBlock(any(), eq(0), eq(LENGTH));
    }

    private void dataToBeCommitted()
    {
        dataToBeCommitted(POSITION);
    }

    private void dataToBeCommitted(final long position)
    {
        when(leaderDataImage.blockPoll(eq(follower), anyInt())).then(inv ->
        {
            follower.onBlock(buffer, (int) position, LENGTH, LEADER_SESSION_ID, 0);

            return LENGTH;
        });

        when(leaderDataImage.position()).thenReturn(position);
    }

    private void noDataCommitted()
    {
        verify(handler, never()).onBlock(any(), anyInt(), anyInt());
    }

    private void poll()
    {
        follower.poll(10, 0);
    }

}
