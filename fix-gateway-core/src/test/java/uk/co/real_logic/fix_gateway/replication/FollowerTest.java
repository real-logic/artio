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
import org.mockito.stubbing.OngoingStubbing;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.logbuffer.FragmentHandler;
import uk.co.real_logic.aeron.logbuffer.Header;
import uk.co.real_logic.agrona.concurrent.AtomicBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.engine.logger.ArchiveReader;
import uk.co.real_logic.fix_gateway.engine.logger.Archiver;
import uk.co.real_logic.fix_gateway.engine.logger.Archiver.SessionArchiver;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
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
    private static final int LEADER_SESSION_ID = 42;
    private static final int OTHER_SESSION_ID = LEADER_SESSION_ID + 1;

    private static final short ID = 3;
    private static final short ID_4 = 4;
    private static final short ID_5 = 5;

    private AtomicBuffer buffer = new UnsafeBuffer(new byte[8 * 1024]);
    private RaftPublication acknowledgementPublication = mock(RaftPublication.class);
    private RaftPublication controlPublication = mock(RaftPublication.class);
    private FragmentHandler handler = mock(FragmentHandler.class);
    private SessionArchiver leaderArchiver = mock(SessionArchiver.class);
    private Subscription controlSubscription = mock(Subscription.class);
    private RaftNode raftNode = mock(RaftNode.class);
    private ArchiveReader archiveReader = mock(ArchiveReader.class);
    private ArchiveReader.SessionReader sessionReader = mock(ArchiveReader.SessionReader.class);
    private Archiver archiver = mock(Archiver.class);

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
        termState,
        archiveReader,
        archiver);

    @Before
    public void setUp()
    {
        follower
            .controlPublication(controlPublication)
            .acknowledgementPublication(acknowledgementPublication)
            .controlSubscription(controlSubscription);

        when(archiveReader.session(LEADER_SESSION_ID)).thenReturn(sessionReader);
        when(archiver.session(LEADER_SESSION_ID)).thenReturn(leaderArchiver);

        follower.follow(0);
    }

    @Test
    public void shouldOnlyVoteForOneCandidateDuringTerm()
    {
        follower.onRequestVote(ID_4, NEW_LEADERSHIP_TERM, POSITION);

        verify(controlPublication).saveReplyVote(eq(ID), eq(ID_4), anyInt(), eq(FOR));

        onHeartbeat();

        follower.onRequestVote(ID_5, NEW_LEADERSHIP_TERM, POSITION);

        verify(controlPublication, never()).saveReplyVote(eq(ID), eq(ID_5), anyInt(), eq(FOR));
    }

    @Test
    public void shouldRecogniseNewLeader()
    {
        termState.noLeader();
        follower.follow(0);
        reset(archiver);

        onHeartbeat();

        verify(archiver).session(LEADER_SESSION_ID);
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

    @Test
    public void shouldCommitResentLogEntries()
    {
        receivesHeartbeat();

        poll();

        receivesResend();

        poll();

        dataCommitted();
    }

    @Test
    public void shouldAcknowledgeResentLogEntries()
    {
        receivesHeartbeat();

        poll();

        receivesResend();

        poll();

        acknowledgeLogEntries();
    }

    @Test
    public void shouldNotCommitResentLogEntriesWithGap()
    {
        receivesHeartbeat();

        poll();

        receivesResendFrom(POSITION + LENGTH, LEADER_SESSION_ID, NEW_LEADERSHIP_TERM);

        poll();

        noDataCommitted();
    }

    @Test
    public void shouldNotCommitResentLogEntriesFromWrongLeader()
    {
        receivesHeartbeat();

        poll();

        receivesResendFrom(POSITION, OTHER_SESSION_ID, NEW_LEADERSHIP_TERM);

        poll();

        noDataCommitted();
    }

    @Test
    public void shouldNotCommitResentLogEntriesFromOldTerm()
    {
        receivesHeartbeat();

        poll();

        receivesResendFrom(POSITION, LEADER_SESSION_ID, OLD_LEADERSHIP_TERM);

        poll();

        noDataCommitted();
    }

    @Test
    public void shouldCommitMoreDataAfterResend()
    {
        shouldCommitResentLogEntries();

        final long endOfResendPosition = POSITION + LENGTH;

        dataToBeCommitted(endOfResendPosition);

        receivesHeartbeat(endOfResendPosition + LENGTH);

        poll();

        dataCommitted();
    }

    private void onHeartbeat()
    {
        follower.onConcensusHeartbeat(ID_4, NEW_LEADERSHIP_TERM, POSITION, LEADER_SESSION_ID);
    }

    private void notifyMissingLogEntries()
    {
        verify(acknowledgementPublication)
            .saveMessageAcknowledgement(POSITION, ID, MISSING_LOG_ENTRIES);
    }

    private void receivesHeartbeat()
    {
        receivesHeartbeat(POSITION + LENGTH);
    }

    private void receivesHeartbeat(final long position)
    {
        whenControlPolled().then(inv ->
        {
            follower.onConcensusHeartbeat(ID_4, NEW_LEADERSHIP_TERM, position, LEADER_SESSION_ID);

            return 1;
        });
    }

    private void receivesResend()
    {
        receivesResendFrom(POSITION, LEADER_SESSION_ID, NEW_LEADERSHIP_TERM);
    }

    private void acknowledgeLogEntries()
    {
        verify(acknowledgementPublication)
            .saveMessageAcknowledgement(POSITION + LENGTH, ID, OK);
    }

    private void receivesResendFrom(final long position, final int leaderSessionId, final int leaderShipTerm)
    {
        whenControlPolled().then(inv ->
        {
            follower.onResend(leaderSessionId, leaderShipTerm, position, buffer, 0, LENGTH);

            return 1;
        });

        dataInArchive(position);
    }

    private OngoingStubbing<Integer> whenControlPolled()
    {
        return when(controlSubscription.poll(any(), anyInt()));
    }

    private void dataCommitted()
    {
        verify(handler, atLeastOnce()).onFragment(any(), eq(0), eq(LENGTH), any());
    }

    private void dataToBeCommitted()
    {
        dataToBeCommitted(POSITION);
    }

    private void dataToBeCommitted(final long position)
    {
        when(leaderArchiver.poll()).thenReturn(LENGTH);

        when(leaderArchiver.position()).thenReturn(position);

        dataInArchive(position);
    }

    private void dataInArchive(final long position)
    {
        when(sessionReader.readUpTo(
            eq(position + HEADER_LENGTH), eq((long) LENGTH), any())).then(inv ->
        {
            final Object[] arguments = inv.getArguments();
            final FragmentHandler handler = (FragmentHandler) arguments[2];
            handler.onFragment(buffer, 0, LENGTH, mock(Header.class));

            return LENGTH + HEADER_LENGTH;
        });
    }

    private void noDataCommitted()
    {
        verify(handler, never()).onFragment(any(), anyInt(), anyInt(), any());
    }

    private void poll()
    {
        follower.poll(10, 0);
    }

}
