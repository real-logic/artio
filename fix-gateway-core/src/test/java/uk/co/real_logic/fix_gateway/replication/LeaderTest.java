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
import io.aeron.logbuffer.BlockHandler;
import org.agrona.DirectBuffer;
import org.agrona.collections.IntHashSet;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.OngoingStubbing;
import org.mockito.verification.VerificationMode;
import uk.co.real_logic.fix_gateway.engine.logger.ArchiveReader;
import uk.co.real_logic.fix_gateway.engine.logger.Archiver;
import uk.co.real_logic.fix_gateway.engine.logger.Archiver.SessionArchiver;

import static io.aeron.Publication.BACK_PRESSURED;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.fix_gateway.replication.messages.AcknowledgementStatus.MISSING_LOG_ENTRIES;

public class LeaderTest
{
    private static final short ID = 2;
    private static final int LEADERSHIP_TERM = 1;
    private static final int LEADER_SESSION_ID = 42;
    private static final long TIME = 10L;
    private static final long POSITION = 40L;
    private static final int HEARTBEAT_INTERVAL_IN_MS = 10;
    private static final short FOLLOWER_ID = 4;
    private static final short OTHER_FOLLOWER_ID = 5;
    private static final DirectBuffer NODE_STATE_BUFFER = new UnsafeBuffer(new byte[1]);

    private RaftPublication controlPublication = mock(RaftPublication.class);
    private ClusterAgent clusterNode = mock(ClusterAgent.class);
    private Subscription acknowledgementSubscription = mock(Subscription.class);
    private Subscription dataSubscription = mock(Subscription.class);
    private Subscription controlSubscription = mock(Subscription.class);
    private ArchiveReader archiveReader = mock(ArchiveReader.class);
    private ArchiveReader.SessionReader sessionReader = mock(ArchiveReader.SessionReader.class);
    private Archiver archiver = mock(Archiver.class);
    private SessionArchiver sessionArchiver = mock(SessionArchiver.class);
    private TermState termState = new TermState()
        .leadershipTerm(LEADERSHIP_TERM)
        .consensusPosition(POSITION);
    private NodeStateHandler nodeStateHandler = mock(NodeStateHandler.class);

    private Leader leader = new Leader(
        ID,
        new EntireClusterAcknowledgementStrategy(),
        new IntHashSet(40, -1),
        clusterNode,
        0,
        HEARTBEAT_INTERVAL_IN_MS,
        termState,
        LEADER_SESSION_ID,
        archiveReader,
        new RaftArchiver(termState.leaderSessionId(), archiver),
        NODE_STATE_BUFFER,
        nodeStateHandler);

    @Before
    public void setUp()
    {
        when(archiver.session(LEADER_SESSION_ID)).thenReturn(sessionArchiver);
        when(archiveReader.session(LEADER_SESSION_ID)).thenReturn(sessionReader);
        termState.leaderSessionId(LEADER_SESSION_ID);

        leader
            .controlPublication(controlPublication)
            .acknowledgementSubscription(acknowledgementSubscription)
            .dataSubscription(dataSubscription)
            .controlSubscription(controlSubscription)
            .getsElected(TIME, POSITION);

        whenBlockRead().then(inv ->
        {
            final Object[] arguments = inv.getArguments();
            final int length = (int) arguments[1];
            final BlockHandler handler = (BlockHandler) arguments[2];

            if (handler != null)
            {
                handler.onBlock(new UnsafeBuffer(new byte[length]), 0, length, LEADER_SESSION_ID, 1);
            }

            return true;
        });
    }

    @Test
    public void onElectionHeartbeat()
    {
        verify(controlPublication)
            .saveConsensusHeartbeat(ID, LEADERSHIP_TERM, POSITION, LEADER_SESSION_ID, 0, POSITION);
    }

    @Test
    public void shouldResendDataInResponseToMissingLogEntries()
    {
        when(sessionArchiver.archivedPosition()).thenReturn(POSITION);

        leader.readData();

        final long followerPosition = 0;

        receivesMissingLogEntries(followerPosition);

        resendsMissingLogEntries(followerPosition, (int) POSITION, times(1));
    }

    @Test
    public void shouldResendDataInResponseToMissingLogEntriesWhenBackPressured()
    {
        final long followerPosition = 0;

        when(sessionArchiver.archivedPosition()).thenReturn(POSITION);
        backpressureResend(followerPosition);

        leader.readData();

        receivesMissingLogEntries(followerPosition);

        leader.poll(1, 0);

        leader.poll(1, 0);

        resendsMissingLogEntries(followerPosition, (int) POSITION, times(2));
    }

    @Test
    public void shouldSupportMultipleSimultaneousResendsWhenBackPressured()
    {
        final long followerPosition = 0;
        final long otherFollowerPosition = 20;

        when(sessionArchiver.archivedPosition()).thenReturn(POSITION);

        backpressureResend(otherFollowerPosition);
        backpressureResend(followerPosition);

        leader.readData();

        receivesMissingLogEntries(followerPosition);
        receivesMissingLogEntries(otherFollowerPosition, OTHER_FOLLOWER_ID);

        leader.poll(1, 0);

        leader.poll(1, 0);

        resendsMissingLogEntries(otherFollowerPosition, (int) POSITION, times(2));
        resendsMissingLogEntries(followerPosition, (int) POSITION, times(2));
    }

    private void backpressureResend(final long position)
    {
        when(controlPublication.saveResend(anyInt(), anyInt(), eq(position), anyLong(), any(), anyInt(), anyInt()))
            .thenReturn(BACK_PRESSURED, 100L);
    }

    @Test
    public void shouldRespondToMissingLogEntriesWhenTheresNoData()
    {
        whenBlockRead().thenReturn(false);

        final long followerPosition = 0;

        receivesMissingLogEntries(followerPosition);

        resendsMissingLogEntries(followerPosition, 0, times(1));
    }

    private void receivesMissingLogEntries(final long followerPosition)
    {
        receivesMissingLogEntries(followerPosition, FOLLOWER_ID);
    }

    private void receivesMissingLogEntries(final long followerPosition, final short followerId)
    {
        leader.onMessageAcknowledgement(followerPosition, followerId, MISSING_LOG_ENTRIES);
    }

    private void resendsMissingLogEntries(final long followerPosition, final int length, final VerificationMode mode)
    {
        verify(controlPublication, mode).saveResend(
            eq(LEADER_SESSION_ID),
            eq(LEADERSHIP_TERM),
            eq(followerPosition),
            anyLong(),
            any(),
            eq(0),
            eq((int) (length - followerPosition)));
    }

    private OngoingStubbing<Boolean> whenBlockRead()
    {
        return when(sessionReader.readBlock(anyLong(), anyInt(), any()));
    }
}
