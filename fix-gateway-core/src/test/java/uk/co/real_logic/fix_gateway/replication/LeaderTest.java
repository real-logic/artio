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
import io.aeron.logbuffer.BlockHandler;
import io.aeron.logbuffer.ControlledFragmentHandler;
import org.agrona.collections.IntHashSet;
import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.OngoingStubbing;
import uk.co.real_logic.fix_gateway.engine.logger.ArchiveReader;
import uk.co.real_logic.fix_gateway.engine.logger.Archiver;
import uk.co.real_logic.fix_gateway.engine.logger.Archiver.SessionArchiver;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.fix_gateway.messages.AcknowledgementStatus.MISSING_LOG_ENTRIES;

public class LeaderTest
{
    private static final short ID = 2;
    private static final int LEADERSHIP_TERM = 1;
    private static final int LEADER_SESSION_ID = 42;
    private static final long TIME = 10L;
    private static final long POSITION = 40L;
    private static final int HEARTBEAT_INTERVAL_IN_MS = 10;
    private static final short FOLLOWER_ID = 4;

    private RaftPublication controlPublication = mock(RaftPublication.class);
    private ClusterNode clusterNode = mock(ClusterNode.class);
    private Subscription acknowledgementSubscription = mock(Subscription.class);
    private Subscription dataSubscription = mock(Subscription.class);
    private Image leaderDataImage = mock(Image.class);
    private ArchiveReader archiveReader = mock(ArchiveReader.class);
    private ArchiveReader.SessionReader sessionReader = mock(ArchiveReader.SessionReader.class);
    private Archiver archiver = mock(Archiver.class);
    private SessionArchiver sessionArchiver = mock(SessionArchiver.class);
    private TermState termState = new TermState()
        .leadershipTerm(LEADERSHIP_TERM)
        .commitPosition(POSITION);

    private Leader leader = new Leader(
        ID,
        new EntireClusterAcknowledgementStrategy(),
        new IntHashSet(40, -1),
        clusterNode,
        mock(ControlledFragmentHandler.class),
        0,
        HEARTBEAT_INTERVAL_IN_MS,
        termState,
        LEADER_SESSION_ID,
        archiveReader,
        archiver);

    @Before
    public void setUp()
    {
        when(dataSubscription.getImage(LEADER_SESSION_ID)).thenReturn(leaderDataImage);
        when(archiver.session(LEADER_SESSION_ID)).thenReturn(sessionArchiver);
        when(archiveReader.session(LEADER_SESSION_ID)).thenReturn(sessionReader);

        leader
            .controlPublication(controlPublication)
            .acknowledgementSubscription(acknowledgementSubscription)
            .dataSubscription(dataSubscription)
            .getsElected(TIME);

        whenBlockRead().then(inv ->
        {
            final Object[] arguments = inv.getArguments();
            final int length = (int) arguments[1];
            final BlockHandler handler = (BlockHandler) arguments[2];

            if (handler != null)
            {
                handler.onBlock(null, 0, length, LEADER_SESSION_ID, 1);
            }

            return true;
        });
    }

    @Test
    public void onElectionHeartbeat()
    {
        verify(controlPublication).saveConcensusHeartbeat(ID, LEADERSHIP_TERM, POSITION, LEADER_SESSION_ID);
    }

    @Test
    public void shouldResendDataInResponseToMissingLogEntries()
    {
        when(leaderDataImage.filePoll(any(), anyInt())).thenReturn((int) POSITION);
        leader.readData();

        final long followerPosition = 0;

        receivesMissingLogEntries(followerPosition);

        resendsMissingLogEntries(followerPosition, (int) POSITION);
    }

    @Test
    public void shouldRespondToMissingLogEntriesWhenTheresNoData()
    {
        whenBlockRead().thenReturn(false);

        final long followerPosition = 0;

        receivesMissingLogEntries(followerPosition);

        resendsMissingLogEntries(followerPosition, 0);
    }

    private void receivesMissingLogEntries(final long followerPosition)
    {
        leader.onMessageAcknowledgement(followerPosition, FOLLOWER_ID, MISSING_LOG_ENTRIES);
    }

    private void resendsMissingLogEntries(final long followerPosition, final int length)
    {
        verify(controlPublication).saveResend(
            eq(LEADER_SESSION_ID),
            eq(LEADERSHIP_TERM),
            eq(followerPosition),
            any(),
            eq(0),
            eq(length));
    }

    private OngoingStubbing<Boolean> whenBlockRead()
    {
        return when(sessionReader.readBlock(anyLong(), anyInt(), any()));
    }
}
