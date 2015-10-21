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

import org.junit.Test;
import uk.co.real_logic.aeron.Subscription;

import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static uk.co.real_logic.fix_gateway.messages.Vote.FOR;

public class FollowerTest
{
    private static final long POSITION = 40;
    private static final long VOTE_TIMEOUT = 100;
    private static final int OLD_LEADERSHIP_TERM = 1;
    private static final int NEW_LEADERSHIP_TERM = OLD_LEADERSHIP_TERM + 1;

    private static final short ID = 3;
    private static final short ID_4 = 4;
    private static final short ID_5 = 5;

    private ControlPublication controlPublication = mock(ControlPublication.class);
    private ReplicationHandler handler = mock(ReplicationHandler.class);
    private Subscription dataSubscription = mock(Subscription.class);
    private Subscription controlSubscription = mock(Subscription.class);
    private RaftNode raftNode = mock(RaftNode.class);

    private Follower follower = new Follower(
        ID,
        handler,
        raftNode,
        0,
        VOTE_TIMEOUT,
        8 * 1024 * 1024,
        new TermState());

    @Test
    public void shouldOnlyVoteForOneCandidateDuringTerm()
    {
        follower.onRequestVote(ID_4, NEW_LEADERSHIP_TERM, POSITION);

        verify(controlPublication).saveReplyVote(eq(ID), eq(ID_4), anyInt(), eq(FOR));

        follower.onConcensusHeartbeat(ID_4, NEW_LEADERSHIP_TERM, POSITION);

        follower.onRequestVote(ID_5, NEW_LEADERSHIP_TERM, POSITION);

        verify(controlPublication, never()).saveReplyVote(eq(ID), eq(ID_5), anyInt(), eq(FOR));
    }

}
