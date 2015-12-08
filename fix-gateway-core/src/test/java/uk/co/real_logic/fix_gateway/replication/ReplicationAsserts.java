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

import org.hamcrest.Matcher;
import org.hamcrest.Matchers;

import static org.hamcrest.Matchers.allOf;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.fix_gateway.util.CustomMatchers.hasFluentProperty;

public final class ReplicationAsserts
{
    public static void transitionsToCandidate(final RaftNode raftNode)
    {
        verify(raftNode).transitionToCandidate(anyLong());
    }

    public static void neverTransitionsToCandidate(final RaftNode raftNode)
    {
        verify(raftNode, never()).transitionToCandidate(anyLong());
    }

    public static void transitionsToFollower(final RaftNode raftNode)
    {
        verify(raftNode, atLeastOnce()).transitionToFollower(any(Candidate.class), anyLong());
    }

    public static void neverTransitionsToFollower(final RaftNode raftNode)
    {
        verify(raftNode, never()).transitionToFollower(any(Leader.class), anyShort(), anyLong());
    }

    public static void transitionsToLeader(final RaftNode raftNode)
    {
        verify(raftNode).transitionToLeader(anyLong());
    }

    public static void neverTransitionsToLeader(final RaftNode raftNode)
    {
        verify(raftNode, never()).transitionToLeader(anyLong());
    }

    public static void staysFollower(final RaftNode raftNode)
    {
        neverTransitionsToCandidate(raftNode);
        neverTransitionsToLeader(raftNode);
    }

    public static void staysLeader(final RaftNode raftNode)
    {
        neverTransitionsToCandidate(raftNode);
        neverTransitionsToFollower(raftNode);
    }

    public static Matcher<TermState> hasLeaderSessionId(final int leaderSessionId)
    {
        return Matchers.allOf(
            hasFluentProperty("hasLeader", true),
            hasFluentProperty("leaderSessionId", leaderSessionId));
    }

    public static Matcher<TermState> noLeaderMatcher()
    {
        return hasFluentProperty("hasLeader", false);
    }

    public static Matcher<TermState> hasLeadershipTerm(final int leadershipTerm)
    {
        return hasFluentProperty("leadershipTerm", leadershipTerm);
    }

    public static Matcher<TermState> hasCommitPosition(final long commitPosition)
    {
        return hasFluentProperty("commitPosition", commitPosition);
    }

    public static Matcher<TermState> hasLastAppliedPosition(final long lastAppliedPosition)
    {
        return hasFluentProperty("lastAppliedPosition", lastAppliedPosition);
    }

    public static Matcher<TermState> hasPositions(final long position)
    {
        return allOf(hasCommitPosition(position), hasLastAppliedPosition(position));
    }

}
