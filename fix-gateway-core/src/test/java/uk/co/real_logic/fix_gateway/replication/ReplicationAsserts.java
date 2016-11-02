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

import org.hamcrest.Matcher;

import static org.hamcrest.Matchers.allOf;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.fix_gateway.util.CustomMatchers.hasFluentProperty;

public final class ReplicationAsserts
{
    public static void transitionsToCandidate(final ClusterAgent clusterNode)
    {
        verify(clusterNode).transitionToCandidate(anyLong());
    }

    public static void neverTransitionsToCandidate(final ClusterAgent clusterNode)
    {
        verify(clusterNode, never()).transitionToCandidate(anyLong());
    }

    public static void neverTransitionsToFollower(final ClusterAgent clusterNode)
    {
        verify(clusterNode, never()).transitionToFollower(any(Leader.class), anyShort(), anyLong());
    }

    public static void transitionsToLeader(final ClusterAgent clusterNode)
    {
        verify(clusterNode).transitionToLeader(anyLong());
    }

    public static void neverTransitionsToLeader(final ClusterAgent clusterNode)
    {
        verify(clusterNode, never()).transitionToLeader(anyLong());
    }

    public static void staysFollower(final ClusterAgent clusterNode)
    {
        neverTransitionsToCandidate(clusterNode);
        neverTransitionsToLeader(clusterNode);
    }

    public static void staysLeader(final ClusterAgent clusterNode)
    {
        neverTransitionsToCandidate(clusterNode);
        neverTransitionsToFollower(clusterNode);
    }

    public static Matcher<TermState> hasLeaderSessionId(final int leaderSessionId)
    {
        return hasFluentProperty("leaderSessionId",
            hasFluentProperty("get", leaderSessionId));
    }

    public static Matcher<TermState> noLeaderMatcher()
    {
        return hasFluentProperty("hasLeader", false);
    }

    public static Matcher<TermState> hasLeadershipTerm(final int leadershipTerm)
    {
        return hasFluentProperty("leadershipTerm", leadershipTerm);
    }

    public static Matcher<TermState> hasConsensusPosition(final long commitPosition)
    {
        return hasFluentProperty("consensusPosition",
            hasFluentProperty("get", commitPosition));
    }

    public static Matcher<TermState> hasLastAppliedPosition(final long lastAppliedPosition)
    {
        return hasFluentProperty("lastAppliedPosition", lastAppliedPosition);
    }

    public static Matcher<TermState> hasPosition(final long position)
    {
        return allOf(hasConsensusPosition(position), hasLastAppliedPosition(position));
    }
}
