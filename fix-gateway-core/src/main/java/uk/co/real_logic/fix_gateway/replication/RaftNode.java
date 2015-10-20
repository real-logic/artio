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

import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.collections.IntHashSet;
import uk.co.real_logic.fix_gateway.DebugLogger;

/**
 * .
 */
public class RaftNode implements Role
{
    public static final long NOT_LEADER = -3;

    private final short nodeId;
    private final Publication dataPublication;
    private Role currentRole;

    private final TermState termState = new TermState();
    private final Leader leader;
    private final Candidate candidate;
    private final Follower follower;

    private abstract class ClusterRole
    {
        public void transitionToLeader(final Candidate candidate, final long timeInMs)
        {
            throw new UnsupportedOperationException();
        }

        public void transitionToCandidate(final Follower follower, final long timeInMs)
        {
            throw new UnsupportedOperationException();
        }

        public void transitionToFollower(final Candidate candidate, final long timeInMs)
        {
            throw new UnsupportedOperationException();
        }

        public void transitionToFollower(final Leader leader, final long timeInMs)
        {
            throw new UnsupportedOperationException();
        }

        public void transitionToCandidate(final Candidate candidate, final TermState termState)
        {
            throw new UnsupportedOperationException();
        }
    }

    private final ClusterRole leaderRole = new ClusterRole()
    {
        public void transitionToFollower(final Leader leader, final TermState termState, final long timeInMs)
        {
            DebugLogger.log("%d: Follower @ %d in %d\n", nodeId, timeInMs, termState.leadershipTerm());

            currentRole = follower.follow(timeInMs, termState.leadershipTerm(), termState.position());
        }
    };

    private final ClusterRole followerRole = new ClusterRole()
    {
        public void transitionToCandidate(final Follower follower, final long timeInMs)
        {
            currentRole = candidate;

            candidate.startNewElection(timeInMs);
        }
    };

    private final ClusterRole candidateRole = new ClusterRole()
    {
        public void transitionToLeader(final Candidate candidate, long timeInMs)
        {
            DebugLogger.log("%d: Leader @ %d in %d\n", nodeId, timeInMs, termState.leadershipTerm());

            currentRole = leader.getsElected(timeInMs);
        }

        public void transitionToFollower(final Candidate candidate, final long timeInMs)
        {
            DebugLogger.log("%d: Follower @ %d in %d\n", nodeId, timeInMs);

            currentRole = follower.follow(timeInMs, termState.leadershipTerm(), termState.position());
        }

        public void transitionToCandidate(final Candidate candidate, TermState termState)
        {

        }
    };

    public RaftNode(
        final short nodeId,
        final ControlPublication controlPublication,
        final Publication dataPublication,
        final Subscription controlSubscription,
        final Subscription dataSubscription,
        final IntHashSet otherNodes,
        final long timeInMs,
        final long timeoutIntervalInMs,
        final LeadershipTermAcknowledgementStrategy leadershipTermAcknowledgementStrategy,
        final ReplicationHandler handler)
    {
        this.nodeId = nodeId;
        this.dataPublication = dataPublication;

        final long heartbeatTimeInMs = timeoutIntervalInMs / 2;

        leader = new Leader(
            nodeId,
            leadershipTermAcknowledgementStrategy,
            otherNodes,
            controlPublication,
            controlSubscription,
            dataSubscription,
            this,
            handler,
            timeInMs,
            heartbeatTimeInMs,
            termState);

        candidate = new Candidate(
            nodeId,
            controlPublication,
            controlSubscription,
            this,
            otherNodes.size() + 1,
            timeoutIntervalInMs,
            termState);

        follower = new Follower(
            nodeId,
            controlPublication,
            handler,
            dataSubscription,
            controlSubscription,
            this,
            timeInMs,
            timeoutIntervalInMs,
            128 * 1024 * 1024, // TODO: make configurable
            termState);

        currentRole = follower;
    }

    public void transitionToFollower(final Candidate candidate, final long timeInMs)
    {
        candidateRole.transitionToFollower(candidate, timeInMs);
    }

    public void transitionToFollower(final Leader leader, final long timeInMs)
    {
        leaderRole.transitionToFollower(leader, timeInMs);
    }

    public void transitionToLeader(final long timeInMs)
    {
        candidateRole.transitionToLeader(candidate, timeInMs);
    }

    public void becomeCandidate(final long timeInMs, final int oldTerm, final long position)
    {
        followerRole.transitionToCandidate(follower, timeInMs);
    }

    public int poll(final int fragmentLimit, final long timeInMs)
    {
        return currentRole.poll(fragmentLimit, timeInMs);
    }

    public long offer(final DirectBuffer buffer, final int offset, final int length)
    {
        if (!isLeader())
        {
            return NOT_LEADER;
        }

        return dataPublication.offer(buffer, offset, length);
    }

    public boolean isLeader()
    {
        return currentRole == leader;
    }

    public boolean isCandidate()
    {
        return currentRole == candidate;
    }

    public boolean isFollower()
    {
        return currentRole == follower;
    }

    public short nodeId()
    {
        return nodeId;
    }
}
