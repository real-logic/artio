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
import uk.co.real_logic.fix_gateway.DebugLogger;
import uk.co.real_logic.fix_gateway.engine.framer.ReliefValve;

/**
 * .
 */
public class RaftNode implements Role
{
    public static final long NOT_LEADER = -3;

    private final short nodeId;
    private final RaftNodeConfiguration configuration;
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

        public void transitionToCandidate(final Candidate candidate, final long timeInMs)
        {
            throw new UnsupportedOperationException();
        }
    }

    private final ClusterRole leaderRole = new ClusterRole()
    {
        public void transitionToFollower(final Leader leader, final long timeInMs)
        {
            DebugLogger.log("%d: Follower @ %d in %d\n", nodeId, timeInMs, termState.leadershipTerm());

            leader.closeStreams();

            injectFollowerStreams();

            currentRole = follower.follow(timeInMs);
        }
    };

    private final ClusterRole followerRole = new ClusterRole()
    {
        public void transitionToCandidate(final Follower follower, final long timeInMs)
        {
            DebugLogger.log("%d: Candidate @ %d in %d\n", nodeId, timeInMs, termState.leadershipTerm());

            follower.closeStreams();

            injectCandidateStreams();

            currentRole = candidate.startNewElection(timeInMs);
        }
    };

    private final ClusterRole candidateRole = new ClusterRole()
    {
        public void transitionToLeader(final Candidate candidate, long timeInMs)
        {
            DebugLogger.log("%d: Leader @ %d in %d\n", nodeId, timeInMs, termState.leadershipTerm());

            candidate.closeStreams();

            injectLeaderStreams();

            currentRole = leader.getsElected(timeInMs);
        }

        public void transitionToFollower(final Candidate candidate, final long timeInMs)
        {
            DebugLogger.log("%d: Follower @ %d in %d\n", nodeId, timeInMs);

            candidate.closeStreams();

            injectFollowerStreams();

            currentRole = follower.follow(timeInMs);
        }

        public void transitionToCandidate(final Candidate candidate, final long timeInMs)
        {
            currentRole = candidate.startNewElection(timeInMs);
        }
    };

    private void injectLeaderStreams()
    {
        leader
            .controlPublication(controlPublication(configuration.controlStream()))
            .acknowledgementSubscription(subscription(configuration.acknowledgementStream()))
            .dataSubscription(subscription(configuration.dataStream()));
    }

    private void injectCandidateStreams()
    {
        final StreamIdentifier control = configuration.controlStream();
        candidate
            .controlPublication(controlPublication(control))
            .controlSubscription(subscription(control));
    }

    private void injectFollowerStreams()
    {
        follower
            .acknowledgementPublication(controlPublication(configuration.acknowledgementStream()))
            .dataSubscription(subscription(configuration.dataStream()))
            .controlSubscription(subscription(configuration.controlStream()));
    }

    public RaftNode(final RaftNodeConfiguration configuration, final long timeInMs)
    {
        this.configuration = configuration;
        this.nodeId = configuration.nodeId();

        final long timeoutIntervalInMs = configuration.timeoutIntervalInMs();
        final long heartbeatTimeInMs = timeoutIntervalInMs / 4;
        final int clusterSize = configuration.otherNodes().size() + 1;

        leader = new Leader(
            nodeId,
            configuration.acknowledgementStrategy(),
            configuration.otherNodes(),
            this,
            configuration.handler(),
            timeInMs,
            heartbeatTimeInMs,
            termState);

        candidate = new Candidate(
            nodeId,
            this,
            clusterSize,
            timeoutIntervalInMs,
            termState);

        follower = new Follower(
            nodeId,
            configuration.handler(),
            this,
            timeInMs,
            timeoutIntervalInMs,
            128 * 1024 * 1024,
            termState);

        currentRole = follower;
    }

    private Publication publication(final StreamIdentifier id)
    {
        return configuration.aeron().addPublication(id.channel(), id.streamId());
    }

    private ControlPublication controlPublication(final StreamIdentifier id)
    {
        return new ControlPublication(
            configuration.maxClaimAttempts(),
            configuration.idleStrategy(),
            configuration.failCounter(),
            ReliefValve.NONE,
            publication(id));
    }

    private Subscription subscription(final StreamIdentifier id)
    {
        return configuration.aeron().addSubscription(id.channel(), id.streamId());
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

    public void transitionToCandidate(final long timeInMs)
    {
        followerRole.transitionToCandidate(follower, timeInMs);
    }

    public int poll(final int fragmentLimit, final long timeInMs)
    {
        return currentRole.poll(fragmentLimit, timeInMs);
    }

    public void closeStreams()
    {
        currentRole.closeStreams();
    }

    public long offer(final DirectBuffer buffer, final int offset, final int length)
    {
        if (!roleIsLeader())
        {
            return NOT_LEADER;
        }

        //return dataPublication.offer(buffer, offset, length);
        return 0;
    }

    public boolean roleIsLeader()
    {
        return currentRole == leader;
    }

    public boolean roleIsCandidate()
    {
        return currentRole == candidate;
    }

    public boolean roleIsFollower()
    {
        return currentRole == follower;
    }

    public short nodeId()
    {
        return nodeId;
    }
}
