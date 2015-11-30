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
import uk.co.real_logic.fix_gateway.DebugLogger;
import uk.co.real_logic.fix_gateway.engine.framer.ReliefValve;

/**
 * .
 */
public class RaftNode implements Role
{
    public static final int HEARTBEAT_TO_TIMEOUT_RATIO = 4;
    private final short nodeId;
    private final RaftNodeConfiguration configuration;
    private Role currentRole;

    private final TermState termState = new TermState();
    private final Leader leader;
    private final Candidate candidate;
    private final Follower follower;

    private abstract class NodeState
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

    private final NodeState leaderState = new NodeState()
    {
        public void transitionToFollower(final Leader leader, final long timeInMs)
        {
            DebugLogger.log("%d: L -> Follower @ %d in %d\n", nodeId, timeInMs, termState.leadershipTerm());

            leader.closeStreams();

            injectFollowerStreams();

            currentRole = follower.follow(timeInMs);
        }
    };

    private final NodeState followerState = new NodeState()
    {
        public void transitionToCandidate(final Follower follower, final long timeInMs)
        {
            DebugLogger.log("%d: F -> Candidate @ %d in %d\n", nodeId, timeInMs, termState.leadershipTerm());

            follower.closeStreams();

            injectCandidateStreams();

            currentRole = candidate.startNewElection(timeInMs);
        }
    };

    private final NodeState candidateState = new NodeState()
    {
        public void transitionToLeader(final Candidate candidate, long timeInMs)
        {
            DebugLogger.log("%d: C -> Leader @ %d in %d\n", nodeId, timeInMs, termState.leadershipTerm());

            candidate.closeStreams();

            injectLeaderStreams();

            currentRole = leader.getsElected(timeInMs);
        }

        public void transitionToFollower(final Candidate candidate, final long timeInMs)
        {
            DebugLogger.log("%d: C -> Follower @ %d\n", nodeId, timeInMs);

            candidate.closeStreams();

            injectFollowerStreams();

            currentRole = follower.follow(timeInMs);
        }
    };

    private void injectLeaderStreams()
    {
        leader
            .controlPublication(raftPublication(configuration.controlStream()))
            .acknowledgementSubscription(subscription(configuration.acknowledgementStream()))
            .dataSubscription(subscription(configuration.dataStream()));
    }

    private void injectCandidateStreams()
    {
        final StreamIdentifier control = configuration.controlStream();
        candidate
            .controlPublication(raftPublication(control))
            .controlSubscription(subscription(control));
    }

    private void injectFollowerStreams()
    {
        configuration.archiver()
                     .subscription(subscription(configuration.dataStream()));

        final StreamIdentifier controlStream = configuration.controlStream();
        follower
            .acknowledgementPublication(raftPublication(configuration.acknowledgementStream()))
            .controlPublication(raftPublication(controlStream))
            .controlSubscription(subscription(controlStream));
    }

    public RaftNode(final RaftNodeConfiguration configuration, final long timeInMs)
    {
        this.configuration = configuration;
        this.nodeId = configuration.nodeId();

        final long timeoutIntervalInMs = configuration.timeoutIntervalInMs();
        final long heartbeatTimeInMs = timeoutIntervalInMs / HEARTBEAT_TO_TIMEOUT_RATIO;
        final int clusterSize = configuration.otherNodes().size() + 1;

        leader = new Leader(
            nodeId,
            configuration.acknowledgementStrategy(),
            configuration.otherNodes(),
            this,
            configuration.fragmentHandler(),
            timeInMs,
            heartbeatTimeInMs,
            termState,
            configuration.leaderSessionId(),
            configuration.archiveReader());

        candidate = new Candidate(
            nodeId,
            this,
            clusterSize,
            timeoutIntervalInMs,
            termState);

        follower = new Follower(
            nodeId,
            configuration.fragmentHandler(),
            this,
            timeInMs,
            timeoutIntervalInMs,
            termState,
            configuration.archiveReader(),
            configuration.archiver());

        startAsFollower(timeInMs);
    }

    private void startAsFollower(final long timeInMs)
    {
        injectFollowerStreams();

        currentRole = follower.follow(timeInMs);
    }

    private Publication publication(final StreamIdentifier id)
    {
        return configuration.aeron().addPublication(id.channel(), id.streamId());
    }

    private RaftPublication raftPublication(final StreamIdentifier id)
    {
        return new RaftPublication(
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
        candidateState.transitionToFollower(candidate, timeInMs);
    }

    public void transitionToFollower(final Leader leader, final long timeInMs)
    {
        leaderState.transitionToFollower(leader, timeInMs);
    }

    public void transitionToLeader(final long timeInMs)
    {
        candidateState.transitionToLeader(candidate, timeInMs);
    }

    public void transitionToCandidate(final long timeInMs)
    {
        followerState.transitionToCandidate(follower, timeInMs);
    }

    public int poll(final int fragmentLimit, final long timeInMs)
    {
        final Role role = currentRole;
        final int commandCount = role.pollCommands(fragmentLimit, timeInMs);

        if (role != currentRole)
        {
            return commandCount + poll(fragmentLimit, timeInMs);
        }

        return commandCount +
               role.readData() +
               role.checkConditions(timeInMs);
    }

    public void closeStreams()
    {
        currentRole.closeStreams();
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
