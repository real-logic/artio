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

import uk.co.real_logic.fix_gateway.DebugLogger;
import uk.co.real_logic.fix_gateway.engine.logger.ArchiveReader;

/**
 * .
 */
public class RaftNode implements Role
{
    public static final int HEARTBEAT_TO_TIMEOUT_RATIO = 5;

    private final short nodeId;
    private final RaftNodeConfiguration configuration;
    private final TermState termState = new TermState();
    private final Leader leader;
    private final Candidate candidate;
    private final Follower follower;
    private final RaftTransport transport;

    private Role currentRole;

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

        public void transitionToFollower(final Candidate candidate, final short votedFor, final long timeInMs)
        {
            throw new UnsupportedOperationException();
        }

        public void transitionToFollower(final Leader leader, final short votedFor, final long timeInMs)
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
        public void transitionToFollower(final Leader leader, final short votedFor, final long timeInMs)
        {
            DebugLogger.log("%d: L -> Follower @ %d in %d\n", nodeId, timeInMs, termState.leadershipTerm());

            leader.closeStreams();

            transport.injectFollowerSubscriptions(follower);

            currentRole = follower.votedFor(votedFor)
                                  .follow(timeInMs);
        }
    };

    private final NodeState followerState = new NodeState()
    {
        public void transitionToCandidate(final Follower follower, final long timeInMs)
        {
            DebugLogger.log("%d: F -> Candidate @ %d in %d\n", nodeId, timeInMs, termState.leadershipTerm());

            follower.closeStreams();

            currentRole = candidate.startNewElection(timeInMs);
        }
    };

    private final NodeState candidateState = new NodeState()
    {
        public void transitionToLeader(final Candidate candidate, long timeInMs)
        {
            DebugLogger.log("%d: C -> Leader @ %d in %d\n", nodeId, timeInMs, termState.leadershipTerm());

            candidate.closeStreams();

            transport.injectLeaderSubscriptions(leader);

            currentRole = leader.getsElected(timeInMs);
        }

        public void transitionToFollower(final Candidate candidate, final short votedFor, final long timeInMs)
        {
            DebugLogger.log("%d: C -> Follower @ %d in %d\n", nodeId, timeInMs, termState.leadershipTerm());

            candidate.closeStreams();

            transport.injectFollowerSubscriptions(follower);

            currentRole = follower.votedFor(votedFor)
                                  .follow(timeInMs);
        }
    };

    public RaftNode(final RaftNodeConfiguration configuration, final long timeInMs)
    {
        this.configuration = configuration;
        this.nodeId = configuration.nodeId();
        this.transport = configuration.raftTransport();

        final long timeoutIntervalInMs = configuration.timeoutIntervalInMs();
        final long heartbeatTimeInMs = timeoutIntervalInMs / HEARTBEAT_TO_TIMEOUT_RATIO;
        final int clusterSize = configuration.otherNodes().size() + 1;
        final int ourSessionId = configuration.leaderSessionId();
        final ArchiveReader archiveReader = configuration.archiveReader();

        leader = new Leader(
            nodeId,
            configuration.acknowledgementStrategy(),
            configuration.otherNodes(),
            this,
            configuration.fragmentHandler(),
            timeInMs,
            heartbeatTimeInMs,
            termState,
            ourSessionId,
            archiveReader);

        candidate = new Candidate(
            nodeId,
            ourSessionId,
            this,
            clusterSize,
            timeoutIntervalInMs,
            termState,
            configuration.acknowledgementStrategy());

        follower = new Follower(
            nodeId,
            configuration.fragmentHandler(),
            this,
            timeInMs,
            timeoutIntervalInMs,
            termState,
            archiveReader,
            configuration.archiver());

        transport.initialiseRoles(leader, candidate, follower);

        startAsFollower(timeInMs);
    }

    private void startAsFollower(final long timeInMs)
    {
        transport.injectFollowerSubscriptions(follower);

        currentRole = follower.follow(timeInMs);
    }

    public void transitionToFollower(final Candidate candidate, final short votedFor, final long timeInMs)
    {
        candidateState.transitionToFollower(candidate, votedFor, timeInMs);
    }

    public void transitionToFollower(final Leader leader, final short votedFor, final long timeInMs)
    {
        leaderState.transitionToFollower(leader, votedFor, timeInMs);
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

    public TermState termState()
    {
        return termState;
    }
}
