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

import io.aeron.Publication;
import org.agrona.collections.IntHashSet;
import uk.co.real_logic.fix_gateway.DebugLogger;
import uk.co.real_logic.fix_gateway.engine.logger.ArchiveReader;
import uk.co.real_logic.fix_gateway.engine.logger.Archiver;

import static java.util.Objects.requireNonNull;

/**
 * Agent that manages the clustering and archival.
 */
public class ClusterAgent
{
    private static final int HEARTBEAT_TO_TIMEOUT_RATIO = 5;

    private final short nodeId;
    private final TermState termState = new TermState();
    private final Leader leader;
    private final Candidate candidate;
    private final Follower follower;
    private final RaftTransport transport;
    private final InboundPipe inboundPipe;
    private final OutboundPipe outboundPipe;
    private final ClusterStreams clusterStreams;

    private Role currentRole;

    public ClusterAgent(final ClusterNodeConfiguration configuration, final long timeInMs)
    {
        configuration.conclude();

        nodeId = configuration.nodeId();
        transport = configuration.raftTransport();

        final Publication dataPublication = transport.leaderPublication();
        final ArchiveReader archiveReader = configuration.archiveReader();
        final int ourSessionId = dataPublication.sessionId();
        final long timeoutIntervalInMs = configuration.timeoutIntervalInMs();
        final long heartbeatTimeInMs = timeoutIntervalInMs / HEARTBEAT_TO_TIMEOUT_RATIO;
        final IntHashSet otherNodes = configuration.otherNodes();
        final int clusterSize = otherNodes.size() + 1;
        final AcknowledgementStrategy acknowledgementStrategy = configuration.acknowledgementStrategy();
        final Archiver archiver = configuration.archiver();
        final RaftArchiver raftArchiver = new RaftArchiver(termState.leaderSessionId(), archiver);

        requireNonNull(otherNodes, "otherNodes");
        requireNonNull(acknowledgementStrategy, "acknowledgementStrategy");
        requireNonNull(archiveReader, "archiveReader");
        requireNonNull(archiver, "archiver");

        leader = new Leader(
            nodeId,
            acknowledgementStrategy,
            otherNodes,
            this,
            timeInMs,
            heartbeatTimeInMs,
            termState,
            ourSessionId,
            archiveReader,
            raftArchiver
        );

        candidate = new Candidate(
            nodeId,
            ourSessionId,
            this,
            clusterSize,
            timeoutIntervalInMs,
            termState,
            acknowledgementStrategy);

        follower = new Follower(
            nodeId,
            this,
            timeInMs,
            timeoutIntervalInMs,
            termState,
            raftArchiver
        );

        transport.initialiseRoles(leader, candidate, follower);

        startAsFollower(timeInMs);

        clusterStreams = new ClusterStreams(
            transport, ourSessionId, termState.leaderSessionId(), termState.consensusPosition(), dataPublication);

        inboundPipe = new InboundPipe(
            configuration.copyFromSubscription(), configuration.nonLeaderHandler(), clusterStreams());
        outboundPipe = new OutboundPipe(
            configuration.copyToPublication(), clusterStreams());
    }

    private abstract class NodeState
    {
        void transitionToLeader(final Candidate candidate, final long timeInMs)
        {
            throw new UnsupportedOperationException();
        }

        void transitionToCandidate(final Follower follower, final long timeInMs)
        {
            throw new UnsupportedOperationException();
        }

        void transitionToFollower(final Candidate candidate, final short votedFor, final long timeInMs)
        {
            throw new UnsupportedOperationException();
        }

        void transitionToFollower(final Leader leader, final short votedFor, final long timeInMs)
        {
            throw new UnsupportedOperationException();
        }

        void transitionToCandidate(final Candidate candidate, final long timeInMs)
        {
            throw new UnsupportedOperationException();
        }
    }

    private final NodeState leaderState = new NodeState()
    {
        void transitionToFollower(final Leader leader, final short votedFor, final long timeInMs)
        {
            DebugLogger.log("%d: L -> Follower @ %d in %d\n", nodeId, timeInMs, termState.leadershipTerm());

            leader.closeStreams();

            transport.injectFollowerSubscriptions(follower);

            currentRole = follower.votedFor(votedFor)
                .follow(timeInMs);

            onNewLeader();
        }
    };

    private final NodeState followerState = new NodeState()
    {
        void transitionToCandidate(final Follower follower, final long timeInMs)
        {
            DebugLogger.log("%d: F -> Candidate @ %d in %d\n", nodeId, timeInMs, termState.leadershipTerm());

            follower.closeStreams();

            currentRole = candidate.startNewElection(timeInMs);
        }
    };

    private final NodeState candidateState = new NodeState()
    {
        void transitionToLeader(final Candidate candidate, long timeInMs)
        {
            DebugLogger.log("%d: C -> Leader @ %d in %d\n", nodeId, timeInMs, termState.leadershipTerm());

            candidate.closeStreams();

            transport.injectLeaderSubscriptions(leader);

            currentRole = leader.getsElected(timeInMs);

            onNewLeader();
        }

        void transitionToFollower(final Candidate candidate, final short votedFor, final long timeInMs)
        {
            DebugLogger.log("%d: C -> Follower @ %d in %d\n", nodeId, timeInMs, termState.leadershipTerm());

            candidate.closeStreams();

            transport.injectFollowerSubscriptions(follower);

            currentRole = follower.votedFor(votedFor)
                .follow(timeInMs);
        }
    };

    private void startAsFollower(final long timeInMs)
    {
        transport.injectFollowerSubscriptions(follower);

        currentRole = follower.follow(timeInMs);
    }

    void transitionToFollower(final Candidate candidate, final short votedFor, final long timeInMs)
    {
        candidateState.transitionToFollower(candidate, votedFor, timeInMs);
    }

    void transitionToFollower(final Leader leader, final short votedFor, final long timeInMs)
    {
        leaderState.transitionToFollower(leader, votedFor, timeInMs);
    }

    void transitionToLeader(final long timeInMs)
    {
        candidateState.transitionToLeader(candidate, timeInMs);
    }

    void transitionToCandidate(final long timeInMs)
    {
        followerState.transitionToCandidate(follower, timeInMs);
    }

    void onNewLeader()
    {
        // TODO: do we need this?
        //leaderSessionId.set(this.termState.leaderSessionId());
    }

    public int poll(final int fragmentLimit, final long timeInMs)
    {
        final Role role = currentRole;
        final int commandCount = role.pollCommands(fragmentLimit, timeInMs);

        if (role != currentRole)
        {
            final int remainingFragments = fragmentLimit - commandCount;
            return commandCount + poll(remainingFragments, timeInMs);
        }

        return commandCount +
               role.readData() +
               role.checkConditions(timeInMs) +
               inboundPipe.poll(fragmentLimit) +
               outboundPipe.poll(fragmentLimit);
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

    public ClusterStreams clusterStreams()
    {
        return clusterStreams;
    }
}
