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
import org.agrona.DirectBuffer;
import org.agrona.collections.IntHashSet;
import uk.co.real_logic.fix_gateway.DebugLogger;
import uk.co.real_logic.fix_gateway.messages.AcknowledgementStatus;
import uk.co.real_logic.fix_gateway.messages.Vote;

import static uk.co.real_logic.fix_gateway.messages.Vote.AGAINST;
import static uk.co.real_logic.fix_gateway.messages.Vote.FOR;
import static uk.co.real_logic.fix_gateway.replication.Follower.NO_ONE;

public class Candidate implements Role, RaftHandler
{
    private final RaftSubscriber raftSubscriber;

    private final TermState termState;
    private final short nodeId;
    private final int sessionId;
    private final RaftNode raftNode;
    private final int clusterSize;
    private final AcknowledgementStrategy acknowledgementStrategy;
    private final IntHashSet votesFor;
    private final RandomTimeout voteTimeout;

    private RaftPublication controlPublication;
    private Subscription controlSubscription;
    private int leaderShipTerm;
    private long position;
    private long timeInMs;

    public Candidate(final short nodeId,
                     final int sessionId,
                     final RaftNode raftNode,
                     final int clusterSize,
                     final long voteTimeout,
                     final TermState termState,
                     final AcknowledgementStrategy acknowledgementStrategy)
    {
        this.nodeId = nodeId;
        this.sessionId = sessionId;
        this.raftNode = raftNode;
        this.clusterSize = clusterSize;
        this.acknowledgementStrategy = acknowledgementStrategy;
        this.voteTimeout = new RandomTimeout(voteTimeout, 0L);
        this.termState = termState;
        votesFor = new IntHashSet(2 * clusterSize, -1);
        raftSubscriber = new RaftSubscriber(DebugRaftHandler.wrap(nodeId, this));
    }

    public int checkConditions(final long timeInMs)
    {
        if (voteTimeout.hasTimedOut(timeInMs))
        {
            DebugLogger.log("%d: restartElection @ %d in %d\n", nodeId, timeInMs, leaderShipTerm);

            startElection(timeInMs);

            return 1;
        }

        return 0;
    }

    public int pollCommands(final int fragmentLimit, final long timeInMs)
    {
        this.timeInMs = timeInMs;

        return controlSubscription.poll(raftSubscriber, fragmentLimit);
    }

    public void closeStreams()
    {

    }

    public void onMessageAcknowledgement(
        final long newAckedPosition, final short nodeId, final AcknowledgementStatus status)
    {

    }

    public void onRequestVote(
        final short candidateId, final int candidateSessionId, final int leaderShipTerm, long lastAckedPosition)
    {
        if (leaderShipTerm > this.leaderShipTerm && lastAckedPosition >= this.position)
        {
            replyVote(candidateId, leaderShipTerm, FOR);

            transitionToFollower(leaderShipTerm, candidateId, this.position, candidateSessionId);
        }
        else
        {
            replyVote(candidateId, leaderShipTerm, AGAINST);
        }
    }

    private long replyVote(final short candidateId, final int leaderShipTerm, final Vote vote)
    {
        return controlPublication.saveReplyVote(nodeId, candidateId, leaderShipTerm, vote);
    }

    public void onReplyVote(
        final short senderNodeId, final short candidateId, final int leaderShipTerm, final Vote vote)
    {
        DebugLogger.log("%d: Received vote from %d about %d in %d%n", nodeId, senderNodeId, candidateId, leaderShipTerm);

        if (shouldCountVote(candidateId, leaderShipTerm, vote) && countVote(senderNodeId))
        {
            voteTimeout.onKeepAlive(timeInMs);

            if (acknowledgementStrategy.isElected(votesFor.size(), clusterSize))
            {
                termState
                    .leadershipTerm(leaderShipTerm)
                    .leaderSessionId(sessionId);

                raftNode.transitionToLeader(timeInMs);
            }
        }
    }

    private boolean countVote(final short senderNodeId)
    {
        return votesFor.add(senderNodeId);
    }

    private boolean shouldCountVote(final short candidateId, final int leaderShipTerm, final Vote vote)
    {
        return candidateId == nodeId && leaderShipTerm == this.leaderShipTerm && vote == FOR;
    }

    public void onConcensusHeartbeat(short nodeId,
                                     final int leaderShipTerm,
                                     final long position,
                                     final int dataSessionId)
    {
        if (nodeId != this.nodeId)
        {
            final boolean hasHigherPosition = position >= this.position;
            if (hasHigherPosition)
            {
                transitionToFollower(leaderShipTerm, NO_ONE, position, dataSessionId);
            }
            DebugLogger.log("%d: New Leader %s%n", this.nodeId, hasHigherPosition);
        }
    }

    private void transitionToFollower(final int leaderShipTerm,
                                      final short votedFor,
                                      final long position,
                                      final int leaderSessionId)
    {
        votesFor.clear();
        termState
            .allPositions(position)
            .leaderSessionId(leaderSessionId)
            .leadershipTerm(leaderShipTerm);

        raftNode.transitionToFollower(this, votedFor, timeInMs);
    }

    public void onResend(final int leaderSessionId,
                         final int leaderShipTerm,
                         final long startPosition,
                         final DirectBuffer bodyBuffer,
                         final int bodyOffset,
                         final int bodyLength)
    {
        // Ignore this message
    }

    public Candidate startNewElection(final long timeInMs)
    {
        this.position = termState.commitPosition();
        this.leaderShipTerm = termState.leadershipTerm();

        DebugLogger.log("%d: startNewElection @ %d in %d\n", nodeId, timeInMs, leaderShipTerm);

        startElection(timeInMs);
        return this;
    }

    private void startElection(final long timeInMs)
    {
        voteTimeout.onKeepAlive(timeInMs);
        leaderShipTerm++;
        countVote(nodeId); // Vote for yourself
        controlPublication.saveRequestVote(nodeId, sessionId, position, leaderShipTerm);
    }

    public Candidate controlPublication(final RaftPublication controlPublication)
    {
        this.controlPublication = controlPublication;
        return this;
    }

    public Candidate controlSubscription(final Subscription controlSubscription)
    {
        this.controlSubscription = controlSubscription;
        return this;
    }

    public long commitPosition()
    {
        return termState.commitPosition();
    }
}
