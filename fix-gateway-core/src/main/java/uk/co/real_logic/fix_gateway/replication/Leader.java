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
import io.aeron.logbuffer.BlockHandler;
import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import org.agrona.DirectBuffer;
import org.agrona.collections.CollectionUtil;
import org.agrona.collections.IntHashSet;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.engine.logger.ArchiveReader;
import uk.co.real_logic.fix_gateway.replication.messages.AcknowledgementStatus;
import uk.co.real_logic.fix_gateway.replication.messages.Vote;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.*;
import static io.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static uk.co.real_logic.fix_gateway.replication.messages.AcknowledgementStatus.MISSING_LOG_ENTRIES;
import static uk.co.real_logic.fix_gateway.replication.messages.AcknowledgementStatus.OK;

class Leader implements Role, RaftHandler
{
    private static final UnsafeBuffer EMPTY_BUFFER = new UnsafeBuffer(new byte[0]);

    private static final int NO_SESSION_ID = -1;

    private final TermState termState;
    private final int ourSessionId;
    private final short nodeId;
    private final AcknowledgementStrategy acknowledgementStrategy;
    private final RaftSubscription raftSubscription;
    private final ClusterAgent clusterNode;
    private final long heartbeatIntervalInMs;
    private final ArchiveReader archiveReader;

    // Counts of how many acknowledgements
    private final Long2LongHashMap nodeToPosition = new Long2LongHashMap(NO_SESSION_ID);
    private final List<ResendHandler> resendHandlers = new ArrayList<>();

    private ArchiveReader.SessionReader ourArchiveReader;
    private RaftPublication controlPublication;
    private Subscription acknowledgementSubscription;
    private Subscription dataSubscription;
    private Subscription controlSubscription;

    /** Position in the log that has been acknowledged by a majority of the cluster
     * commitPosition >= lastAppliedPosition
     */
    private final AtomicLong consensusPosition;
    private final RaftArchiver raftArchiver;
    private final DirectBuffer nodeState;
    private final NodeStateHandler nodeStateHandler;
    /** Position in the log that has been applied to the state machine*/
    private long lastAppliedPosition;

    /**
     * This is [concensusPosition] - [streamPosition]. Updated
     * when you get elected leader and valid for the duration of your leadership term.
     */
    private long streamPositionDelta;
    private long previousStreamPosition;

    private long nextHeartbeatTimeInMs;
    private long timeInMs;

    Leader(
        final short nodeId,
        final AcknowledgementStrategy acknowledgementStrategy,
        final IntHashSet followers,
        final ClusterAgent clusterNode,
        final long timeInMs,
        final long heartbeatIntervalInMs,
        final TermState termState,
        final int ourSessionId,
        final ArchiveReader archiveReader,
        final RaftArchiver raftArchiver,
        final DirectBuffer nodeState,
        final NodeStateHandler nodeStateHandler)
    {
        this.nodeId = nodeId;
        this.acknowledgementStrategy = acknowledgementStrategy;
        this.clusterNode = clusterNode;
        this.termState = termState;
        this.ourSessionId = ourSessionId;
        this.heartbeatIntervalInMs = heartbeatIntervalInMs;
        this.archiveReader = archiveReader;
        this.consensusPosition = termState.consensusPosition();
        this.raftArchiver = raftArchiver;
        this.nodeState = nodeState;
        this.nodeStateHandler = nodeStateHandler;

        followers.forEach(follower -> nodeToPosition.put(follower, 0));
        updateNextHeartbeatTime(timeInMs);
        raftSubscription = new RaftSubscription(DebugRaftHandler.wrap(nodeId, this));
    }

    public int readData()
    {
        final RaftArchiver raftArchiver = this.raftArchiver;
        if (raftArchiver.checkLeaderArchiver())
        {
            return 0;
        }

        final int bytesRead = raftArchiver.poll();
        if (bytesRead > 0)
        {
            nodeToPosition.put(ourSessionId, raftArchiver.archivedPosition());
        }

        return bytesRead;
    }

    public int checkConditions(final long timeInMs)
    {
        final int resends = CollectionUtil.removeIf(resendHandlers, ResendHandler::reAttemptResend);
        final long newPosition = acknowledgementStrategy.findAckedTerm(nodeToPosition);
        final int delta = (int) (newPosition - consensusPosition.get());
        if (delta > 0)
        {
            consensusPosition.set(newPosition);

            heartbeat();

            // Deliberately Suppress below heartbeat because there's no need to send two
            return resends + delta;
        }

        if (timeInMs > nextHeartbeatTimeInMs)
        {
            heartbeat();

            return resends + 1;
        }

        return resends;
    }

    public int pollCommands(final int fragmentLimit, final long timeInMs)
    {
        this.timeInMs = timeInMs;

        return acknowledgementSubscription.controlledPoll(raftSubscription, fragmentLimit) +
               controlSubscription.controlledPoll(raftSubscription, fragmentLimit);
    }

    public void closeStreams()
    {
        if (acknowledgementSubscription != null)
        {
            acknowledgementSubscription.close();
        }
        if (dataSubscription != null)
        {
            dataSubscription.close();
        }
    }

    private void heartbeat()
    {
        final long currentPosition = consensusPosition.get();
        heartbeat(currentPosition);
    }

    private void heartbeat(final long currentPosition)
    {
        final long streamPosition = currentPosition - streamPositionDelta;
        if (controlPublication.saveConsensusHeartbeat(
            nodeId,
            termState.leadershipTerm(),
            currentPosition,
            ourSessionId,
            previousStreamPosition,
            streamPosition) > 0)
        {
            previousStreamPosition = streamPosition;
            updateNextHeartbeatTime(timeInMs);
        }
    }

    void updateNextHeartbeatTime(final long timeInMs)
    {
        this.nextHeartbeatTimeInMs = timeInMs + heartbeatIntervalInMs;
    }

    public Action onMessageAcknowledgement(
        long position, final short nodeId, final AcknowledgementStatus status)
    {
        if (status == OK)
        {
            nodeToPosition.put(nodeId, position);
        }

        if (status == MISSING_LOG_ENTRIES)
        {
            final int length = (int) (raftArchiver.archivedPosition() - position);
            if (validateReader())
            {
                final ResendHandler resendHandler = new ResendHandler();
                resendHandler.messageAcknowledgementPosition = position;
                final long readPosition = Math.max(position, HEADER_LENGTH);
                if (!ourArchiveReader.readBlock(readPosition, length, resendHandler))
                {
                    resendHandler.emptyResend();
                }

                if (!resendHandler.resendIsComplete())
                {
                    resendHandlers.add(resendHandler);
                }
            }
            else
            {
                return ABORT;
            }
        }

        return CONTINUE;
    }

    private boolean validateReader()
    {
        if (ourArchiveReader == null)
        {
            ourArchiveReader = archiveReader.session(ourSessionId);
            termState.leaderSessionId(ourSessionId);
            if (ourArchiveReader == null)
            {
                return false;
            }
        }

        return true;
    }

    public Action onRequestVote(
        final short candidateId, final int candidateSessionId, final int leaderShipTerm, final long lastAckedPosition)
    {
        // Ignore requests from yourself
        if (candidateId != this.nodeId)
        {
            if (termState.leadershipTerm() < leaderShipTerm && lastAckedPosition >= consensusPosition.get())
            {
                if (!replyVote(candidateId, leaderShipTerm, Vote.FOR))
                {
                    return ABORT;
                }

                termState.noLeader();

                transitionToFollower(leaderShipTerm, candidateId);
                return BREAK;
            }
            else
            {
                if (!replyVote(candidateId, leaderShipTerm, Vote.AGAINST))
                {
                    return ABORT;
                }
            }
        }

        return CONTINUE;
    }

    private boolean replyVote(final short candidateId, final int leaderShipTerm, final Vote against)
    {
        return controlPublication.saveReplyVote(nodeId, candidateId, leaderShipTerm, against, nodeState) >= 0;
    }

    public Action onReplyVote(
        final short senderNodeId,
        final short candidateId,
        final int leaderShipTerm,
        final Vote vote,
        final DirectBuffer nodeStateBuffer,
        final int nodeStateLength,
        final int aeronSessionId)
    {
        if (candidateId == nodeId)
        {
            nodeStateHandler.onNewNodeState(senderNodeId, aeronSessionId, nodeStateBuffer, nodeStateLength);
        }

        return CONTINUE;
    }

    public Action onResend(
        final int leaderSessionId,
        final int leaderShipTerm,
        final long startPosition,
        final DirectBuffer bodyBuffer,
        final int bodyOffset,
        final int bodyLength)
    {
        // Ignore this message
        return CONTINUE;
    }

    public Action onConsensusHeartbeat(final short nodeId,
                                       final int leaderShipTerm,
                                       final long position, final long startPosition,
                                       final long streamPosition, final int leaderSessionId)
    {
        if (nodeId != this.nodeId && leaderShipTerm > termState.leadershipTerm())
        {
            termState.leaderSessionId(leaderSessionId);

            transitionToFollower(leaderShipTerm, Follower.NO_ONE);

            return BREAK;
        }

        return CONTINUE;
    }

    private void transitionToFollower(final int newLeaderShipTerm, final short votedFor)
    {
        termState
            .leadershipTerm(newLeaderShipTerm)
            .lastAppliedPosition(lastAppliedPosition)
            .receivedPosition(lastAppliedPosition);

        clusterNode.transitionToFollower(this, votedFor, timeInMs);
    }

    Leader getsElected(final long timeInMs, final long streamPosition)
    {
        this.timeInMs = timeInMs;

        termState.leaderSessionId(ourSessionId);

        lastAppliedPosition = Math.max(HEADER_LENGTH, termState.lastAppliedPosition());

        final long currentPosition = consensusPosition.get();
        streamPositionDelta = currentPosition - streamPosition;
        heartbeat(currentPosition);

        raftArchiver.checkLeaderArchiver();

        return this;
    }

    Leader acknowledgementSubscription(final Subscription acknowledgementSubscription)
    {
        this.acknowledgementSubscription = acknowledgementSubscription;
        return this;
    }

    Leader dataSubscription(final Subscription dataSubscription)
    {
        this.dataSubscription = dataSubscription;
        raftArchiver.dataSubscription(dataSubscription);
        return this;
    }

    Leader controlPublication(final RaftPublication controlPublication)
    {
        this.controlPublication = controlPublication;
        return this;
    }

    Leader controlSubscription(final Subscription controlSubscription)
    {
        this.controlSubscription = controlSubscription;
        return this;
    }

    private class ResendHandler implements BlockHandler
    {
        // retry a resend when it gets back pressured
        private long messageAcknowledgementPosition;
        private DirectBuffer repeatResendBuffer;
        private int repeatResendOffset;
        private int repeatResendLength;

        public void onBlock(
            final DirectBuffer buffer, final int offset, final int length, final int sessionId, final int termId)
        {
            saveResend(buffer, offset, length);
        }

        private void saveResend(final DirectBuffer buffer, final int offset, final int length)
        {
            if (controlPublication.saveResend(
                ourSessionId, termState.leadershipTerm(), messageAcknowledgementPosition, buffer, offset, length) < 0)
            {
                repeatResendBuffer = buffer;
                repeatResendOffset = offset;
                repeatResendLength = length;
            }
            else
            {
                repeatResendBuffer = null;
                repeatResendOffset = 0;
                repeatResendLength = 0;
            }
        }

        private boolean reAttemptResend()
        {
            final boolean resendIsComplete = resendIsComplete();
            if (!resendIsComplete)
            {
                saveResend(repeatResendBuffer, repeatResendOffset, repeatResendLength);
            }
            return resendIsComplete;
        }

        private boolean resendIsComplete()
        {
            return repeatResendBuffer == null;
        }

        private void emptyResend()
        {
            saveResend(EMPTY_BUFFER, 0, 0);
        }
    }

}
