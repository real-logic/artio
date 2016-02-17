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

import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.logbuffer.FragmentHandler;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.fix_gateway.DebugLogger;
import uk.co.real_logic.fix_gateway.engine.logger.ArchiveReader;
import uk.co.real_logic.fix_gateway.engine.logger.Archiver;
import uk.co.real_logic.fix_gateway.engine.logger.Archiver.SessionArchiver;
import uk.co.real_logic.fix_gateway.messages.AcknowledgementStatus;
import uk.co.real_logic.fix_gateway.messages.Vote;

import static uk.co.real_logic.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static uk.co.real_logic.fix_gateway.messages.AcknowledgementStatus.MISSING_LOG_ENTRIES;
import static uk.co.real_logic.fix_gateway.messages.AcknowledgementStatus.OK;
import static uk.co.real_logic.fix_gateway.messages.Vote.AGAINST;
import static uk.co.real_logic.fix_gateway.messages.Vote.FOR;

public class Follower implements Role, RaftHandler
{
    public static final short NO_ONE = -1;

    private final RaftSubscriber raftSubscriber;

    private final short nodeId;
    private final FragmentHandler handler;
    private final RaftNode raftNode;
    private final TermState termState;
    private final ArchiveReader archiveReader;
    private final Archiver archiver;
    private final RandomTimeout replyTimeout;

    private RaftPublication acknowledgementPublication;
    private RaftPublication controlPublication;
    private SessionArchiver leaderArchiver;
    private ArchiveReader.SessionReader leaderArchiveReader;
    private Subscription controlSubscription;
    private long receivedPosition;
    private long commitPosition;
    private long lastAppliedPosition;

    private short votedFor = NO_ONE;
    private int leaderShipTerm;
    private long timeInMs;

    public Follower(
        final short nodeId,
        final FragmentHandler handler,
        final RaftNode raftNode,
        final long timeInMs,
        final long replyTimeoutInMs,
        final TermState termState,
        final ArchiveReader archiveReader,
        final Archiver archiver)
    {
        this.nodeId = nodeId;
        this.handler = handler;
        this.raftNode = raftNode;
        this.termState = termState;
        this.archiveReader = archiveReader;
        this.archiver = archiver;
        replyTimeout = new RandomTimeout(replyTimeoutInMs, timeInMs);
        raftSubscriber = new RaftSubscriber(DebugRaftHandler.wrap(nodeId, this));
    }

    public int pollCommands(final int fragmentLimit, final long timeInMs)
    {
        this.timeInMs = timeInMs;

        final int read = controlSubscription.poll(raftSubscriber, fragmentLimit);
        if (read > 0)
        {
            onReplyKeepAlive(this.timeInMs);
        }
        return read;
    }

    public int checkConditions(final long timeInMs)
    {
        if (replyTimeout.hasTimedOut(timeInMs))
        {
            termState
                .receivedPosition(receivedPosition)
                .lastAppliedPosition(lastAppliedPosition)
                .commitPosition(commitPosition)
                .leadershipTerm(leaderShipTerm)
                .noLeader();

            raftNode.transitionToCandidate(timeInMs);

            return 1;
        }

        return 0;
    }

    public int readData()
    {
        if (checkLeaderArchiver())
        {
            return 0;
        }

        final long imagePosition = leaderArchiver.archivedPosition();
        if (imagePosition < receivedPosition)
        {
            // TODO: theoretically not possible, but maybe have a sanity check?
        }

        if (imagePosition > receivedPosition)
        {
            saveMessageAcknowledgement(MISSING_LOG_ENTRIES);

            return 1;
        }

        final int bytesRead = leaderArchiver.poll();
        if (bytesRead > 0)
        {
            receivedPosition += bytesRead;
            saveMessageAcknowledgement(OK);
            onReplyKeepAlive(timeInMs);
        }

        return bytesRead + attemptToCommitData();
    }

    private boolean checkLeaderArchiver()
    {
        // Leader may not have written anything onto its data stream when it becomes the leader
        // Most of the time this will be false
        if (leaderArchiver == null)
        {
            leaderArchiver = archiver.session(termState.leaderSessionId());
            termState.leaderSessionId(termState.leaderSessionId());
            if (leaderArchiver == null)
            {
                return true;
            }
        }
        return false;
    }

    private void saveMessageAcknowledgement(final AcknowledgementStatus status)
    {
        acknowledgementPublication.saveMessageAcknowledgement(receivedPosition, nodeId, status);
    }

    private int attemptToCommitData()
    {
        // see readData()
        if (leaderArchiveReader == null)
        {
            leaderArchiveReader = archiveReader.session(termState.leaderSessionId());
            if (leaderArchiveReader == null)
            {
                return 0;
            }
        }

        final long canCommitUpToPosition = Math.min(commitPosition, receivedPosition);
        final int committableBytes = (int) (canCommitUpToPosition - lastAppliedPosition);
        if (committableBytes > 0)
        {
            final int readBytes = (int) leaderArchiveReader.readUpTo(
                lastAppliedPosition + HEADER_LENGTH, committableBytes, handler);

            if (readBytes < committableBytes)
            {
                System.err.printf("Wanted to read %d, but only read %d%n", committableBytes, readBytes);
            }

            return readBytes;
        }

        return 0;
    }

    public void closeStreams()
    {
    }

    private void onReplyKeepAlive(final long timeInMs)
    {
        replyTimeout.onKeepAlive(timeInMs);
    }

    public void onMessageAcknowledgement(
        final long newAckedPosition, final short nodeId, final AcknowledgementStatus status)
    {
        // not interested in this message
    }

    public void onRequestVote(
        final short candidateId, final int candidateSessionId, final int leaderShipTerm, final long candidatePosition)
    {
        if (canVoteFor(candidateId) && safeToVote(leaderShipTerm, candidatePosition))
        {
            votedFor = candidateId;
            controlPublication.saveReplyVote(nodeId, candidateId, leaderShipTerm, FOR);
            DebugLogger.log("%d: vote for %d in %d%n", nodeId, candidateId, leaderShipTerm);
            onReplyKeepAlive(timeInMs);
        }
        else if (candidateId != nodeId)
        {
            controlPublication.saveReplyVote(nodeId, candidateId, leaderShipTerm, AGAINST);
            DebugLogger.log("%d: vote against %d in %d%n", nodeId, candidateId, leaderShipTerm);
        }
    }

    private boolean safeToVote(final int leaderShipTerm, final long candidatePosition)
    {
        // Term has to be strictly greater because a follower has already
        // Voted for someone in its current leaderShipTerm and is electing for the
        // next leaderShipTerm
        return candidatePosition >= receivedPosition && leaderShipTerm > this.leaderShipTerm;
    }

    private boolean canVoteFor(final short candidateId)
    {
        return votedFor == NO_ONE || votedFor == candidateId;
    }

    public void onReplyVote(
        final short senderNodeId, final short candidateId, final int leaderShipTerm, final Vote vote)
    {
        // not interested in this message
    }

    public void onConcensusHeartbeat(final short leaderNodeId,
                                     final int leaderShipTerm,
                                     final long position,
                                     final int leaderSessionId)
    {
        if (leaderNodeId != this.nodeId &&
            leaderShipTerm > this.leaderShipTerm)
        {
            termState
                .leadershipTerm(leaderShipTerm)
                .receivedPosition(receivedPosition)
                .lastAppliedPosition(lastAppliedPosition)
                .commitPosition(position)
                .leaderSessionId(leaderSessionId);

            if (leaderSessionId != termState.leaderSessionId())
            {
                checkLeaderChange();
            }

            follow(this.timeInMs);
        }

        if (position > commitPosition)
        {
            commitPosition = position;
        }
    }

    public void onResend(final int leaderSessionId,
                         final int leaderShipTerm,
                         final long startPosition,
                         final DirectBuffer bodyBuffer,
                         final int bodyOffset,
                         final int bodyLength)
    {
        if (isValidPosition(leaderSessionId, leaderShipTerm, startPosition))
        {
            if (!checkLeaderArchiver())
            {
                leaderArchiver.patch(bodyBuffer, bodyOffset, bodyLength);
                receivedPosition += bodyLength;
                onReplyKeepAlive(timeInMs);
                saveMessageAcknowledgement(OK);
            }
        }
    }

    private boolean isValidPosition(
        final int leaderSessionId, final int leaderShipTerm, final long position)
    {
        return position == receivedPosition
            && leaderSessionId == termState.leaderSessionId()
            && leaderShipTerm == this.leaderShipTerm;
    }

    public Follower follow(final long timeInMs)
    {
        onReplyKeepAlive(timeInMs);
        votedFor = NO_ONE;
        readTermState();
        return this;
    }

    private void readTermState()
    {
        leaderShipTerm = termState.leadershipTerm();
        receivedPosition = termState.receivedPosition();
        lastAppliedPosition = termState.lastAppliedPosition();
        commitPosition = termState.commitPosition();
        checkLeaderChange();
    }

    private void checkLeaderChange()
    {
        if (termState.hasLeader())
        {
            final int sessionId = termState.leaderSessionId();
            leaderArchiver = archiver.session(sessionId);
            leaderArchiveReader = archiveReader.session(sessionId);
        }
        else
        {
            leaderArchiver = null;
            leaderArchiveReader = null;
        }
    }

    public Follower acknowledgementPublication(final RaftPublication acknowledgementPublication)
    {
        this.acknowledgementPublication = acknowledgementPublication;
        return this;
    }

    public Follower controlPublication(final RaftPublication controlPublication)
    {
        this.controlPublication = controlPublication;
        return this;
    }

    public Follower controlSubscription(final Subscription controlSubscription)
    {
        this.controlSubscription = controlSubscription;
        return this;
    }

    public Follower votedFor(final short votedFor)
    {
        this.votedFor = votedFor;
        return this;
    }

    public long commitPosition()
    {
        return commitPosition;
    }
}
