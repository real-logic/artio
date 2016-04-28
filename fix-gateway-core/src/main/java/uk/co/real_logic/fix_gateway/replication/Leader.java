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

import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.logbuffer.BlockHandler;
import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.DirectBuffer;
import org.agrona.collections.IntHashSet;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.engine.logger.ArchiveReader;
import uk.co.real_logic.fix_gateway.engine.logger.Archiver;
import uk.co.real_logic.fix_gateway.engine.logger.Archiver.SessionArchiver;
import uk.co.real_logic.fix_gateway.messages.AcknowledgementStatus;
import uk.co.real_logic.fix_gateway.messages.Vote;

import static uk.co.real_logic.fix_gateway.messages.AcknowledgementStatus.MISSING_LOG_ENTRIES;
import static uk.co.real_logic.fix_gateway.messages.AcknowledgementStatus.OK;

public class Leader implements Role, RaftHandler
{
    private static final UnsafeBuffer EMPTY_BUFFER = new UnsafeBuffer(new byte[0]);

    private static final int NO_SESSION_ID = -1;

    private final TermState termState;
    private final int ourSessionId;
    private final short nodeId;
    private final AcknowledgementStrategy acknowledgementStrategy;
    private final RaftSubscription raftSubscription;
    private final RaftNode raftNode;
    private final FragmentHandler handler;
    private final long heartbeatIntervalInMs;
    private final ArchiveReader archiveReader;
    private final Archiver archiver;

    // Counts of how many acknowledgements
    private final Long2LongHashMap nodeToPosition = new Long2LongHashMap(NO_SESSION_ID);
    private final ResendHandler resendHandler = new ResendHandler();

    private ArchiveReader.SessionReader ourArchiveReader;
    private RaftPublication controlPublication;
    private Subscription acknowledgementSubscription;
    private Subscription dataSubscription;
    private Subscription controlSubscription;
    private Image leaderDataImage;
    private SessionArchiver ourArchiver;

    /**
     * Keeps track of the position of the local archiver, this may be ahead of the commit position to
     * enable resends on acknowledged initial sequences on the log.
     */
    private long archivedPosition = 0;
    /** Position in the log that has been committed to disk, commitPosition >= lastAppliedPosition */
    private long commitPosition = 0;
    // TODO: re-think invariants given initial state of lastAppliedPosition
    /** Position in the log that has been applied to the state machine*/
    private long lastAppliedPosition = DataHeaderFlyweight.HEADER_LENGTH;

    private long nextHeartbeatTimeInMs;
    private int leaderShipTerm;
    private long timeInMs;

    private long position;

    public Leader(
        final short nodeId,
        final AcknowledgementStrategy acknowledgementStrategy,
        final IntHashSet followers,
        final RaftNode raftNode,
        final FragmentHandler handler,
        final long timeInMs,
        final long heartbeatIntervalInMs,
        final TermState termState,
        final int ourSessionId,
        final ArchiveReader archiveReader,
        final Archiver archiver)
    {
        this.nodeId = nodeId;
        this.acknowledgementStrategy = acknowledgementStrategy;
        this.raftNode = raftNode;
        this.handler = handler;
        this.termState = termState;
        this.ourSessionId = ourSessionId;
        this.heartbeatIntervalInMs = heartbeatIntervalInMs;
        this.archiveReader = archiveReader;
        this.archiver = archiver;

        followers.forEach(follower -> nodeToPosition.put(follower, 0));
        updateHeartbeatInterval(timeInMs);
        raftSubscription = new RaftSubscription(DebugRaftHandler.wrap(nodeId, this));
    }

    public int readData()
    {
        setupArchival();
        if (canArchive())
        {
            archivedPosition += leaderDataImage.filePoll(ourArchiver, leaderDataImage.termBufferLength());
            nodeToPosition.put(nodeId, archivedPosition);
        }

        final long newPosition = acknowledgementStrategy.findAckedTerm(nodeToPosition);
        final int delta = (int) (newPosition - commitPosition);
        if (delta > 0)
        {
            commitPosition = newPosition;

            heartbeat();

            applyUnappliedFragments();

            return delta;
        }

        return 0;
    }

    private void applyUnappliedFragments()
    {
        final long readUpTo = archiveReader.readUpTo(
            ourSessionId, lastAppliedPosition, commitPosition, handler);
        if (readUpTo != ArchiveReader.UNKNOWN_SESSION)
        {
            lastAppliedPosition = readUpTo;
        }
    }

    private void setupArchival()
    {
        if (leaderDataImage == null)
        {
            leaderDataImage = dataSubscription.getImage(ourSessionId);
        }

        if (ourArchiver == null)
        {
            ourArchiver = archiver.session(ourSessionId);
        }
    }

    public boolean canArchive()
    {
        return leaderDataImage != null && ourArchiver != null;
    }

    public int checkConditions(final long timeInMs)
    {
        if (timeInMs > nextHeartbeatTimeInMs)
        {
            heartbeat();

            return 1;
        }

        return 0;
    }

    public int pollCommands(final int fragmentLimit, final long timeInMs)
    {
        this.timeInMs = timeInMs;

        return acknowledgementSubscription.controlledPoll(raftSubscription, fragmentLimit) +
               controlSubscription.controlledPoll(raftSubscription, fragmentLimit);
    }

    public void closeStreams()
    {
        acknowledgementSubscription.close();
        dataSubscription.close();
    }

    private void heartbeat()
    {
        controlPublication.saveConcensusHeartbeat(nodeId, leaderShipTerm, commitPosition, ourSessionId);
        updateHeartbeatInterval(timeInMs);
    }

    public void updateHeartbeatInterval(final long timeInMs)
    {
        this.nextHeartbeatTimeInMs = timeInMs + heartbeatIntervalInMs;
    }

    public Action onMessageAcknowledgement(
        final long position, final short nodeId, final AcknowledgementStatus status)
    {
        if (status == OK)
        {
            nodeToPosition.put(nodeId, position);
        }

        if (status == MISSING_LOG_ENTRIES)
        {
            final int length = (int) (archivedPosition - position);
            this.position = position;
            if (validateReader())
            {
                if (!ourArchiveReader.readBlock(position, length, resendHandler))
                {
                    saveResend(EMPTY_BUFFER, 0, 0);
                }
            }
            else
            {
                // TODO: decide if this is the right mechanic
                //saveResend(EMPTY_BUFFER, 0, 0);
            }
        }

        return Action.CONTINUE;
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
            if (this.leaderShipTerm < leaderShipTerm && lastAckedPosition >= commitPosition)
            {
                controlPublication.saveReplyVote(nodeId, candidateId, leaderShipTerm, Vote.FOR);

                termState.noLeader();

                transitionToFollower(leaderShipTerm, candidateId);
                return Action.BREAK;
            }
            else
            {
                controlPublication.saveReplyVote(nodeId, candidateId, leaderShipTerm, Vote.AGAINST);
            }
        }

        return Action.CONTINUE;
    }

    public Action onReplyVote(
        final short senderNodeId, final short candidateId, final int leaderShipTerm, final Vote vote)
    {
        // Ignore this message
        return Action.CONTINUE;
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
        return Action.CONTINUE;
    }

    public Action onConcensusHeartbeat(final short nodeId,
                                     final int leaderShipTerm,
                                     final long position,
                                     final int leaderSessionId)
    {
        if (nodeId != this.nodeId && leaderShipTerm > this.leaderShipTerm)
        {
            termState.leaderSessionId(leaderSessionId);

            transitionToFollower(leaderShipTerm, Follower.NO_ONE);

            return Action.BREAK;
        }

        return Action.CONTINUE;
    }

    private void transitionToFollower(final int leaderShipTerm, final short votedFor)
    {
        termState
            .leadershipTerm(leaderShipTerm)
            .commitPosition(commitPosition)
            .lastAppliedPosition(lastAppliedPosition)
            .receivedPosition(lastAppliedPosition);

        raftNode.transitionToFollower(this, votedFor, timeInMs);
    }

    public Leader getsElected(final long timeInMs)
    {
        this.timeInMs = timeInMs;

        leaderShipTerm = termState.leadershipTerm();
        commitPosition = termState.commitPosition();
        lastAppliedPosition = Math.max(DataHeaderFlyweight.HEADER_LENGTH, termState.lastAppliedPosition());
        heartbeat();

        if (commitPosition > lastAppliedPosition)
        {
            setupArchival();
            applyUnappliedFragments();
        }

        return this;
    }

    public Leader acknowledgementSubscription(final Subscription acknowledgementSubscription)
    {
        this.acknowledgementSubscription = acknowledgementSubscription;
        return this;
    }

    public Leader dataSubscription(final Subscription dataSubscription)
    {
        this.dataSubscription = dataSubscription;
        archiver.subscription(dataSubscription);
        return this;
    }

    public Leader controlPublication(final RaftPublication controlPublication)
    {
        this.controlPublication = controlPublication;
        return this;
    }

    public Leader controlSubscription(final Subscription controlSubscription)
    {
        this.controlSubscription = controlSubscription;
        return this;
    }

    public long commitPosition()
    {
        return commitPosition;
    }

    private class ResendHandler implements BlockHandler
    {
        public void onBlock(
            final DirectBuffer buffer, final int offset, final int length, final int sessionId, final int termId)
        {
            saveResend(buffer, offset, length);
        }
    }

    private void saveResend(final DirectBuffer buffer, final int offset, final int length)
    {
        controlPublication.saveResend(ourSessionId, leaderShipTerm, position, buffer, offset, length);
    }

}
