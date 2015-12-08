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

import uk.co.real_logic.aeron.Image;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.logbuffer.BlockHandler;
import uk.co.real_logic.aeron.logbuffer.FragmentHandler;
import uk.co.real_logic.aeron.logbuffer.Header;
import uk.co.real_logic.agrona.BitUtil;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.LangUtil;
import uk.co.real_logic.agrona.collections.IntHashSet;
import uk.co.real_logic.agrona.collections.Long2LongHashMap;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.engine.logger.ArchiveReader;
import uk.co.real_logic.fix_gateway.messages.AcknowledgementStatus;
import uk.co.real_logic.fix_gateway.messages.Vote;

import static uk.co.real_logic.aeron.logbuffer.FrameDescriptor.*;
import static uk.co.real_logic.aeron.protocol.DataHeaderFlyweight.HEADER_LENGTH;
import static uk.co.real_logic.fix_gateway.messages.AcknowledgementStatus.MISSING_LOG_ENTRIES;
import static uk.co.real_logic.fix_gateway.messages.AcknowledgementStatus.WRONG_TERM;

public class Leader implements Role, RaftHandler
{
    public static final int NO_SESSION_ID = -1;

    private final TermState termState;
    private final int ourSessionId;
    private final ArchiveReader archiveReader;
    private final short nodeId;
    private final AcknowledgementStrategy acknowledgementStrategy;
    private final RaftSubscriber raftSubscriber = new RaftSubscriber(this);
    private final RaftNode raftNode;
    private final FragmentHandler handler;
    private final long heartbeatIntervalInMs;

    // Counts of how many acknowledgements
    private final Long2LongHashMap nodeToPosition = new Long2LongHashMap(NO_SESSION_ID);
    private final ResendHandler resendHandler = new ResendHandler();

    private RaftPublication controlPublication;
    private Subscription acknowledgementSubscription;
    private Subscription dataSubscription;
    private Subscription controlSubscription;
    private Image leaderDataImage;
    private Fragmenter fragmenter;
    private long commitAndLastAppliedPosition = 0;
    private long nextHeartbeatTimeInMs;
    private int leaderShipTerm;
    private long timeInMs;

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
        final ArchiveReader archiveReader)
    {
        this.nodeId = nodeId;
        this.acknowledgementStrategy = acknowledgementStrategy;
        this.raftNode = raftNode;
        this.handler = handler;
        this.termState = termState;
        this.ourSessionId = ourSessionId;
        this.archiveReader = archiveReader;
        this.heartbeatIntervalInMs = heartbeatIntervalInMs;
        followers.forEach(follower -> nodeToPosition.put(follower, 0));
        updateHeartbeatInterval(timeInMs);
    }

    public int readData()
    {
        checkFragmenter();

        // TODO: archive the data, don't just put into handler
        // TODO: push archived log into handler
        // TODO: split commit and last applied positions
        final long newPosition = acknowledgementStrategy.findAckedTerm(nodeToPosition);
        final int delta = (int) (newPosition - commitAndLastAppliedPosition);
        if (delta > 0)
        {
            commitAndLastAppliedPosition += leaderDataImage.blockPoll(fragmenter, delta);
            heartbeat();

            return delta;
        }

        return 0;
    }

    private void checkFragmenter()
    {
        if (leaderDataImage == null)
        {
            leaderDataImage = dataSubscription.getImage(ourSessionId);
            if (fragmenter == null && leaderDataImage != null)
            {
                fragmenter = new Fragmenter(leaderDataImage);
            }
        }
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

        return acknowledgementSubscription.poll(raftSubscriber, fragmentLimit) +
               controlSubscription.poll(raftSubscriber, fragmentLimit);
    }

    public void closeStreams()
    {
        acknowledgementSubscription.close();
        dataSubscription.close();
    }

    private void heartbeat()
    {
        controlPublication.saveConcensusHeartbeat(nodeId, leaderShipTerm, commitAndLastAppliedPosition, ourSessionId);
        updateHeartbeatInterval(timeInMs);
    }

    public void updateHeartbeatInterval(final long timeInMs)
    {
        this.nextHeartbeatTimeInMs = timeInMs + heartbeatIntervalInMs;
    }

    public void onMessageAcknowledgement(
        final long position, final short nodeId, final AcknowledgementStatus status)
    {
        if (status != WRONG_TERM)
        {
            nodeToPosition.put(nodeId, position);
        }

        if (status == MISSING_LOG_ENTRIES)
        {
            final int length = (int) (commitAndLastAppliedPosition - position);
            resendHandler.position(position);
            if (!archiveReader.readBlock(ourSessionId, position, length, resendHandler))
            {
                // TODO: error
            }
        }
    }

    public void onRequestVote(final short candidateId, final int leaderShipTerm, final long lastAckedPosition)
    {
        if (this.leaderShipTerm < leaderShipTerm && lastAckedPosition >= commitAndLastAppliedPosition)
        {
            controlPublication.saveReplyVote(nodeId, candidateId, leaderShipTerm, Vote.FOR);

            termState.noLeader();

            transitionToFollower(leaderShipTerm, commitAndLastAppliedPosition, candidateId);
        }
        else
        {
            controlPublication.saveReplyVote(nodeId, candidateId, leaderShipTerm, Vote.AGAINST);
        }
    }

    public void onReplyVote(
        final short senderNodeId, final short candidateId, final int leaderShipTerm, final Vote vote)
    {
        // Ignore this message
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

    public void onConcensusHeartbeat(final short nodeId,
                                     final int leaderShipTerm,
                                     final long position,
                                     final int leaderSessionId)
    {
        if (nodeId != this.nodeId && leaderShipTerm > this.leaderShipTerm)
        {
            termState.leaderSessionId(leaderSessionId);

            transitionToFollower(leaderShipTerm, position, Follower.NO_ONE);
        }
    }

    private void transitionToFollower(final int leaderShipTerm, final long position, final short votedFor)
    {
        termState
            .leadershipTerm(leaderShipTerm)
            .commitPosition(position)
            .lastAppliedPosition(commitAndLastAppliedPosition)
            .receivedPosition(commitAndLastAppliedPosition);

        raftNode.transitionToFollower(this, votedFor, timeInMs);
    }

    public Leader getsElected(final long timeInMs)
    {
        this.timeInMs = timeInMs;

        leaderShipTerm = termState.leadershipTerm();
        commitAndLastAppliedPosition = termState.commitPosition();
        heartbeat();

        final int toBeCommitted = (int) (commitAndLastAppliedPosition - termState.lastAppliedPosition());
        if (toBeCommitted > 0)
        {
            checkFragmenter();

            leaderDataImage.blockPoll(fragmenter, toBeCommitted);
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

    private class ResendHandler implements BlockHandler
    {
        private long position;

        public void onBlock(
            final DirectBuffer buffer, final int offset, final int length, final int sessionId, final int termId)
        {
            controlPublication.saveResend(ourSessionId, leaderShipTerm, position, buffer, offset, length);
        }

        public void position(final long position)
        {
            this.position = position;
        }
    }

    private final class Fragmenter implements BlockHandler
    {
        private Header header;

        private Fragmenter(final Image image)
        {
            header = new Header(image.initialTermId(), image.termBufferLength());
        }

        public void onBlock(
            final DirectBuffer buffer,
            int termOffset,
            final int length,
            final int sessionId,
            final int termId)
        {
            final UnsafeBuffer termBuffer = (UnsafeBuffer) buffer;
            final int limit = Math.min(termOffset + length, termBuffer.capacity());

            try
            {
                do
                {
                    final int frameLength = frameLengthVolatile(termBuffer, termOffset);
                    if (frameLength <= 0)
                    {
                        break;
                    }

                    final int fragmentOffset = termOffset;
                    termOffset += BitUtil.align(frameLength, FRAME_ALIGNMENT);

                    if (!isPaddingFrame(termBuffer, fragmentOffset))
                    {
                        header.buffer(termBuffer);
                        header.offset(fragmentOffset);

                        handler.onFragment(termBuffer, fragmentOffset + HEADER_LENGTH, frameLength - HEADER_LENGTH, header);
                    }
                }
                while (termOffset < limit);
            }
            catch (final Exception ex)
            {
                LangUtil.rethrowUnchecked(ex);
            }
        }
    }
}
