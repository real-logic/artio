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
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import io.aeron.logbuffer.Header;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.DirectBuffer;
import uk.co.real_logic.fix_gateway.DebugLogger;
import uk.co.real_logic.fix_gateway.dictionary.generation.Exceptions;
import uk.co.real_logic.fix_gateway.engine.logger.ArchiveReader;
import uk.co.real_logic.fix_gateway.engine.logger.ArchiveReader.SessionReader;
import uk.co.real_logic.fix_gateway.replication.messages.ConsensusHeartbeatDecoder;
import uk.co.real_logic.fix_gateway.replication.messages.MessageHeaderDecoder;
import uk.co.real_logic.fix_gateway.replication.messages.ResendDecoder;

import java.util.PriorityQueue;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.*;
import static java.util.Comparator.comparingLong;
import static uk.co.real_logic.fix_gateway.LogTag.RAFT;
import static uk.co.real_logic.fix_gateway.engine.logger.ArchiveDescriptor.alignTerm;
import static uk.co.real_logic.fix_gateway.replication.ClusterSubscription.Ternary.*;
import static uk.co.real_logic.fix_gateway.replication.ReservedValue.NO_FILTER;

/**
 * Not thread safe, create a new cluster subscription for each thread that wants to read
 * from the RAFT stream.
 */
class ClusterSubscription extends ClusterableSubscription
{
    private final MessageHeaderDecoder messageHeader = new MessageHeaderDecoder();
    private final ResendDecoder resend = new ResendDecoder();
    private final ConsensusHeartbeatDecoder consensusHeartbeat = new ConsensusHeartbeatDecoder();
    private final ControlledFragmentHandler onControlMessage = this::onControlMessage;
    private final ControlledFragmentHandler archiveHandler = this::onArchiveHandler;
    private final PriorityQueue<FutureAck> futureAcks = new PriorityQueue<>(
        comparingLong(FutureAck::startPosition));

    private final MessageFilter messageFilter;
    private final Subscription dataSubscription;
    private final Subscription controlSubscription;
    private final ArchiveReader archiveReader;
    private final ClusterHeader clusterHeader;

    private int currentLeadershipTerm = Integer.MIN_VALUE;
    private long lastAppliedStreamPosition;
    private long previousConsensusPosition;
    private Image dataImage;
    private ClusterFragmentHandler handler;
    private SessionReader leaderArchiveReader;
    // replicated position - transport/stream position
    private long positionDelta;

    ClusterSubscription(
        final Subscription dataSubscription,
        final int clusterStreamId,
        final Subscription controlSubscription,
        final ArchiveReader archiveReader)
    {
        this.controlSubscription = controlSubscription;
        this.archiveReader = archiveReader;
        // We use clusterStreamId as a reserved value filter
        if (clusterStreamId == NO_FILTER)
        {
            throw new IllegalArgumentException("ClusterStreamId must not be 0");
        }

        this.dataSubscription = dataSubscription;
        messageFilter = new MessageFilter(clusterStreamId);
        clusterHeader = new ClusterHeader(clusterStreamId);
    }

    public int poll(final ClusterFragmentHandler fragmentHandler, final int fragmentLimit)
    {
        this.handler = fragmentHandler;

        if (cannotAdvance())
        {
            final Ternary hasMatchingFutureAck = hasMatchingFutureAck();
            if (hasMatchingFutureAck == FAILED)
            {
                return 0;
            }

            if (hasMatchingFutureAck == FALSE)
            {
                controlSubscription.controlledPoll(onControlMessage, fragmentLimit);

                if (cannotAdvance())
                {
                    if (appliedBehindConcensus() && hasLeaderArchiveReader())
                    {
                        readFromLog();

                        return 1;
                    }

                    return 0;
                }
            }

            if (cannotAdvance() && appliedBehindConcensus() && hasLeaderArchiveReader())
            {
                readFromLog();
            }
        }

        return dataImage.controlledPoll(messageFilter, fragmentLimit);
    }

    private boolean appliedBehindConcensus()
    {
        return (messageFilter.streamConsensusPosition - lastAppliedStreamPosition) > 0;
    }

    private void readFromLog()
    {
        final long lastAppliedStreamPosition = this.lastAppliedStreamPosition;
        final long beginPosition = lastAppliedStreamPosition + DataHeaderFlyweight.HEADER_LENGTH;
        final long endPosition = messageFilter.streamConsensusPosition;

        final long readUpTo = leaderArchiveReader.readUpTo(
            beginPosition,
            endPosition,
            archiveHandler);

        if (readUpTo > 0)
        {
            this.lastAppliedStreamPosition = alignTerm(readUpTo);
        }

        DebugLogger.log(
            RAFT,
            "Subscription readFromLog(appliedStreamPos=%d, endPos=%d, appliedStreamPosAfter=%d)%n",
            lastAppliedStreamPosition,
            endPosition,
            this.lastAppliedStreamPosition);
    }

    private Action onArchiveHandler(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        clusterHeader.update(header.position(), header.sessionId());
        return handler.onFragment(buffer, offset, length, clusterHeader);
    }

    enum Ternary
    {
        TRUE,
        FALSE,
        FAILED
    }

    Ternary hasMatchingFutureAck()
    {
        final FutureAck ack = futureAcks.peek();
        if (ack != null && previousConsensusPosition == ack.startPosition)
        {
            futureAcks.poll();

            final boolean success = onSwitchTerms(
                ack.leaderShipTerm,
                ack.leaderSessionId,
                ack.startPosition,
                ack.streamStartPosition(),
                ack.streamPosition);

            return success ? TRUE : FAILED;
        }

        return FALSE;
    }

    @SuppressWarnings("FinalParameters")
    private Action onControlMessage(
        final DirectBuffer buffer,
        int offset,
        final int length,
        final Header header)
    {
        messageHeader.wrap(buffer, offset);
        final int actingBlockLength = messageHeader.blockLength();
        final int version = messageHeader.version();

        switch (messageHeader.templateId())
        {
            case ConsensusHeartbeatDecoder.TEMPLATE_ID:
            {
                offset += MessageHeaderDecoder.ENCODED_LENGTH;

                consensusHeartbeat.wrap(buffer, offset, actingBlockLength, version);

                final int leaderShipTerm = consensusHeartbeat.leaderShipTerm();
                final int leaderSessionId = consensusHeartbeat.leaderSessionId();
                final long position = consensusHeartbeat.position();
                final long streamStartPosition = consensusHeartbeat.streamStartPosition();
                final long streamPosition = consensusHeartbeat.streamPosition();

                return onConsensusHeartbeat(
                    leaderShipTerm, leaderSessionId, position, streamStartPosition, streamPosition);
            }

            case ResendDecoder.TEMPLATE_ID:
            {
                offset += MessageHeaderDecoder.ENCODED_LENGTH;

                resend.wrap(buffer, offset, actingBlockLength, version);

                onResend(
                    resend.leaderSessionId(),
                    resend.leaderShipTerm(),
                    resend.startPosition(),
                    resend.streamStartPosition(),
                    buffer,
                    RaftSubscription.bodyOffset(offset, actingBlockLength),
                    resend.bodyLength());
            }
        }

        return CONTINUE;
    }

    Action onConsensusHeartbeat(
        final int leaderShipTerm,
        final int leaderSessionId,
        final long position,
        final long streamStartPosition,
        final long streamPosition)
    {
        DebugLogger.log(
            RAFT,
            "Subscription Heartbeat(leaderShipTerm=%d, pos=%d, sStartPos=%d, sPos=%d, leaderSessId=%d)%n",
            leaderShipTerm,
            position,
            streamStartPosition,
            streamPosition,
            leaderSessionId);

        final long length = streamPosition - streamStartPosition;
        final long startPosition = position - length;

        if (leaderShipTerm == currentLeadershipTerm)
        {
            if (messageFilter.streamConsensusPosition < streamPosition)
            {
                messageFilter.streamConsensusPosition = streamPosition;
                previousConsensusPosition = alignTerm(position);

                return BREAK;
            }
        }
        else if (isNextLeadershipTerm(leaderShipTerm))
        {
            if (startPosition != previousConsensusPosition)
            {
                save(leaderShipTerm, leaderSessionId, startPosition, streamStartPosition, streamPosition);
            }
            else
            {
                final boolean success = onSwitchTerms(
                    leaderShipTerm, leaderSessionId, position, streamStartPosition, streamPosition);
                return success ? BREAK : ABORT;
            }
        }
        else if (leaderShipTerm > currentLeadershipTerm)
        {
            save(leaderShipTerm, leaderSessionId, startPosition, streamStartPosition, streamPosition);
        }

        // We deliberately ignore leaderShipTerm < currentLeadershipTerm, as they would be old
        // leader messages

        return CONTINUE;
    }

    private boolean isNextLeadershipTerm(final int leaderShipTermId)
    {
        return leaderShipTermId == currentLeadershipTerm + 1 || dataImage == null;
    }

    Action onResend(
        final int leaderSessionId,
        final int leaderShipTerm,
        final long startPosition,
        final long streamStartPosition,
        final DirectBuffer bodyBuffer,
        final int bodyOffset,
        final int bodyLength)
    {
        final long streamPosition = streamStartPosition + bodyLength;
        final long position = startPosition + bodyLength;

        Action action = CONTINUE;
        if (startPosition == previousConsensusPosition)
        {
            final boolean nextLeadershipTerm = isNextLeadershipTerm(leaderShipTerm);
            if (nextLeadershipTerm)
            {
                // THe new leader's image isn't available yet.
                if (!onSwitchTermUpdateSources(leaderSessionId))
                {
                    return ABORT;
                }
            }

            // If next chunk needed then just commit the thing immediately

            // TODO: correct session id
            clusterHeader.update(position, leaderSessionId);
            action = handler.onFragment(bodyBuffer, bodyOffset, bodyLength, clusterHeader);
            if (action == ABORT)
            {
                return action;
            }

            if (nextLeadershipTerm)
            {
                onSwitchTermUpdatePositions(leaderShipTerm, position, streamPosition, streamPosition);
            }
            else
            {
                lastAppliedStreamPosition = alignTerm(lastAppliedStreamPosition + bodyLength);
                previousConsensusPosition = alignTerm(previousConsensusPosition + bodyLength);
            }
        }
        else if (startPosition > previousConsensusPosition)
        {
            save(
                leaderShipTerm,
                leaderSessionId,
                startPosition,
                streamStartPosition,
                streamPosition);
        }

        return action;
    }

    private boolean onSwitchTerms(
        final int leaderShipTerm,
        final int leaderSessionId,
        final long position,
        final long streamConsumedPosition,
        final long streamPosition)
    {
        final boolean success = onSwitchTermUpdateSources(leaderSessionId);
        if (success)
        {
            onSwitchTermUpdatePositions(leaderShipTerm, position, streamConsumedPosition, streamPosition);
        }
        return success;
    }

    // Can be retried if update is aborted.
    private boolean onSwitchTermUpdateSources(final int leaderSessionId)
    {
        final Image newDataImage = dataSubscription.imageBySessionId(leaderSessionId);
        if (newDataImage == null)
        {
            return false;
        }

        dataImage = newDataImage;

        return true;
    }

    private boolean hasLeaderArchiveReader()
    {
        if (leaderArchiveReader == null)
        {
            leaderArchiveReader = archiveReader.session(dataImage.sessionId());
        }

        return leaderArchiveReader != null;
    }

    // Mutates state in a non-abortable way.
    private void onSwitchTermUpdatePositions(
        final int leaderShipTerm,
        final long position,
        final long streamConsumedPosition,
        final long streamPosition)
    {
        messageFilter.streamConsensusPosition = streamPosition;
        currentLeadershipTerm = leaderShipTerm;
        lastAppliedStreamPosition = alignTerm(streamConsumedPosition);
        previousConsensusPosition = alignTerm(position);
        positionDelta = position - streamConsumedPosition;
    }

    private void save(
        final int leaderShipTerm,
        final int leaderSessionId,
        final long startPosition,
        final long streamStartPosition,
        final long streamPosition)
    {
        futureAcks.add(new FutureAck(
            leaderShipTerm, leaderSessionId, streamStartPosition, streamPosition, startPosition));
    }

    private boolean cannotAdvance()
    {
        return dataImage == null || messageFilter.streamConsensusPosition <= dataImage.position();
    }

    public void close()
    {
        Exceptions.closeAll(dataSubscription, controlSubscription, archiveReader);
    }

    private final class MessageFilter implements ControlledFragmentHandler
    {
        private final int clusterStreamId;

        private long streamConsensusPosition;

        private MessageFilter(final int clusterStreamId)
        {
            this.clusterStreamId = clusterStreamId;
        }

        public Action onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
        {
            final int aeronSessionId = header.sessionId();
            final long headerPosition = header.position();
            final long fragmentStartPosition = headerPosition - length;
            final int clusterStreamId = ReservedValue.clusterStreamId(header);

            DebugLogger.log(
                RAFT,
                "Subscription onFragment(hdrPos=%d, csnsPos=%d, ourStream=%d, msgStream=%d, sessId=%d)%n",
                headerPosition,
                streamConsensusPosition,
                this.clusterStreamId,
                clusterStreamId,
                aeronSessionId);

            // We never have to deal with the case where a message fragment spans over the end of a leadership term
            // - they are aligned.

            // Consensus hasn't been reached for this message.
            if (headerPosition > streamConsensusPosition)
            {
                return ABORT;
            }

            // Skip data published on the leader's publication when they weren't a leader.
            if (fragmentStartPosition < lastAppliedStreamPosition)
            {
                return CONTINUE;
            }

            if (this.clusterStreamId == clusterStreamId)
            {
                messageHeader.wrap(buffer, offset);
                if (messageHeader.templateId() != ConsensusHeartbeatDecoder.TEMPLATE_ID)
                {
                    clusterHeader.update(headerPosition + positionDelta, header.sessionId());
                    final Action action = handler.onFragment(buffer, offset, length, clusterHeader);
                    if (action != ABORT)
                    {
                        lastAppliedStreamPosition = alignTerm(lastAppliedStreamPosition + length);
                    }
                    return action;
                }
            }

            return CONTINUE;
        }
    }

    private final class FutureAck
    {
        private final int leaderShipTerm;
        private final int leaderSessionId;
        private final long streamPosition;
        private final long startPosition;
        private final long streamStartPosition;

        private FutureAck(
            final int leaderShipTerm,
            final int leaderSessionId,
            final long streamStartPosition,
            final long streamPosition,
            final long startPosition)
        {
            this.leaderShipTerm = leaderShipTerm;
            this.leaderSessionId = leaderSessionId;
            this.streamStartPosition = streamStartPosition;
            this.streamPosition = streamPosition;
            this.startPosition = startPosition;
        }

        private long streamStartPosition()
        {
            return streamStartPosition;
        }

        public long startPosition()
        {
            return startPosition;
        }
    }

    long streamPosition()
    {
        return messageFilter.streamConsensusPosition;
    }

    public long positionOf(final int aeronSessionId)
    {
        return streamPosition();
    }

    int currentLeadershipTerm()
    {
        return currentLeadershipTerm;
    }
}
