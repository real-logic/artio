/*
 * Copyright 2015-2017 Real Logic Ltd.
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
public class ClusterSubscription extends ClusterableSubscription
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
    private long lastAppliedTransportPosition;
    private long previousConsensusPosition;
    private Image dataImage;
    private ClusterFragmentHandler handler;
    private SessionReader leaderArchiveReader;
    // replicated position - transport/stream position
    // replicated - transport = delta
    // replicated = delta + transport
    // transport = replicated - delta
    private long positionDelta;

    private long transportConsensusPosition;

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
                    if (gapBeforeSubscription() && hasLeaderArchiveReader())
                    {
                        readFromLog();

                        return 1;
                    }

                    return 0;
                }
            }

            if (cannotAdvance() && gapBeforeSubscription() && hasLeaderArchiveReader())
            {
                readFromLog();
            }
        }

        messageFilter.messagesConsumed = 0;
        lastAppliedTransportPosition =
            dataImage.controlledPeek(lastAppliedTransportPosition, messageFilter, transportConsensusPosition);

        return messageFilter.messagesConsumed;
    }

    public long peek(
        final long initialPosition,
        final ClusterFragmentHandler fragmentHandler,
        final long limitPosition)
    {
        if (dataImage == null)
        {
            final Ternary hasMatchingFutureAck = hasMatchingFutureAck();
            if (hasMatchingFutureAck != TRUE)
            {
                return 0;
            }
        }

        this.handler = fragmentHandler;

        final long transportInitialPosition = initialPosition - positionDelta;
        final long transportLimitPosition = Math.max(limitPosition - positionDelta, transportConsensusPosition);

        validatePosition(transportInitialPosition);

        return dataImage.controlledPeek(transportInitialPosition, messageFilter, transportLimitPosition);
    }

    public void position(final long newPosition)
    {
        final long newTransportPosition = newPosition - positionDelta;

        validatePosition(newTransportPosition);

        lastAppliedTransportPosition = newTransportPosition;
    }

    private void validatePosition(final long newTransportPosition)
    {
        if (newTransportPosition < lastAppliedTransportPosition || newTransportPosition > transportConsensusPosition)
        {
            throw new IllegalArgumentException(
                "newPosition of " + newTransportPosition + " out of range " +
                lastAppliedTransportPosition + "-" + transportConsensusPosition);
        }
    }

    private boolean gapBeforeSubscription()
    {
        //return dataImage != null && lastAppliedTransportPosition < dataImage.position();
        // Disable reading from the archive until we evaluate the restart + long-netsplit recovery scenarios.
        return false;
    }

    private void readFromLog()
    {
        final long lastAppliedTransportPosition = this.lastAppliedTransportPosition;
        final long beginPosition = lastAppliedTransportPosition + DataHeaderFlyweight.HEADER_LENGTH;
        final long endPosition = transportConsensusPosition;

        final long readUpTo = leaderArchiveReader.readUpTo(
            beginPosition,
            endPosition,
            archiveHandler);

        if (readUpTo > 0)
        {
            this.lastAppliedTransportPosition = alignTerm(readUpTo);
        }

        DebugLogger.log(
            RAFT,
            "Subscription readFromLog(appliedTransPos=%d, endPos=%d, appliedTransPosAfter=%d)%n",
            lastAppliedTransportPosition,
            endPosition,
            this.lastAppliedTransportPosition);
    }

    private Action onArchiveHandler(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        clusterHeader.update(header.position(), header.sessionId(), header.flags());
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
                ack.transportStartPosition(),
                ack.transportPosition);

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
                final long transportStartPosition = consensusHeartbeat.transportStartPosition();
                final long transportPosition = consensusHeartbeat.transportPosition();

                return onConsensusHeartbeat(
                    leaderShipTerm, leaderSessionId, position, transportStartPosition, transportPosition);
            }

            case ResendDecoder.TEMPLATE_ID:
            {
                offset += MessageHeaderDecoder.ENCODED_LENGTH;

                resend.wrap(buffer, offset, actingBlockLength, version);

                onResend(
                    resend.leaderSessionId(),
                    resend.leaderShipTerm(),
                    resend.startPosition(),
                    resend.transportStartPosition(),
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
        final long transportStartPosition,
        final long transportPosition)
    {
        DebugLogger.log(
            RAFT,
            "Subscription Heartbeat(leaderShipTerm=%d, pos=%d, tStartPos=%d, tPos=%d, leaderSessId=%d)%n",
            leaderShipTerm,
            position,
            transportStartPosition,
            transportPosition,
            leaderSessionId);

        final long length = transportPosition - transportStartPosition;
        final long startPosition = position - length;

        if (leaderShipTerm == currentLeadershipTerm)
        {
            if (transportConsensusPosition < transportPosition)
            {
                transportConsensusPosition = transportPosition;
                previousConsensusPosition = alignTerm(position);

                return BREAK;
            }
        }
        else if (isNextLeadershipTerm(leaderShipTerm))
        {
            if (startPosition != previousConsensusPosition)
            {
                save(leaderShipTerm, leaderSessionId, startPosition, transportStartPosition, transportPosition);
            }
            else
            {
                final boolean success = onSwitchTerms(
                    leaderShipTerm, leaderSessionId, position, transportStartPosition, transportPosition);
                return success ? BREAK : ABORT;
            }
        }
        else if (leaderShipTerm > currentLeadershipTerm)
        {
            save(leaderShipTerm, leaderSessionId, startPosition, transportStartPosition, transportPosition);
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
        final long transportStartPosition,
        final DirectBuffer bodyBuffer,
        final int bodyOffset,
        final int bodyLength)
    {
        final long transportPosition = transportStartPosition + bodyLength;
        final long position = startPosition + bodyLength;

        Action action = CONTINUE;
        if (startPosition == previousConsensusPosition)
        {
            final boolean nextLeadershipTerm = isNextLeadershipTerm(leaderShipTerm);
            if (nextLeadershipTerm)
            {
                // The new leader's image isn't available yet.
                if (!onSwitchTermUpdateSources(leaderSessionId))
                {
                    return ABORT;
                }
            }

            // If next chunk needed then just commit the thing immediately

            // TODO: correct session id
            // TODO: correct header flags
            clusterHeader.update(position, leaderSessionId, (byte) 0);
            action = handler.onFragment(bodyBuffer, bodyOffset, bodyLength, clusterHeader);
            if (action == ABORT)
            {
                return action;
            }

            if (nextLeadershipTerm)
            {
                onSwitchTermUpdatePositions(leaderShipTerm, position, transportPosition, transportPosition);
            }
            else
            {
                lastAppliedTransportPosition = alignTerm(lastAppliedTransportPosition + bodyLength);
                previousConsensusPosition = alignTerm(previousConsensusPosition + bodyLength);
            }
        }
        else if (startPosition > previousConsensusPosition)
        {
            save(
                leaderShipTerm,
                leaderSessionId,
                startPosition,
                transportStartPosition,
                transportPosition);
        }

        return action;
    }

    private boolean onSwitchTerms(
        final int leaderShipTerm,
        final int leaderSessionId,
        final long position,
        final long transportConsumedPosition,
        final long transportPosition)
    {
        final boolean success = onSwitchTermUpdateSources(leaderSessionId);
        if (success)
        {
            onSwitchTermUpdatePositions(leaderShipTerm, position, transportConsumedPosition, transportPosition);
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
        final long transportConsumedPosition,
        final long transportPosition)
    {
        transportConsensusPosition = transportPosition;
        currentLeadershipTerm = leaderShipTerm;
        lastAppliedTransportPosition = alignTerm(transportConsumedPosition);
        previousConsensusPosition = alignTerm(position);
        positionDelta = position - transportConsumedPosition;
    }

    private void save(
        final int leaderShipTerm,
        final int leaderSessionId,
        final long startPosition,
        final long transportStartPosition,
        final long transportPosition)
    {
        futureAcks.add(new FutureAck(
            leaderShipTerm, leaderSessionId, transportStartPosition, transportPosition, startPosition));
    }

    private boolean cannotAdvance()
    {
        return dataImage == null || transportConsensusPosition <= dataImage.position();
    }

    public void close()
    {
        Exceptions.closeAll(dataSubscription, controlSubscription, archiveReader);
    }

    private final class MessageFilter implements ControlledFragmentHandler
    {
        private final int clusterStreamId;

        private int messagesConsumed;

        private MessageFilter(final int clusterStreamId)
        {
            this.clusterStreamId = clusterStreamId;
        }

        public Action onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
        {
            final int aeronSessionId = header.sessionId();
            final long headerPosition = header.position();
            final int clusterStreamId = ReservedValue.clusterStreamId(header);

            DebugLogger.log(
                RAFT,
                "Subscription onFragment(hdrPos=%d, csnsPos=%d, ourStream=%d, msgStream=%d, sessId=%d)%n",
                headerPosition,
                transportConsensusPosition,
                this.clusterStreamId,
                clusterStreamId,
                aeronSessionId);

            // We never have to deal with the case where a message fragment spans over the end of a leadership term
            // - they are aligned.

            if (this.clusterStreamId == clusterStreamId)
            {
                messageHeader.wrap(buffer, offset);
                if (messageHeader.templateId() != ConsensusHeartbeatDecoder.TEMPLATE_ID)
                {
                    clusterHeader.update(headerPosition + positionDelta, header.sessionId(), header.flags());
                    final Action action = handler.onFragment(buffer, offset, length, clusterHeader);
                    if (action != ABORT)
                    {
                        lastAppliedTransportPosition = alignTerm(lastAppliedTransportPosition + length);
                        messagesConsumed++;
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
        private final long transportPosition;
        private final long startPosition;
        private final long transportStartPosition;

        private FutureAck(
            final int leaderShipTerm,
            final int leaderSessionId,
            final long transportStartPosition,
            final long transportPosition,
            final long startPosition)
        {
            this.leaderShipTerm = leaderShipTerm;
            this.leaderSessionId = leaderSessionId;
            this.transportStartPosition = transportStartPosition;
            this.transportPosition = transportPosition;
            this.startPosition = startPosition;
        }

        private long transportStartPosition()
        {
            return transportStartPosition;
        }

        public long startPosition()
        {
            return startPosition;
        }
    }

    long transportPosition()
    {
        return transportConsensusPosition;
    }

    int currentLeadershipTerm()
    {
        return currentLeadershipTerm;
    }

    public long position()
    {
        return lastAppliedTransportPosition + positionDelta;
    }
}
