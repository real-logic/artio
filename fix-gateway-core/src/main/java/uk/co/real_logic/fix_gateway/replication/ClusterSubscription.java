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
    private final PriorityQueue<FutureAck> futureAcks = new PriorityQueue<>(
        comparingLong(FutureAck::startPosition));

    private final MessageFilter messageFilter;
    private final Subscription dataSubscription;
    private final Subscription controlSubscription;
    private final ArchiveReader archiveReader;

    private Header resendHeader;
    private int currentLeadershipTermId = Integer.MIN_VALUE;
    private long streamConsumedPosition;
    private long previousAcknowledgedPosition;
    private Image dataImage;
    private ControlledFragmentHandler handler;
    private SessionReader leaderArchiveReader;

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
    }

    public int controlledPoll(final ControlledFragmentHandler fragmentHandler, final int fragmentLimit)
    {
        this.handler = fragmentHandler;

        if (cannotAdvance())
        {
            if (!hasMatchingFutureAck())
            {
                controlSubscription.controlledPoll(onControlMessage, fragmentLimit);

                if (cannotAdvance())
                {
                    return 0;
                }
            }
            else if (cannotAdvance() && leaderArchiveReader != null)
            {
                streamConsumedPosition = leaderArchiveReader.readUpTo(
                    streamConsumedPosition,
                    messageFilter.streamConsensusPosition,
                    handler);
            }
        }

        return dataImage.controlledPoll(messageFilter, fragmentLimit);
    }

    boolean hasMatchingFutureAck()
    {
        final FutureAck ack = futureAcks.peek();
        if (ack != null
            && previousAcknowledgedPosition == ack.startPosition)
        {
            futureAcks.poll();

            switchTerms(
                ack.leaderShipTermId, ack.leaderSessionId, ack.startPosition, ack.streamStartPosition(), ack.streamPosition);

            return true;
        }

        return false;
    }

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
        final int leaderShipTermId,
        final int leaderSessionId,
        final long position,
        final long streamStartPosition,
        final long streamPosition)
    {
        DebugLogger.log(
            RAFT,
            "Subscription Heartbeat(leaderShipTerm=%d, pos=%d, sStartPos=%d, sPos=%d, leaderSessId=%d)%n",
            leaderShipTermId,
            position,
            streamStartPosition,
            streamPosition,
            leaderSessionId);

        final long length = streamPosition - streamStartPosition;
        final long startPosition = position - length;

        if (leaderShipTermId == currentLeadershipTermId)
        {
            if (messageFilter.streamConsensusPosition < streamPosition)
            {
                messageFilter.streamConsensusPosition = streamPosition;
                previousAcknowledgedPosition = position;

                return BREAK;
            }
        }
        else if (isNextLeadershipTerm(leaderShipTermId))
        {
            if (startPosition != previousAcknowledgedPosition)
            {
                save(leaderShipTermId, leaderSessionId, startPosition, streamStartPosition, streamPosition);
            }
            else
            {
                switchTerms(leaderShipTermId, leaderSessionId, position, streamStartPosition, streamPosition);

                return BREAK;
            }
        }
        else if (leaderShipTermId > currentLeadershipTermId)
        {
            save(leaderShipTermId, leaderSessionId, startPosition, streamStartPosition, streamPosition);
        }

        // We deliberately ignore leaderShipTerm < currentLeadershipTermId, as they would be old
        // leader messages

        return CONTINUE;
    }

    private boolean isNextLeadershipTerm(final int leaderShipTermId)
    {
        return leaderShipTermId == currentLeadershipTermId + 1 || dataImage == null;
    }

    Action onResend(
        final int leaderSessionId,
        final int leaderShipTermId,
        final long startPosition,
        final long streamStartPosition,
        final DirectBuffer bodyBuffer,
        final int bodyOffset,
        final int bodyLength)
    {
        final long streamPosition = streamStartPosition + bodyLength;

        Action action = CONTINUE;
        if (startPosition == previousAcknowledgedPosition)
        {
            // If next chunk needed then just commit the thing immediately
            resendHeader.buffer(bodyBuffer);
            resendHeader.offset(bodyOffset);
            action = handler.onFragment(bodyBuffer, bodyOffset, bodyLength, resendHeader);
            if (action == ABORT)
            {
                return action;
            }

            if (isNextLeadershipTerm(leaderShipTermId))
            {
                final long position = startPosition + bodyLength;
                switchTerms(
                    leaderShipTermId,
                    leaderSessionId,
                    position,
                    streamPosition,
                    streamPosition);
            }
            else
            {
                streamConsumedPosition += bodyLength;
                previousAcknowledgedPosition += bodyLength;
            }
        }
        else if (startPosition > previousAcknowledgedPosition)
        {
            save(
                leaderShipTermId,
                leaderSessionId,
                startPosition,
                streamStartPosition,
                streamPosition);
        }

        return action;
    }

    private void switchTerms(
        final int leaderShipTermId,
        final int leaderSessionId,
        final long position,
        final long streamConsumedPosition,
        final long streamPosition)
    {
        dataImage = dataSubscription.imageBySessionId(leaderSessionId);
        resendHeader = new Header(dataImage.initialTermId(), dataImage.termBufferLength());
        leaderArchiveReader = archiveReader.session(leaderSessionId);

        messageFilter.streamConsensusPosition = streamPosition;
        currentLeadershipTermId = leaderShipTermId;
        this.streamConsumedPosition = streamConsumedPosition;
        previousAcknowledgedPosition = position;
    }

    private void save(
        final int leaderShipTermId,
        final int leaderSessionId,
        final long startPosition,
        final long streamStartPosition,
        final long streamPosition)
    {
        futureAcks.add(new FutureAck(
            leaderShipTermId, leaderSessionId, streamStartPosition, streamPosition, startPosition));
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
            final long headerPosition = header.position();
            final long fragmentStartPosition = headerPosition - length;
            final int clusterStreamId = ReservedValue.clusterStreamId(header);

            DebugLogger.log(
                RAFT,
                "Subscription onFragment(hdrPos=%d, csnsPos=%d, ourStream=%d, msgStream=%d)%n",
                headerPosition,
                streamConsensusPosition,
                this.clusterStreamId,
                clusterStreamId);

            // We never have to deal with the case where a message fragment spans over the end of a leadership term
            // - they are aligned.

            // Concensus hasn't been reached for this message.
            if (headerPosition > streamConsensusPosition)
            {
                return ABORT;
            }

            // Skip data published on the leader's publication when they weren't a leader.
            if (fragmentStartPosition < streamConsumedPosition)
            {
                return CONTINUE;
            }

            if (this.clusterStreamId == clusterStreamId)
            {
                messageHeader.wrap(buffer, offset);
                if (messageHeader.templateId() != ConsensusHeartbeatDecoder.TEMPLATE_ID)
                {
                    final Action action = handler.onFragment(buffer, offset, length, header);
                    streamConsumedPosition += length;
                    return action;
                }
            }

            return CONTINUE;
        }
    }

    private final class FutureAck
    {
        private final int leaderShipTermId;
        private final int leaderSessionId;
        private final long streamPosition;
        private final long startPosition;
        private final long streamStartPosition;

        private FutureAck(
            final int leaderShipTermId,
            final int leaderSessionId,
            final long streamStartPosition,
            final long streamPosition,
            final long startPosition)
        {
            this.leaderShipTermId = leaderShipTermId;
            this.leaderSessionId = leaderSessionId;
            this.streamStartPosition = streamStartPosition;
            this.streamPosition = streamPosition;
            this.startPosition = startPosition;
        }

        private int leaderShipTerm()
        {
            return leaderShipTermId;
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

    int currentLeadershipTermId()
    {
        return currentLeadershipTermId;
    }
}
