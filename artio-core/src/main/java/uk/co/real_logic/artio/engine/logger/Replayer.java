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
package uk.co.real_logic.artio.engine.logger;

import io.aeron.ControlledFragmentAssembler;
import io.aeron.ExclusivePublication;
import io.aeron.Subscription;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.ExclusiveBufferClaim;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.IntHashSet;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.artio.Pressure;
import uk.co.real_logic.artio.builder.Encoder;
import uk.co.real_logic.artio.decoder.HeaderDecoder;
import uk.co.real_logic.artio.decoder.ResendRequestDecoder;
import uk.co.real_logic.artio.decoder.SequenceResetDecoder;
import uk.co.real_logic.artio.dictionary.generation.GenerationUtil;
import uk.co.real_logic.artio.engine.PossDupEnabler;
import uk.co.real_logic.artio.engine.ReplayHandler;
import uk.co.real_logic.artio.engine.SenderSequenceNumbers;
import uk.co.real_logic.artio.messages.*;
import uk.co.real_logic.artio.protocol.ProtocolHandler;
import uk.co.real_logic.artio.protocol.ProtocolSubscription;
import uk.co.real_logic.artio.util.AsciiBuffer;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

import java.util.Set;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.ABORT;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;
import static uk.co.real_logic.artio.engine.FixEngine.ENGINE_LIBRARY_ID;
import static uk.co.real_logic.artio.messages.MessageStatus.OK;

/**
 * The replayer responds to resend requests with data from the log of sent messages.
 *
 * This agent subscribes to the stream of incoming fix data messages. It parses
 * Resend Request messages and searches the log, using the replay index to find
 * relevant messages to resend.
 */
public class Replayer implements ProtocolHandler, ControlledFragmentHandler, Agent
{
    static final int MESSAGE_FRAME_BLOCK_LENGTH =
        MessageHeaderDecoder.ENCODED_LENGTH + FixMessageDecoder.BLOCK_LENGTH + FixMessageDecoder.bodyHeaderLength();
    static final int SIZE_OF_LENGTH_FIELD = 2;
    static final int MOST_RECENT_MESSAGE = 0;
    private static final int POLL_LIMIT = 10;

    private static final int NONE = -1;

    private final ResendRequestDecoder resendRequest = new ResendRequestDecoder();
    private final MessageHeaderDecoder messageHeader = new MessageHeaderDecoder();
    private final FixMessageDecoder fixMessage = new FixMessageDecoder();
    private final HeaderDecoder fixHeader = new HeaderDecoder();
    private final GapFillEncoder gapFillEncoder = new GapFillEncoder();

    private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
    private final FixMessageEncoder fixMessageEncoder = new FixMessageEncoder();

    // Used in onMessage and onFragment
    private final AsciiBuffer asciiBuffer = new MutableAsciiBuffer();

    private final ExclusiveBufferClaim bufferClaim;
    private final PossDupEnabler possDupEnabler;
    private final ProtocolSubscription protocolSubscription = ProtocolSubscription.of(this);
    private final ControlledFragmentAssembler assembler = new ControlledFragmentAssembler(this);

    private final ReplayQuery replayQuery;
    private final ExclusivePublication publication;
    private final IdleStrategy idleStrategy;
    private final ErrorHandler errorHandler;
    private final int maxClaimAttempts;
    private final Subscription subscription;
    private final String agentNamePrefix;
    private final IntHashSet gapFillMessageTypes;
    private final ReplayHandler replayHandler;
    private final SenderSequenceNumbers senderSequenceNumbers;

    private int currentMessageOffset;
    private int currentMessageLength;

    private int beginGapFillSeqNum = NONE;
    private int lastSeqNo = NONE;
    private long connectionId;
    private long sessionId;
    private int sequenceIndex;
    private boolean backpressured;

    public Replayer(
        final ReplayQuery replayQuery,
        final ExclusivePublication publication,
        final ExclusiveBufferClaim bufferClaim,
        final IdleStrategy idleStrategy,
        final ErrorHandler errorHandler,
        final int maxClaimAttempts,
        final Subscription subscription,
        final String agentNamePrefix,
        final EpochClock clock,
        final Set<String> gapfillOnReplayMessageTypes,
        final ReplayHandler replayHandler,
        final SenderSequenceNumbers senderSequenceNumbers)
    {
        this.replayQuery = replayQuery;
        this.publication = publication;
        this.bufferClaim = bufferClaim;
        this.idleStrategy = idleStrategy;
        this.errorHandler = errorHandler;
        this.maxClaimAttempts = maxClaimAttempts;
        this.subscription = subscription;
        this.agentNamePrefix = agentNamePrefix;
        this.replayHandler = replayHandler;
        this.senderSequenceNumbers = senderSequenceNumbers;

        possDupEnabler = new PossDupEnabler(
            bufferClaim,
            this::claimBuffer,
            this::onPreCommit,
            this::onIllegalState,
            this::onException,
            clock,
            publication.maxPayloadLength());

        gapFillMessageTypes = new IntHashSet();
        gapfillOnReplayMessageTypes.forEach(messageTypeAsString ->
            gapFillMessageTypes.add(GenerationUtil.packMessageType(messageTypeAsString)));
    }

    private void onPreCommit(final MutableDirectBuffer buffer, final int offset)
    {
        final int frameOffset = offset + MessageHeaderEncoder.ENCODED_LENGTH;
        fixMessageEncoder
            .wrap(buffer, frameOffset)
            .connection(connectionId);
    }

    public Action onMessage(
        final DirectBuffer srcBuffer,
        final int srcOffset,
        final int length,
        final int libraryId,
        final long connectionId,
        final long sessionId,
        final int sequenceIndex,
        final int messageType,
        final long timestamp,
        final MessageStatus status,
        final int sequenceNumber,
        final long position)
    {
        if (messageType == ResendRequestDecoder.MESSAGE_TYPE && status == OK)
        {
            final int limit = Math.min(length, srcBuffer.capacity() - srcOffset);

            asciiBuffer.wrap(srcBuffer);
            currentMessageOffset = srcOffset;
            currentMessageLength = limit;

            resendRequest.reset();
            resendRequest.decode(asciiBuffer, srcOffset, limit);

            final int beginSeqNo;
            if (backpressured)
            {
                if (beginGapFillSeqNum != NONE)
                {
                    beginSeqNo = beginGapFillSeqNum;
                }
                else
                {
                    beginSeqNo = lastSeqNo + 1;
                }
            }
            else
            {
                beginSeqNo = resendRequest.beginSeqNo();
            }

            final int endSeqNo = resendRequest.endSeqNo();
            final boolean replayUpToMostRecent = endSeqNo == MOST_RECENT_MESSAGE;
            // Validate endSeqNo
            if (!replayUpToMostRecent && endSeqNo < beginSeqNo)
            {
                onIllegalState(
                    "[%s] Error in resend request, endSeqNo (%d) < beginSeqNo (%d)",
                    message(), endSeqNo, beginSeqNo);
                return CONTINUE;
            }

            this.connectionId = connectionId;
            this.sessionId = sessionId;
            this.sequenceIndex = sequenceIndex;
            this.lastSeqNo = beginSeqNo - 1;

            backpressured = false;
            final int count = replayQuery.query(
                assembler,
                sessionId,
                beginSeqNo,
                sequenceIndex,
                endSeqNo,
                sequenceIndex);

            if (backpressured)
            {
                return ABORT;
            }

            // If the last N messages were admin messages then we need to send a gapfill
            // after the replay query has run.
            if (beginGapFillSeqNum != NONE)
            {
                final int newSequenceNumber =
                    replayUpToMostRecent ? newSeqNo(connectionId) : endSeqNo + 1;
                final Action action = sendGapFill(beginGapFillSeqNum, newSequenceNumber);
                if (action == ABORT)
                {
                    backpressured = true;
                    return action;
                }
            }

            // Validate that we've replayed the correct number of messages.
            // If we have missing messages for some reason then just gap fill them.
            if (!replayUpToMostRecent)
            {
                final int expectedCount = endSeqNo - beginSeqNo + 1;
                if (count != expectedCount)
                {
                    if (count == 0)
                    {
                        final Action action = sendGapFill(beginSeqNo, endSeqNo + 1);
                        if (action == ABORT)
                        {
                            return action;
                        }
                    }

                    onIllegalState(
                        "[%s] Error in resend request, count(%d) < expectedCount (%d)",
                        message(), count, expectedCount);
                }
            }
        }

        return CONTINUE;
    }

    private int newSeqNo(final long connectionId)
    {
        return senderSequenceNumbers.lastSentSequenceNumber(connectionId) + 1;
    }

    // Callback for the ReplayQuery:
    public Action onFragment(
        final DirectBuffer srcBuffer, final int srcOffset, final int srcLength, final Header header)
    {
        messageHeader.wrap(srcBuffer, srcOffset);
        final int actingBlockLength = messageHeader.blockLength();
        final int offset = srcOffset + MessageHeaderDecoder.ENCODED_LENGTH;

        fixMessage.wrap(
            srcBuffer,
            offset,
            actingBlockLength,
            messageHeader.version());

        final int messageOffset = srcOffset + MESSAGE_FRAME_BLOCK_LENGTH;
        final int messageLength = srcLength - MESSAGE_FRAME_BLOCK_LENGTH;

        asciiBuffer.wrap(srcBuffer);
        fixHeader.decode(asciiBuffer, messageOffset, messageLength);
        final int msgSeqNum = fixHeader.msgSeqNum();
        final int messageType = fixMessage.messageType();

        replayHandler.onReplayedMessage(
            asciiBuffer,
            messageOffset,
            messageLength,
            fixMessage.libraryId(),
            fixMessage.session(),
            fixMessage.sequenceIndex(),
            messageType);

        if (gapFillMessageTypes.contains(messageType))
        {
            if (beginGapFillSeqNum == NONE)
            {
                beginGapFillSeqNum = lastSeqNo + 1;
            }

            lastSeqNo = msgSeqNum;
            return CONTINUE;
        }
        else
        {
            if (beginGapFillSeqNum != NONE)
            {
                sendGapFill(beginGapFillSeqNum, msgSeqNum + 1);
            }
            else if (msgSeqNum > lastSeqNo + 1)
            {
                sendGapFill(lastSeqNo, msgSeqNum + 1);
            }

            final Action action = possDupEnabler.enablePossDupFlag(
                srcBuffer, messageOffset, messageLength, srcOffset, srcLength);
            if (action == ABORT)
            {
                backpressured = true;
            }
            else
            {
                lastSeqNo = msgSeqNum;
            }

            return action;
        }
    }

    private Action sendGapFill(final int msgSeqNo, final int newSeqNo)
    {
        final long result = gapFillEncoder.encode(resendRequest.header(), msgSeqNo, newSeqNo);
        final int gapFillLength = Encoder.length(result);
        final int gapFillOffset = Encoder.offset(result);

        if (claimBuffer(MESSAGE_FRAME_BLOCK_LENGTH + gapFillLength))
        {
            final int destOffset = bufferClaim.offset();
            final MutableDirectBuffer destBuffer = bufferClaim.buffer();

            fixMessageEncoder
                .wrapAndApplyHeader(destBuffer, destOffset, messageHeaderEncoder)
                .libraryId(ENGINE_LIBRARY_ID)
                .messageType(SequenceResetDecoder.MESSAGE_TYPE)
                .session(this.sessionId)
                .sequenceIndex(this.sequenceIndex)
                .connection(this.connectionId)
                .timestamp(0)
                .status(MessageStatus.OK)
                .putBody(gapFillEncoder.buffer(), gapFillOffset, gapFillLength);

            bufferClaim.commit();

            this.beginGapFillSeqNum = NONE;

            return CONTINUE;
        }
        else
        {
            return ABORT;
        }
    }

    public Action onDisconnect(final int libraryId, final long connectionId, final DisconnectReason reason)
    {
        return CONTINUE;
    }

    private void onException(final Throwable e)
    {
        final String message = String.format("[%s] Error replying to message", message());
        errorHandler.onError(new IllegalArgumentException(message, e));
    }

    private void onIllegalState(final String message, final Object... arguments)
    {
        errorHandler.onError(new IllegalStateException(String.format(message, arguments)));
    }

    private String message()
    {
        return asciiBuffer.getAscii(currentMessageOffset, currentMessageLength);
    }

    private boolean claimBuffer(final int newLength)
    {
        for (int i = 0; i < maxClaimAttempts; i++)
        {
            final long position = publication.tryClaim(newLength, bufferClaim);
            if (position > 0)
            {
                idleStrategy.reset();
                return true;
            }
            else if (Pressure.isBackPressured(position))
            {
                idleStrategy.idle();
            }
            else
            {
                return false;
            }
        }

        return false;
    }

    public int doWork()
    {
        return senderSequenceNumbers.poll() + subscription.controlledPoll(protocolSubscription, POLL_LIMIT);
    }

    public void onClose()
    {
        publication.close();
        replayQuery.close();
    }

    public String roleName()
    {
        return agentNamePrefix + "Replayer";
    }
}
