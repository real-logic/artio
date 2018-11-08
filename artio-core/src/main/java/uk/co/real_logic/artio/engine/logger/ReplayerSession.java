/*
 * Copyright 2015-2018 Real Logic Ltd, Adaptive Financial Consulting Ltd.
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

import io.aeron.ExclusivePublication;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.ExclusiveBufferClaim;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.IntHashSet;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.artio.Pressure;
import uk.co.real_logic.artio.builder.Encoder;
import uk.co.real_logic.artio.decoder.HeaderDecoder;
import uk.co.real_logic.artio.decoder.SequenceResetDecoder;
import uk.co.real_logic.artio.engine.PossDupEnabler;
import uk.co.real_logic.artio.engine.ReplayHandler;
import uk.co.real_logic.artio.engine.SenderSequenceNumbers;
import uk.co.real_logic.artio.messages.*;
import uk.co.real_logic.artio.util.AsciiBuffer;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.ABORT;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;
import static uk.co.real_logic.artio.engine.FixEngine.ENGINE_LIBRARY_ID;
import static uk.co.real_logic.artio.engine.logger.Replayer.MESSAGE_FRAME_BLOCK_LENGTH;

class ReplayerSession implements ControlledFragmentHandler
{
    private static final int NONE = -1;

    private final FixMessageEncoder fixMessageEncoder = new FixMessageEncoder();
    private final MessageHeaderDecoder messageHeader = new MessageHeaderDecoder();
    private final FixMessageDecoder fixMessage = new FixMessageDecoder();
    private final HeaderDecoder fixHeader = new HeaderDecoder();
    private final GapFillEncoder gapFillEncoder = new GapFillEncoder();
    private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
    private final AsciiBuffer asciiBuffer = new MutableAsciiBuffer();

    private final ExclusiveBufferClaim bufferClaim;
    private final PossDupEnabler possDupEnabler;
    private final String message;
    private final IdleStrategy idleStrategy;
    private final ReplayHandler replayHandler;
    private final int maxClaimAttempts;
    private final IntHashSet gapFillMessageTypes;
    private final SenderSequenceNumbers senderSequenceNumbers;
    private final ExclusivePublication publication;
    private final ReplayQuery replayQuery;
    private final ErrorHandler errorHandler;

    private int beginSeqNo;
    private int endSeqNo;
    private boolean upToMostRecent;
    private long connectionId;
    private long sessionId;
    private int sequenceIndex;
    private int lastSeqNo;

    private int beginGapFillSeqNum = NONE;

    private ReplayOperation currentReplayOperation;

    ReplayerSession(
        final ExclusiveBufferClaim bufferClaim,
        final IdleStrategy idleStrategy,
        final ReplayHandler replayHandler,
        final int maxClaimAttempts,
        final IntHashSet gapFillMessageTypes,
        final SenderSequenceNumbers senderSequenceNumbers,
        final ExclusivePublication publication, final EpochClock clock, final int beginSeqNo,
        final int endSeqNo,
        final boolean upToMostRecent,
        final long connectionId,
        final long sessionId,
        final int sequenceIndex,
        final ReplayQuery replayQuery,
        final String message,
        final ErrorHandler errorHandler,
        final HeaderDecoder requestHeader)
    {
        this.bufferClaim = bufferClaim;
        this.idleStrategy = idleStrategy;
        this.replayHandler = replayHandler;
        this.maxClaimAttempts = maxClaimAttempts;
        this.gapFillMessageTypes = gapFillMessageTypes;
        this.senderSequenceNumbers = senderSequenceNumbers;
        this.publication = publication;
        this.beginSeqNo = beginSeqNo;
        this.endSeqNo = endSeqNo;
        this.upToMostRecent = upToMostRecent;
        this.connectionId = connectionId;
        this.sessionId = sessionId;
        this.sequenceIndex = sequenceIndex;
        this.message = message;
        this.errorHandler = errorHandler;
        this.replayQuery = replayQuery;

        lastSeqNo = beginSeqNo - 1;

        gapFillEncoder.setupMessage(requestHeader);

        possDupEnabler = new PossDupEnabler(
            bufferClaim,
            this::claimBuffer,
            this::onPreCommit,
            this::onIllegalState,
            this::onException,
            clock,
            publication.maxPayloadLength());
    }

    private void onPreCommit(final MutableDirectBuffer buffer, final int offset)
    {
        final int frameOffset = offset + MessageHeaderEncoder.ENCODED_LENGTH;
        fixMessageEncoder
            .wrap(buffer, frameOffset)
            .connection(connectionId);
    }

    private void onException(final Throwable e)
    {
        final String exMessage = String.format("[%s] Error replying to message", message);
        errorHandler.onError(new IllegalArgumentException(exMessage, e));
    }

    private void onIllegalState(final String message, final Object... arguments)
    {
        errorHandler.onError(new IllegalStateException(String.format(message, arguments)));
    }

    void query()
    {
        currentReplayOperation = replayQuery.query(
            this,
            sessionId,
            beginSeqNo,
            sequenceIndex,
            endSeqNo,
            sequenceIndex);
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
            if (action != ABORT)
            {
                lastSeqNo = msgSeqNum;
            }

            return action;
        }
    }

    private Action sendGapFill(final int msgSeqNo, final int newSeqNo)
    {
        final long result = gapFillEncoder.encode(msgSeqNo, newSeqNo);
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

    boolean attempCurrentReplayOperation()
    {
        return currentReplayOperation.attemptReplay() && completeReplay();
    }

    private boolean completeReplay()
    {
        // Load state needed to complete the replay
        final int replayedMessages = currentReplayOperation.replayedMessages();

        // If the last N messages were admin messages then we need to send a gapfill
        // after the replay query has run.
        if (beginGapFillSeqNum != NONE)
        {
            final int newSequenceNumber =
                upToMostRecent ? newSeqNo(connectionId) : endSeqNo + 1;
            final Action action = sendGapFill(beginGapFillSeqNum, newSequenceNumber);
            return action != ABORT;
        }
        else
        {
            // Validate that we've replayed the correct number of messages.
            // If we have missing messages for some reason then just gap fill them.
            if (!upToMostRecent)
            {
                final int expectedCount = endSeqNo - beginSeqNo + 1;
                if (replayedMessages != expectedCount)
                {
                    if (replayedMessages == 0)
                    {
                        final Action action = sendGapFill(beginSeqNo, endSeqNo + 1);
                        if (action == ABORT)
                        {
                            return false;
                        }
                    }

                    onIllegalState(
                        "[%s] Error in resend request, count(%d) < expectedCount (%d)",
                        message, replayedMessages, expectedCount);
                }
            }
        }

        return true;
    }

    private int newSeqNo(final long connectionId)
    {
        return senderSequenceNumbers.lastSentSequenceNumber(connectionId) + 1;
    }

}
