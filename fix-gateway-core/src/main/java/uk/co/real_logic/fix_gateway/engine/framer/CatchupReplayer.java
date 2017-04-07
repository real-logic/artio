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
package uk.co.real_logic.fix_gateway.engine.framer;

import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import uk.co.real_logic.fix_gateway.DebugLogger;
import uk.co.real_logic.fix_gateway.builder.Encoder;
import uk.co.real_logic.fix_gateway.builder.HeaderEncoder;
import uk.co.real_logic.fix_gateway.builder.SequenceResetEncoder;
import uk.co.real_logic.fix_gateway.decoder.HeaderDecoder;
import uk.co.real_logic.fix_gateway.decoder.HeartbeatDecoder;
import uk.co.real_logic.fix_gateway.decoder.SequenceResetDecoder;
import uk.co.real_logic.fix_gateway.engine.PossDupEnabler;
import uk.co.real_logic.fix_gateway.engine.logger.ReplayQuery;
import uk.co.real_logic.fix_gateway.fields.UtcTimestampEncoder;
import uk.co.real_logic.fix_gateway.messages.FixMessageDecoder;
import uk.co.real_logic.fix_gateway.messages.FixMessageEncoder;
import uk.co.real_logic.fix_gateway.messages.MessageHeaderDecoder;
import uk.co.real_logic.fix_gateway.messages.MessageHeaderEncoder;
import uk.co.real_logic.fix_gateway.protocol.GatewayPublication;
import uk.co.real_logic.fix_gateway.util.AsciiBuffer;
import uk.co.real_logic.fix_gateway.util.MutableAsciiBuffer;

import static io.aeron.Publication.BACK_PRESSURED;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.ABORT;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;
import static uk.co.real_logic.fix_gateway.LogTag.CATCHUP;
import static uk.co.real_logic.fix_gateway.messages.MessageStatus.CATCHUP_REPLAY;
import static uk.co.real_logic.fix_gateway.messages.SessionReplyStatus.MISSING_MESSAGES;
import static uk.co.real_logic.fix_gateway.messages.SessionReplyStatus.OK;

public class CatchupReplayer implements ControlledFragmentHandler, Continuation
{
    private static final int ENCODE_BUFFER_SIZE = 8 * 1024;

    public static final int FRAME_LENGTH =
        MessageHeaderEncoder.ENCODED_LENGTH + FixMessageEncoder.BLOCK_LENGTH + FixMessageEncoder.bodyHeaderLength();

    private enum State
    {
        REPLAYING,
        SEND_MISSING,
        SEND_OK
    }

    private static final int OUT_OF_RANGE = -1;

    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final FixMessageDecoder messageDecoder = new FixMessageDecoder();
    private final FixMessageEncoder messageEncoder = new FixMessageEncoder();

    private final AsciiBuffer asciiBuffer = new MutableAsciiBuffer();
    private final HeaderDecoder headerDecoder = new HeaderDecoder();
    private final BufferClaim bufferClaim = new BufferClaim();

    private final PossDupEnabler possDupEnabler;
    private final ReplayQuery inboundMessages;
    private final GatewayPublication inboundPublication;
    private final ErrorHandler errorHandler;
    private final long correlationId;
    private final int libraryId;
    private final int lastReceivedSeqNum;
    private final int currentSequenceIndex;
    private final GatewaySession session;
    private final long catchupEndTimeInMs;

    private int replayFromSequenceNumber;
    private int replayFromSequenceIndex;
    private boolean abortedReplay;
    private State state = State.REPLAYING;

    private SequenceResetEncoder sequenceResetEncoder;
    private UtcTimestampEncoder timestampEncoder;
    private MutableAsciiBuffer encodeBuffer;

    private int heartbeatRangeSequenceNumberStart = OUT_OF_RANGE;

    CatchupReplayer(
        final ReplayQuery inboundMessages,
        final GatewayPublication inboundPublication,
        final ErrorHandler errorHandler,
        final long correlationId,
        final int libraryId,
        final int lastReceivedSeqNum,
        final int currentSequenceIndex,
        final int replayFromSequenceNumber,
        final int replayFromSequenceIndex,
        final GatewaySession session,
        final long catchupEndTimeInMs)
    {
        this.inboundMessages = inboundMessages;
        this.inboundPublication = inboundPublication;
        this.errorHandler = errorHandler;
        this.correlationId = correlationId;
        this.libraryId = libraryId;
        this.lastReceivedSeqNum = lastReceivedSeqNum;
        this.currentSequenceIndex = currentSequenceIndex;
        this.replayFromSequenceNumber = replayFromSequenceNumber;
        this.replayFromSequenceIndex = replayFromSequenceIndex;
        this.session = session;
        this.catchupEndTimeInMs = catchupEndTimeInMs;

        possDupEnabler = new PossDupEnabler(
            bufferClaim, this::claimBuffer, this::onPreCommit, this::onIllegalState, errorHandler);
    }

    private void onPreCommit()
    {
        final int frameOffset = bufferClaim.offset() + MessageHeaderEncoder.ENCODED_LENGTH;
        messageEncoder
            .wrap(bufferClaim.buffer(), frameOffset)
            .libraryId(libraryId)
            .status(CATCHUP_REPLAY);
    }

    private void onIllegalState(final String msg)
    {
        errorHandler.onError(new IllegalStateException(msg));
    }

    private boolean claimBuffer(final int length)
    {
        return inboundPublication.claim(length, bufferClaim) > 0;
    }

    public Action onFragment(
        final DirectBuffer srcBuffer,
        final int srcOffset,
        final int srcLength,
        final Header header)
    {
        final int messageLength = srcLength - FRAME_LENGTH;
        final int messageOffset = srcOffset + FRAME_LENGTH;

        messageHeaderDecoder.wrap(srcBuffer, srcOffset);

        messageDecoder.wrap(
            srcBuffer,
            srcOffset + MessageHeaderDecoder.ENCODED_LENGTH,
            messageHeaderDecoder.blockLength(),
            messageHeaderDecoder.version());

        asciiBuffer.wrap(srcBuffer, messageOffset, messageLength);
        headerDecoder.decode(asciiBuffer, 0, messageLength);

        if (messageDecoder.messageType() == HeartbeatDecoder.MESSAGE_TYPE)
        {
            if (heartbeatRangeSequenceNumberStart == OUT_OF_RANGE)
            {
                heartbeatRangeSequenceNumberStart = headerDecoder.msgSeqNum();
            }

            return CONTINUE;
        }
        else
        {
            if (heartbeatRangeSequenceNumberStart != OUT_OF_RANGE)
            {
                if (!sendGapFill())
                {
                    return ABORT;
                }
            }

            return processNormalMessage(srcBuffer, srcOffset, srcLength, messageLength, messageOffset);
        }
    }

    private boolean sendGapFill()
    {
        if (sequenceResetEncoder == null)
        {
            sequenceResetEncoder = new SequenceResetEncoder();
            timestampEncoder = new UtcTimestampEncoder();
            encodeBuffer = new MutableAsciiBuffer(new byte[ENCODE_BUFFER_SIZE]);
            sequenceResetEncoder.gapFillFlag(true);

            final HeaderEncoder header = sequenceResetEncoder.header();

            header.senderCompID(headerDecoder.senderCompID());
            if (headerDecoder.hasSenderSubID())
            {
                header.senderSubID(headerDecoder.senderSubID());
            }
            if (headerDecoder.hasSenderLocationID())
            {
                header.senderLocationID(headerDecoder.senderLocationID());
            }

            header.targetCompID(headerDecoder.targetCompID());
            if (headerDecoder.hasTargetSubID())
            {
                header.targetSubID(headerDecoder.targetSubID());
            }
            if (headerDecoder.hasTargetLocationID())
            {
                header.targetLocationID(headerDecoder.targetLocationID());
            }
        }

        final int heartbeatRangeSequenceNumberEnd = headerDecoder.msgSeqNum();

        sequenceResetEncoder.header().msgSeqNum(heartbeatRangeSequenceNumberStart);
        sequenceResetEncoder.newSeqNo(heartbeatRangeSequenceNumberEnd);
        sequenceResetEncoder.header().sendingTime(
            timestampEncoder.buffer(), timestampEncoder.encode(System.currentTimeMillis()));

        final long result = sequenceResetEncoder.encode(encodeBuffer, 0);
        final int encodedLength = Encoder.length(result);
        final int encodedOffset = Encoder.offset(result);
        final boolean sent = inboundPublication.saveMessage(
            encodeBuffer, encodedOffset, encodedLength,
            libraryId, SequenceResetDecoder.MESSAGE_TYPE,
            messageDecoder.session(), replayFromSequenceIndex, libraryId,
            CATCHUP_REPLAY) > 0;

        if (sent)
        {
            heartbeatRangeSequenceNumberStart = OUT_OF_RANGE;
        }

        return sent;
    }

    private Action processNormalMessage(
        final DirectBuffer srcBuffer,
        final int srcOffset,
        final int srcLength,
        final int messageLength,
        final int messageOffset)
    {
        final Action action = possDupEnabler.enablePossDupFlag(
            srcBuffer, messageOffset, messageLength, srcOffset, srcLength);
        if (action == CONTINUE)
        {
            // store the point to continue from if an abort happens.
            replayFromSequenceNumber = headerDecoder.msgSeqNum() + 1;
            replayFromSequenceIndex = messageDecoder.sequenceIndex();

            return CONTINUE;
        }
        else
        {
            abortedReplay = true;
            return ABORT;
        }
    }

    public long attempt()
    {
        DebugLogger.log(CATCHUP, "Attempt replay for %d\n", session.sessionId());
        switch (state)
        {
            case REPLAYING:
            {
                if (notLoggingInboundMessages())
                {
                    state = State.SEND_MISSING;
                    return sendMissingMessages();
                }

                // Know at this point that we've indexed up to the latest message.
                // adding 1 to convert to inclusive numbering
                abortedReplay = false;
                try
                {
                    DebugLogger.log(CATCHUP,
                        "Querying for %d, currently at (%d, %d)\n",
                        session.sessionId(), lastReceivedSeqNum, currentSequenceIndex);

                    inboundMessages.query(
                        this,
                        session.sessionId(),
                        replayFromSequenceNumber,
                        replayFromSequenceIndex,
                        lastReceivedSeqNum,
                        currentSequenceIndex);
                }
                catch (final IllegalStateException e)
                {
                    // Missing file, just retry the next time round.
                    abortedReplay = true;
                }

                if (abortedReplay || replayIncomplete())
                {
                    if (System.currentTimeMillis() > catchupEndTimeInMs)
                    {
                        state = State.SEND_MISSING;
                        return sendMissingMessages();
                    }
                    else
                    {
                        return BACK_PRESSURED;
                    }
                }
                else
                {
                    state = State.SEND_OK;
                    return sendOk(inboundPublication, correlationId, session);
                }
            }

            case SEND_MISSING:
            {
                return sendMissingMessages();
            }

            case SEND_OK:
            {
                return sendOk(inboundPublication, correlationId, session);
            }

            // Javac required fall-through case that should never be reached
            default:
            {
                return 1;
            }
        }
    }

    private boolean replayIncomplete()
    {
        return replayFromSequenceIndex < currentSequenceIndex
            || replayFromSequenceNumber < lastReceivedSeqNum;
    }

    private boolean notLoggingInboundMessages()
    {
        return inboundMessages == null;
    }

    private long sendOk(
        final GatewayPublication publication,
        final long correlationId,
        final GatewaySession session)
    {
        return sendOk(publication, correlationId, session, libraryId);
    }

    static long sendOk(
        final GatewayPublication publication,
        final long correlationId,
        final GatewaySession session,
        final int libraryId)
    {
        DebugLogger.log(CATCHUP, "OK for %d\n", session.sessionId());
        final long position = publication.saveRequestSessionReply(libraryId, OK, correlationId);
        if (position >= 0)
        {
            session.play();
        }
        return position;
    }

    private long sendMissingMessages()
    {
        DebugLogger.log(CATCHUP, "Missing Messages for %d\n", session.sessionId());
        final long position = inboundPublication.saveRequestSessionReply(libraryId, MISSING_MESSAGES, correlationId);
        if (position > 0)
        {
            errorHandler.onError(new IllegalStateException(String.format(
                "Failed to read correct number of messages for %d, finished at [%d, %d] instead of [%d, %d]",
                correlationId,
                replayFromSequenceIndex,
                replayFromSequenceNumber,
                currentSequenceIndex,
                lastReceivedSeqNum)));

            session.play();
        }
        return position;
    }
}
