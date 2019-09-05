/*
 * Copyright 2015-2019 Real Logic Ltd, Adaptive Financial Consulting Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.co.real_logic.artio.engine.framer;

import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.EpochClock;
import uk.co.real_logic.artio.DebugLogger;
import uk.co.real_logic.artio.builder.AbstractSequenceResetEncoder;
import uk.co.real_logic.artio.builder.Encoder;
import uk.co.real_logic.artio.builder.SessionHeaderEncoder;
import uk.co.real_logic.artio.decoder.SessionHeaderDecoder;
import uk.co.real_logic.artio.engine.PossDupEnabler;
import uk.co.real_logic.artio.engine.logger.ReplayOperation;
import uk.co.real_logic.artio.engine.logger.ReplayQuery;
import uk.co.real_logic.artio.engine.logger.SequenceNumberIndexReader;
import uk.co.real_logic.artio.fields.UtcTimestampEncoder;
import uk.co.real_logic.artio.messages.FixMessageDecoder;
import uk.co.real_logic.artio.messages.FixMessageEncoder;
import uk.co.real_logic.artio.messages.MessageHeaderDecoder;
import uk.co.real_logic.artio.messages.MessageHeaderEncoder;
import uk.co.real_logic.artio.protocol.GatewayPublication;
import uk.co.real_logic.artio.util.AsciiBuffer;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

import static io.aeron.Publication.BACK_PRESSURED;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.ABORT;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;
import static uk.co.real_logic.artio.LogTag.CATCHUP;
import static uk.co.real_logic.artio.dictionary.SessionConstants.HEARTBEAT_MESSAGE_TYPE;
import static uk.co.real_logic.artio.dictionary.SessionConstants.SEQUENCE_RESET_MESSAGE_TYPE;
import static uk.co.real_logic.artio.messages.MessageStatus.CATCHUP_REPLAY;
import static uk.co.real_logic.artio.messages.SessionReplyStatus.MISSING_MESSAGES;
import static uk.co.real_logic.artio.messages.SessionReplyStatus.OK;

public class CatchupReplayer implements ControlledFragmentHandler, Continuation
{
    private static final int ENCODE_BUFFER_SIZE = 8 * 1024;

    public static final int FRAME_LENGTH =
        MessageHeaderEncoder.ENCODED_LENGTH + FixMessageEncoder.BLOCK_LENGTH + FixMessageEncoder.bodyHeaderLength();

    private enum State
    {
        AWAITING_INDEX,
        REPLAY_QUERY,
        REPLAYING,
        SEND_MISSING,
        SEND_OK
    }

    private static final int OUT_OF_RANGE = -1;

    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final FixMessageDecoder messageDecoder = new FixMessageDecoder();
    private final FixMessageEncoder messageEncoder = new FixMessageEncoder();

    private final AsciiBuffer asciiBuffer = new MutableAsciiBuffer();
    private final BufferClaim bufferClaim = new BufferClaim();

    private final PossDupEnabler possDupEnabler;
    private final SequenceNumberIndexReader receivedSequenceNumberIndex;
    private final ReplayQuery inboundMessages;
    private final GatewayPublication inboundPublication;
    private final ErrorHandler errorHandler;
    private final long correlationId;
    private final long connectionId;
    private final int libraryId;
    private final int lastReceivedSeqNum;
    private final int currentSequenceIndex;
    private final GatewaySession session;
    private final long catchupEndTimeInMs;
    private final long requiredPosition;
    private final SessionHeaderDecoder headerDecoder;

    private int replayFromSequenceNumber;
    private int replayFromSequenceIndex;
    private State state = State.AWAITING_INDEX;
    private String missingMessagesReason;

    private AbstractSequenceResetEncoder sequenceResetEncoder;
    private UtcTimestampEncoder timestampEncoder;
    private MutableAsciiBuffer encodeBuffer;

    private int heartbeatRangeSequenceNumberStart = OUT_OF_RANGE;

    private ReplayOperation replayOperation = null;

    CatchupReplayer(
        final SequenceNumberIndexReader receivedSequenceNumberIndex,
        final ReplayQuery inboundMessages,
        final GatewayPublication inboundPublication,
        final ErrorHandler errorHandler,
        final long correlationId,
        final long connectionId,
        final int libraryId,
        final int lastReceivedSeqNum,
        final int currentSequenceIndex,
        final int replayFromSequenceNumber,
        final int replayFromSequenceIndex,
        final GatewaySession session,
        final long catchupTimeout,
        final EpochClock clock)
    {
        this.receivedSequenceNumberIndex = receivedSequenceNumberIndex;
        this.inboundMessages = inboundMessages;
        this.inboundPublication = inboundPublication;
        this.errorHandler = errorHandler;
        this.correlationId = correlationId;
        this.connectionId = connectionId;
        this.libraryId = libraryId;
        this.lastReceivedSeqNum = lastReceivedSeqNum;
        this.currentSequenceIndex = currentSequenceIndex;
        this.replayFromSequenceNumber = replayFromSequenceNumber;
        this.replayFromSequenceIndex = replayFromSequenceIndex;
        this.session = session;
        this.catchupEndTimeInMs = clock.time() + catchupTimeout;
        this.requiredPosition = inboundPublication.position();
        this.headerDecoder = session.fixDictionary().makeHeaderDecoder();

        possDupEnabler = new PossDupEnabler(
            bufferClaim,
            this::claimBuffer,
            this::onPreCommit,
            this::onIllegalState,
            errorHandler,
            clock,
            inboundPublication.maxPayloadLength(),
            CATCHUP);
    }

    private void onPreCommit(final MutableDirectBuffer buffer, final int offset)
    {
        final int frameOffset = offset + MessageHeaderEncoder.ENCODED_LENGTH;
        messageEncoder
            .wrap(buffer, frameOffset)
            .connection(connectionId)
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

        if (messageDecoder.messageType() == HEARTBEAT_MESSAGE_TYPE)
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
            sequenceResetEncoder = session.fixDictionary().makeSequenceResetEncoder();
            timestampEncoder = new UtcTimestampEncoder();
            encodeBuffer = new MutableAsciiBuffer(new byte[ENCODE_BUFFER_SIZE]);
            sequenceResetEncoder.gapFillFlag(true);

            final SessionHeaderEncoder header = sequenceResetEncoder.header()
                .possDupFlag(true);

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
            libraryId, SEQUENCE_RESET_MESSAGE_TYPE,
            messageDecoder.session(), replayFromSequenceIndex, libraryId,
            CATCHUP_REPLAY, heartbeatRangeSequenceNumberEnd) > 0;

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
        }
        return action;
    }

    public long attempt()
    {
        DebugLogger.log(CATCHUP, "Attempt replay for sessionId=%d%n",
            session.sessionId());
        switch (state)
        {
            case AWAITING_INDEX:
            {
                final long indexedPosition = receivedSequenceNumberIndex.indexedPosition(inboundPublication.id());

                if (indexedPosition >= requiredPosition)
                {
                    state = State.REPLAY_QUERY;
                }
                else
                {
                    DebugLogger.log(CATCHUP,
                        "Awaiting index position: indexed=%d vs required=%d%n",
                        indexedPosition,
                        requiredPosition);
                }
                return BACK_PRESSURED;
            }

            case REPLAY_QUERY:
            {
                if (notLoggingInboundMessages())
                {
                    return switchToMissingMessages("Not logging inbound messages");
                }

                DebugLogger.log(CATCHUP,
                    "Querying for sessionId=%d, currently at (%d, %d)%n",
                    session.sessionId(), lastReceivedSeqNum, currentSequenceIndex);

                replayOperation = inboundMessages.query(
                    this,
                    session.sessionId(),
                    replayFromSequenceNumber,
                    replayFromSequenceIndex,
                    lastReceivedSeqNum,
                    currentSequenceIndex,
                    CATCHUP);

                state = State.REPLAYING;

                return BACK_PRESSURED;
            }

            case REPLAYING:
            {
                // Timeout the catchup operations
                if (System.currentTimeMillis() > catchupEndTimeInMs)
                {
                    return switchToMissingMessages("Catchup operation timed out");
                }

                if (replayOperation.attemptReplay())
                {
                    if (hasMissingMessages())
                    {
                        return switchToMissingMessages("Is missing messages from replay index query");
                    }
                    else
                    {
                        state = State.SEND_OK;
                        return sendOk(inboundPublication, correlationId, session);
                    }
                }
                else
                {
                    // Incomplete operation
                    return BACK_PRESSURED;
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

    private long switchToMissingMessages(final String reason)
    {
        state = State.SEND_MISSING;
        missingMessagesReason = reason;
        return sendMissingMessages();
    }

    private boolean hasMissingMessages()
    {
        return replayFromSequenceIndex < currentSequenceIndex || replayFromSequenceNumber < lastReceivedSeqNum;
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
        DebugLogger.log(CATCHUP, "OK for sessionId=%d%n", session.sessionId());
        final long position = publication.saveRequestSessionReply(libraryId, OK, correlationId);
        if (position >= 0)
        {
            session.play();
        }

        return position;
    }

    private long sendMissingMessages()
    {
        DebugLogger.log(CATCHUP, "Missing Messages for sessionId=%d%n", session.sessionId());
        final long position = inboundPublication.saveRequestSessionReply(libraryId, MISSING_MESSAGES, correlationId);
        if (position > 0)
        {
            errorHandler.onError(new IllegalStateException(String.format(
                "Failed to read correct number of messages for sessionId=%d," +
                " finished at [%d, %d] instead of [%d, %d] - %s",
                session.sessionId(),
                replayFromSequenceIndex,
                replayFromSequenceNumber,
                currentSequenceIndex,
                lastReceivedSeqNum,
                missingMessagesReason)));

            missingMessagesReason = null;

            session.play();
        }

        return position;
    }

    public void close()
    {
        if (replayOperation != null)
        {
            replayOperation.close();
        }
    }
}
