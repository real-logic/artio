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
import org.agrona.MutableDirectBuffer;
import uk.co.real_logic.fix_gateway.DebugLogger;
import uk.co.real_logic.fix_gateway.decoder.HeaderDecoder;
import uk.co.real_logic.fix_gateway.engine.logger.ReplayQuery;
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
import static uk.co.real_logic.fix_gateway.LogTag.FIX_MESSAGE;
import static uk.co.real_logic.fix_gateway.messages.FixMessageDecoder.bodyHeaderLength;
import static uk.co.real_logic.fix_gateway.messages.SessionReplyStatus.MISSING_MESSAGES;
import static uk.co.real_logic.fix_gateway.messages.SessionReplyStatus.OK;

class CatchupReplayer implements ControlledFragmentHandler, Continuation
{
    private static final int FRAME_LENGTH =
        MessageHeaderEncoder.ENCODED_LENGTH + FixMessageEncoder.BLOCK_LENGTH + FixMessageEncoder.bodyHeaderLength();

    private static final int FIX_MESSAGE_BODY_OFFSET =
        FixMessageDecoder.BLOCK_LENGTH + bodyHeaderLength() + MessageHeaderDecoder.ENCODED_LENGTH;

    private enum State
    {
        REPLAYING,
        SEND_MISSING,
        SEND_OK
    }

    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final FixMessageDecoder messageDecoder = new FixMessageDecoder();
    private final FixMessageEncoder messageEncoder = new FixMessageEncoder();

    private final AsciiBuffer asciiBuffer = new MutableAsciiBuffer();
    private final HeaderDecoder headerDecoder = new HeaderDecoder();

    private final BufferClaim bufferClaim = new BufferClaim();
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
    }

    public Action onFragment(
        final DirectBuffer srcBuffer,
        final int srcOffset,
        final int length,
        final Header header)
    {
        if (inboundPublication.claim(length, bufferClaim) > 0)
        {
            messageHeaderDecoder.wrap(srcBuffer, srcOffset);

            messageDecoder.wrap(
                srcBuffer,
                srcOffset + MessageHeaderDecoder.ENCODED_LENGTH,
                messageHeaderDecoder.blockLength(),
                messageHeaderDecoder.version());

            final int messageLength = length - FIX_MESSAGE_BODY_OFFSET;
            asciiBuffer.wrap(srcBuffer, srcOffset + FIX_MESSAGE_BODY_OFFSET, messageLength);
            headerDecoder.decode(asciiBuffer, 0, messageLength);

            final MutableDirectBuffer destBuffer = bufferClaim.buffer();
            final int destOffset = bufferClaim.offset();
            destBuffer.putBytes(destOffset, srcBuffer, srcOffset, length);

            final int frameOffset = destOffset + MessageHeaderEncoder.ENCODED_LENGTH;
            messageEncoder
                .wrap(destBuffer, frameOffset)
                .libraryId(libraryId);

            DebugLogger.log(
                FIX_MESSAGE, "Resending: %s\n", destBuffer, destOffset + FRAME_LENGTH, length - FRAME_LENGTH);

            bufferClaim.commit();

            // store the point to continue from if an abort happens.
            replayFromSequenceNumber = headerDecoder.msgSeqNum();
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
                inboundMessages.query(
                    this,
                    session.sessionId(),
                    replayFromSequenceNumber,
                    replayFromSequenceIndex,
                    lastReceivedSeqNum,
                    currentSequenceIndex);

                if (abortedReplay)
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
        final long position = publication.saveRequestSessionReply(libraryId, OK, correlationId);
        if (position >= 0)
        {
            session.play();
        }
        return position;
    }

    private long sendMissingMessages()
    {
        final long position = inboundPublication.saveRequestSessionReply(libraryId, MISSING_MESSAGES, correlationId);
        if (position > 0)
        {
            errorHandler.onError(new IllegalStateException(String.format(
                "Failed to read correct number of messages for %d, finished at [%d, %d]",
                correlationId,
                replayFromSequenceIndex,
                replayFromSequenceNumber)));
        }
        return position;
    }
}
