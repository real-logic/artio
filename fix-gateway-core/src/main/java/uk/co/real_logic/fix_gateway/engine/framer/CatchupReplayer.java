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
import uk.co.real_logic.fix_gateway.engine.logger.ReplayQuery;
import uk.co.real_logic.fix_gateway.messages.FixMessageEncoder;
import uk.co.real_logic.fix_gateway.messages.MessageHeaderEncoder;
import uk.co.real_logic.fix_gateway.protocol.GatewayPublication;

import static io.aeron.Publication.BACK_PRESSURED;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.ABORT;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;
import static uk.co.real_logic.fix_gateway.LogTag.FIX_MESSAGE;
import static uk.co.real_logic.fix_gateway.messages.SessionReplyStatus.MISSING_MESSAGES;
import static uk.co.real_logic.fix_gateway.messages.SessionReplyStatus.OK;

class CatchupReplayer implements ControlledFragmentHandler, Continuation
{
    private static final int FRAME_LENGTH =
        MessageHeaderEncoder.ENCODED_LENGTH + FixMessageEncoder.BLOCK_LENGTH + FixMessageEncoder.bodyHeaderLength();

    private enum State
    {
        REPLAYING,
        SEND_MISSING,
        SEND_OK
    }

    private final FixMessageEncoder messageEncoder = new FixMessageEncoder();
    private final BufferClaim bufferClaim = new BufferClaim();
    private final ReplayQuery inboundMessages;
    private final GatewayPublication inboundPublication;
    private final ErrorHandler errorHandler;
    private final long correlationId;
    private final int libraryId;
    private final int expectedNumberOfMessages;
    private final int lastReceivedSeqNum;
    private final int replayFromSequenceNumber;
    private final GatewaySession session;

    private int replayedMessages = 0;
    private State state = State.REPLAYING;

    CatchupReplayer(
        final ReplayQuery inboundMessages,
        final GatewayPublication inboundPublication,
        final ErrorHandler errorHandler,
        final long correlationId,
        final int libraryId,
        final int expectedNumberOfMessages,
        final int lastReceivedSeqNum,
        final int replayFromSequenceNumber,
        final GatewaySession session)
    {
        this.inboundMessages = inboundMessages;
        this.inboundPublication = inboundPublication;
        this.errorHandler = errorHandler;
        this.correlationId = correlationId;
        this.libraryId = libraryId;
        this.expectedNumberOfMessages = expectedNumberOfMessages;
        this.lastReceivedSeqNum = lastReceivedSeqNum;
        this.replayFromSequenceNumber = replayFromSequenceNumber;
        this.session = session;
    }

    public Action onFragment(
        final DirectBuffer srcBuffer,
        final int srcOffset,
        final int length,
        final Header header)
    {
        if (inboundPublication.claim(length, bufferClaim) > 0)
        {
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
            replayedMessages++;

            return CONTINUE;
        }
        else
        {
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

                // adding 1 to convert to inclusive numbering
                final int beginSeqNo = replayFromSequenceNumber + 1 + replayedMessages;
                final int numberOfMessages =
                    inboundMessages.query(
                        this,
                        session.sessionId(),
                        beginSeqNo,
                        lastReceivedSeqNum);

                if (replayedMessages < expectedNumberOfMessages)
                {
                    if (numberOfMessages == 0)
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
                "Failed to read correct number of messages for %d, expected %d, read %d",
                correlationId,
                expectedNumberOfMessages,
                replayedMessages)));
        }
        return position;
    }
}
