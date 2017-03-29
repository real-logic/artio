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
package uk.co.real_logic.fix_gateway.engine.logger;

import io.aeron.Publication;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.MutableDirectBuffer;
import org.agrona.collections.IntHashSet;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.fix_gateway.Pressure;
import uk.co.real_logic.fix_gateway.builder.Encoder;
import uk.co.real_logic.fix_gateway.decoder.*;
import uk.co.real_logic.fix_gateway.engine.PossDupEnabler;
import uk.co.real_logic.fix_gateway.messages.*;
import uk.co.real_logic.fix_gateway.protocol.ProtocolHandler;
import uk.co.real_logic.fix_gateway.protocol.ProtocolSubscription;
import uk.co.real_logic.fix_gateway.replication.ClusterableSubscription;
import uk.co.real_logic.fix_gateway.util.AsciiBuffer;
import uk.co.real_logic.fix_gateway.util.MutableAsciiBuffer;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.ABORT;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;
import static uk.co.real_logic.fix_gateway.engine.FixEngine.ENGINE_LIBRARY_ID;
import static uk.co.real_logic.fix_gateway.messages.MessageStatus.OK;

/**
 * The replayer responds to resend requests with data from the log of sent messages.
 *
 * This agent subscribes to the stream of incoming fix data messages. It parses
 * Resend Request messages and searches the log, using the replay index to find
 * relevant messages to resend.
 */
public class Replayer implements ProtocolHandler, ControlledFragmentHandler, Agent
{
    public static final int MESSAGE_FRAME_BLOCK_LENGTH =
        MessageHeaderDecoder.ENCODED_LENGTH + FixMessageDecoder.BLOCK_LENGTH + FixMessageDecoder.bodyHeaderLength();
    public static final int SIZE_OF_LENGTH_FIELD = 2;
    public static final int POLL_LIMIT = 10;
    public static final int MOST_RECENT_MESSAGE = 0;

    private static final IntHashSet ADMIN_MESSAGE_TYPES = new IntHashSet();
    private static final int NONE = -1;

    static
    {
        ADMIN_MESSAGE_TYPES.add(LogonDecoder.MESSAGE_TYPE);
        ADMIN_MESSAGE_TYPES.add(LogoutDecoder.MESSAGE_TYPE);
        ADMIN_MESSAGE_TYPES.add(ResendRequestDecoder.MESSAGE_TYPE);
        ADMIN_MESSAGE_TYPES.add(HeartbeatDecoder.MESSAGE_TYPE);
        ADMIN_MESSAGE_TYPES.add(TestRequestDecoder.MESSAGE_TYPE);
        ADMIN_MESSAGE_TYPES.add(SequenceResetDecoder.MESSAGE_TYPE);
    }

    private final ResendRequestDecoder resendRequest = new ResendRequestDecoder();
    private final MessageHeaderDecoder messageHeader = new MessageHeaderDecoder();
    private final FixMessageDecoder fixMessage = new FixMessageDecoder();
    private final HeaderDecoder fixHeader = new HeaderDecoder();
    private final GapFillEncoder gapFillEncoder = new GapFillEncoder();

    private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
    private final FixMessageEncoder fixMessageEncoder = new FixMessageEncoder();

    // Used in onMessage and onFragment
    private final AsciiBuffer asciiBuffer = new MutableAsciiBuffer();

    private final BufferClaim bufferClaim;
    private final PossDupEnabler possDupEnabler;
    private final ProtocolSubscription protocolSubscription = ProtocolSubscription.of(this);

    private final ReplayQuery replayQuery;
    private final Publication publication;
    private final IdleStrategy idleStrategy;
    private final ErrorHandler errorHandler;
    private final int maxClaimAttempts;
    private final ClusterableSubscription subscription;
    private final String agentNamePrefix;

    private int currentMessageOffset;
    private int currentMessageLength;

    private int beginGapFillSeqNum = NONE;
    private int endGapFillSeqNum = NONE;
    private int lastSeenSeqNum = NONE;
    private long connectionId;
    private long sessionId;
    private int sequenceIndex;

    public Replayer(
        final ReplayQuery replayQuery,
        final Publication publication,
        final BufferClaim bufferClaim,
        final IdleStrategy idleStrategy,
        final ErrorHandler errorHandler,
        final int maxClaimAttempts,
        final ClusterableSubscription subscription,
        final String agentNamePrefix)
    {
        this.replayQuery = replayQuery;
        this.publication = publication;
        this.bufferClaim = bufferClaim;
        this.idleStrategy = idleStrategy;
        this.errorHandler = errorHandler;
        this.maxClaimAttempts = maxClaimAttempts;
        this.subscription = subscription;
        this.agentNamePrefix = agentNamePrefix;

        possDupEnabler = new PossDupEnabler(
            bufferClaim, this::claimBuffer, this::nothing, this::onIllegalState, this::onException);
    }

    private void nothing()
    {
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
        final long position)
    {
        if (messageType == ResendRequestDecoder.MESSAGE_TYPE && status == OK)
        {
            final int limit = Math.min(length, srcBuffer.capacity() - srcOffset);

            asciiBuffer.wrap(srcBuffer);
            currentMessageOffset = srcOffset;
            currentMessageLength = limit;
            resendRequest.decode(asciiBuffer, srcOffset, limit);

            final int beginSeqNo = resendRequest.beginSeqNo();
            final int endSeqNo = resendRequest.endSeqNo();
            if (endSeqNo != MOST_RECENT_MESSAGE && endSeqNo < beginSeqNo)
            {
                onIllegalState(
                    "[%s] Error in resend request, endSeqNo (%d) < beginSeqNo (%d)",
                    message(), endSeqNo, beginSeqNo);
                return CONTINUE;
            }


            this.connectionId = connectionId;
            this.sessionId = sessionId;
            this.sequenceIndex = sequenceIndex;
            this.lastSeenSeqNum = beginSeqNo - 1;

            final int count = replayQuery.query(
                this,
                sessionId,
                beginSeqNo,
                sequenceIndex,
                endSeqNo,
                sequenceIndex);

            if (beginGapFillSeqNum != NONE)
            {
                sendGapFillForAdminMessage();
            }

            if (endSeqNo != MOST_RECENT_MESSAGE)
            {
                final int expectedCount = endSeqNo - beginSeqNo + 1;
                if (count != expectedCount)
                {
                    onIllegalState(
                        "[%s] Error in resend request, count(%d) < expectedCount (%d)",
                        message(), count, expectedCount);

                    sendGapFill(lastSeenSeqNum + 1, endSeqNo);
                }
            }
        }

        return CONTINUE;
    }

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

        final int messageOffset = srcOffset + MESSAGE_FRAME_BLOCK_LENGTH; // TODO
        final int messageLength = srcLength - MESSAGE_FRAME_BLOCK_LENGTH;

        if (ADMIN_MESSAGE_TYPES.contains(fixMessage.messageType()))
        {
            asciiBuffer.wrap(srcBuffer);
            fixHeader.decode(asciiBuffer, messageOffset, messageLength);
            final int msgSeqNum = fixHeader.msgSeqNum();
            if (beginGapFillSeqNum == NONE)
            {
                beginGapFillSeqNum = msgSeqNum;
            }
            endGapFillSeqNum = msgSeqNum;

            return CONTINUE;
        }
        else
        {
            if (beginGapFillSeqNum != NONE)
            {
                sendGapFillForAdminMessage();
            }

            return possDupEnabler.enablePossDupFlag(srcBuffer, messageOffset, messageLength, srcOffset, srcLength);
        }
    }

    private Action sendGapFillForAdminMessage()
    {
        return sendGapFill(beginGapFillSeqNum, endGapFillSeqNum);
    }

    private Action sendGapFill(int beginGapFillSeqNum, int endGapFillSeqNum)
    {
        final long result = gapFillEncoder.encode(resendRequest.header(), beginGapFillSeqNum, endGapFillSeqNum);
        final int gapFillLength = Encoder.length(result);
        final int gapFillOffset = Encoder.offset(result);

        if (claimBuffer(MESSAGE_FRAME_BLOCK_LENGTH + gapFillLength))
        {
            int destOffset = bufferClaim.offset();
            final MutableDirectBuffer destBuffer = bufferClaim.buffer();

            messageHeaderEncoder
                .wrap(destBuffer, destOffset)
                .blockLength(fixMessageEncoder.sbeBlockLength())
                .templateId(fixMessageEncoder.sbeTemplateId())
                .schemaId(fixMessageEncoder.sbeSchemaId())
                .version(fixMessageEncoder.sbeSchemaVersion());

            destOffset += MessageHeaderEncoder.ENCODED_LENGTH;

            fixMessageEncoder
                .wrap(destBuffer, destOffset)
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
            this.endGapFillSeqNum = NONE;

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

    public int doWork() throws Exception
    {
        return subscription.poll(protocolSubscription, POLL_LIMIT);
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
