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
import io.aeron.Subscription;
import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import io.aeron.logbuffer.ExclusiveBufferClaim;
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.collections.IntHashSet;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.artio.decoder.ResendRequestDecoder;
import uk.co.real_logic.artio.dictionary.generation.GenerationUtil;
import uk.co.real_logic.artio.engine.ReplayHandler;
import uk.co.real_logic.artio.engine.SenderSequenceNumbers;
import uk.co.real_logic.artio.messages.*;
import uk.co.real_logic.artio.protocol.ProtocolHandler;
import uk.co.real_logic.artio.protocol.ProtocolSubscription;
import uk.co.real_logic.artio.util.AsciiBuffer;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

import java.util.Set;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.*;
import static uk.co.real_logic.artio.messages.MessageStatus.OK;

/**
 * The replayer responds to resend requests with data from the log of sent messages.
 *
 * This agent subscribes to the stream of incoming fix data messages. It parses
 * Resend Request messages and searches the log, using the replay index to find
 * relevant messages to resend.
 */
public class Replayer implements ProtocolHandler, Agent
{
    static final int MESSAGE_FRAME_BLOCK_LENGTH =
        MessageHeaderDecoder.ENCODED_LENGTH + FixMessageDecoder.BLOCK_LENGTH + FixMessageDecoder.bodyHeaderLength();
    static final int SIZE_OF_LENGTH_FIELD = 2;
    static final int MOST_RECENT_MESSAGE = 0;
    private static final int POLL_LIMIT = 10;

    private final ResendRequestDecoder resendRequest = new ResendRequestDecoder();

    private final AsciiBuffer asciiBuffer = new MutableAsciiBuffer();

    private final ExclusiveBufferClaim bufferClaim;
    private final ProtocolSubscription protocolSubscription = ProtocolSubscription.of(this);

    private final ReplayQuery replayQuery;
    private final ExclusivePublication publication;
    private final IdleStrategy idleStrategy;
    private final ErrorHandler errorHandler;
    private final int maxClaimAttempts;
    private final Subscription subscription;
    private final String agentNamePrefix;
    private final IntHashSet gapFillMessageTypes;
    private final EpochClock clock;
    private final ReplayHandler replayHandler;
    private final SenderSequenceNumbers senderSequenceNumbers;

    private ReplayerSession replayerSession;

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
        this.clock = clock;
        this.replayHandler = replayHandler;
        this.senderSequenceNumbers = senderSequenceNumbers;

        gapFillMessageTypes = new IntHashSet();
        gapfillOnReplayMessageTypes.forEach(messageTypeAsString ->
            gapFillMessageTypes.add(GenerationUtil.packMessageType(messageTypeAsString)));
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

            resendRequest.reset();
            resendRequest.decode(asciiBuffer, srcOffset, limit);

            final int beginSeqNo = resendRequest.beginSeqNo();

            final int endSeqNo = resendRequest.endSeqNo();
            final boolean replayUpToMostRecent = endSeqNo == MOST_RECENT_MESSAGE;
            final String message = asciiBuffer.getAscii(srcOffset, limit);
            // Validate endSeqNo
            if (!replayUpToMostRecent && endSeqNo < beginSeqNo)
            {
                errorHandler.onError(new IllegalStateException(String.format(
                    "[%s] Error in resend request, endSeqNo (%d) < beginSeqNo (%d)",
                    message,
                    endSeqNo,
                    beginSeqNo)));
                return CONTINUE;
            }

            replayerSession = new ReplayerSession(
                bufferClaim,
                idleStrategy,
                replayHandler,
                maxClaimAttempts,
                gapFillMessageTypes,
                senderSequenceNumbers,
                publication,
                clock,
                beginSeqNo,
                endSeqNo,
                replayUpToMostRecent,
                connectionId,
                sessionId,
                sequenceIndex,
                replayQuery,
                message,
                errorHandler,
                resendRequest.header());

            replayerSession.query();

            // We break here to avoid another replay request happening at the same time.
            return BREAK;
        }

        return CONTINUE;
    }

    public Action onDisconnect(final int libraryId, final long connectionId, final DisconnectReason reason)
    {
        return CONTINUE;
    }

    public int doWork()
    {
        final int work = senderSequenceNumbers.poll();

        if (replayerSession != null)
        {
            if (replayerSession.attempCurrentReplayOperation())
            {
                replayerSession = null;
            }

            return work + 1;
        }
        else
        {
            return work + subscription.controlledPoll(protocolSubscription, POLL_LIMIT);
        }
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
