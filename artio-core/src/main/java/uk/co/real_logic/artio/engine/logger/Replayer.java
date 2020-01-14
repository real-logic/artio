/*
 * Copyright 2015-2020 Real Logic Limited, Adaptive Financial Consulting Ltd.
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
package uk.co.real_logic.artio.engine.logger;

import java.util.ArrayList;
import java.util.Set;

import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.collections.LongHashSet;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.IdleStrategy;

import static org.agrona.collections.ArrayListUtil.fastUnorderedRemove;


import io.aeron.ExclusivePublication;
import io.aeron.Subscription;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import uk.co.real_logic.artio.DebugLogger;
import uk.co.real_logic.artio.decoder.AbstractResendRequestDecoder;
import uk.co.real_logic.artio.dictionary.SessionConstants;
import uk.co.real_logic.artio.dictionary.generation.GenerationUtil;
import uk.co.real_logic.artio.engine.ReplayHandler;
import uk.co.real_logic.artio.engine.SenderSequenceNumbers;
import uk.co.real_logic.artio.messages.DisconnectReason;
import uk.co.real_logic.artio.messages.FixMessageDecoder;
import uk.co.real_logic.artio.messages.MessageHeaderDecoder;
import uk.co.real_logic.artio.messages.MessageStatus;
import uk.co.real_logic.artio.protocol.ProtocolHandler;
import uk.co.real_logic.artio.protocol.ProtocolSubscription;
import uk.co.real_logic.artio.util.AsciiBuffer;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.COMMIT;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;
import static uk.co.real_logic.artio.LogTag.REPLAY;
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

    private final AsciiBuffer asciiBuffer = new MutableAsciiBuffer();

    private final BufferClaim bufferClaim;
    private final FixSessionCodecsFactory fixSessionCodecsFactory;
    private final ControlledFragmentHandler protocolSubscription;
    private final ArrayList<ReplayerSession> replayerSessions = new ArrayList<>();

    private final ReplayQuery replayQuery;
    private final ExclusivePublication publication;
    private final IdleStrategy idleStrategy;
    private final ErrorHandler errorHandler;
    private final int maxClaimAttempts;
    private final Subscription inboundSubscription;
    private final String agentNamePrefix;
    private final LongHashSet gapFillMessageTypes;
    private final EpochClock clock;
    private final ReplayHandler replayHandler;
    private final SenderSequenceNumbers senderSequenceNumbers;

    public Replayer(
        final ReplayQuery replayQuery,
        final ExclusivePublication publication,
        final BufferClaim bufferClaim,
        final IdleStrategy idleStrategy,
        final ErrorHandler errorHandler,
        final int maxClaimAttempts,
        final Subscription inboundSubscription,
        final String agentNamePrefix,
        final EpochClock clock,
        final Set<String> gapfillOnReplayMessageTypes,
        final ReplayHandler replayHandler,
        final SenderSequenceNumbers senderSequenceNumbers,
        final FixSessionCodecsFactory fixSessionCodecsFactory)
    {
        this.replayQuery = replayQuery;
        this.publication = publication;
        this.bufferClaim = bufferClaim;
        this.idleStrategy = idleStrategy;
        this.errorHandler = errorHandler;
        this.maxClaimAttempts = maxClaimAttempts;
        this.inboundSubscription = inboundSubscription;
        this.agentNamePrefix = agentNamePrefix;
        this.clock = clock;
        this.replayHandler = replayHandler;
        this.senderSequenceNumbers = senderSequenceNumbers;
        this.fixSessionCodecsFactory = fixSessionCodecsFactory;

        gapFillMessageTypes = new LongHashSet();
        gapfillOnReplayMessageTypes.forEach(messageTypeAsString ->
            gapFillMessageTypes.add(GenerationUtil.packMessageType(messageTypeAsString)));

        protocolSubscription = ProtocolSubscription.of(this, this.fixSessionCodecsFactory);
    }

    public Action onMessage(
        final DirectBuffer srcBuffer,
        final int srcOffset,
        final int length,
        final int libraryId,
        final long connectionId,
        final long sessionId,
        final int sequenceIndex,
        final long messageType,
        final long timestamp,
        final MessageStatus status,
        final int sequenceNumber,
        final long position)
    {
        if (messageType == SessionConstants.RESEND_REQUEST_MESSAGE_TYPE && status == OK)
        {
            final int limit = Math.min(length, srcBuffer.capacity() - srcOffset);

            asciiBuffer.wrap(srcBuffer);

            final FixReplayerCodecs sessionCodecs = fixSessionCodecsFactory.get(sessionId);
            final AbstractResendRequestDecoder resendRequest = sessionCodecs.resendRequest();
            resendRequest.reset();
            resendRequest.decode(asciiBuffer, srcOffset, limit);

            final int beginSeqNo = resendRequest.beginSeqNo();
            final int endSeqNo = resendRequest.endSeqNo();

            DebugLogger.log(REPLAY,
                "Received Resend Request for range: [%d, %d]%n",
                beginSeqNo,
                endSeqNo);

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

            final GapFillEncoder encoder = sessionCodecs.makeGapFillEncoder();
            encoder.setupMessage(resendRequest.header());

            final ReplayerSession replayerSession = new ReplayerSession(
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
                encoder);

            replayerSession.query();

            replayerSessions.add(replayerSession);

            return COMMIT;
        }

        return CONTINUE;
    }

    public Action onDisconnect(final int libraryId, final long connectionId, final DisconnectReason reason)
    {
        return CONTINUE;
    }

    public int doWork()
    {
        int work = senderSequenceNumbers.poll();
        work += pollReplayerSessions();
        return work + inboundSubscription.controlledPoll(protocolSubscription, POLL_LIMIT);
    }

    private int pollReplayerSessions()
    {
        final ArrayList<ReplayerSession> replayerSessions = this.replayerSessions;
        final int size = replayerSessions.size();

        for (int lastIndex = size - 1, i = lastIndex; i >= 0; i--)
        {
            final ReplayerSession replayerSession = replayerSessions.get(i);
            if (replayerSession.attempReplay())
            {
                fastUnorderedRemove(replayerSessions, i, lastIndex--);
            }
        }
        return size;
    }

    public void onClose()
    {
        replayerSessions.forEach(ReplayerSession::close);
        publication.close();
        replayQuery.close();
    }

    public String roleName()
    {
        return agentNamePrefix + "Replayer";
    }
}
