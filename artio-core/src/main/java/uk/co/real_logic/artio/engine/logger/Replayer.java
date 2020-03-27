/*
 * Copyright 2015-2020 Real Logic Limited, Adaptive Financial Consulting Ltd., Monotonic Ltd.
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

import io.aeron.ExclusivePublication;
import io.aeron.Subscription;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.collections.LongHashSet;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.status.AtomicCounter;
import uk.co.real_logic.artio.DebugLogger;
import uk.co.real_logic.artio.decoder.AbstractResendRequestDecoder;
import uk.co.real_logic.artio.dictionary.generation.GenerationUtil;
import uk.co.real_logic.artio.engine.ReplayHandler;
import uk.co.real_logic.artio.engine.ReplayerCommandQueue;
import uk.co.real_logic.artio.engine.SenderSequenceNumbers;
import uk.co.real_logic.artio.fields.EpochFractionFormat;
import uk.co.real_logic.artio.fields.UtcTimestampEncoder;
import uk.co.real_logic.artio.messages.*;
import uk.co.real_logic.artio.util.AsciiBuffer;
import uk.co.real_logic.artio.util.CharFormatter;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

import java.util.ArrayList;
import java.util.Set;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.*;
import static org.agrona.collections.ArrayListUtil.fastUnorderedRemove;
import static uk.co.real_logic.artio.LogTag.REPLAY;

/**
 * The replayer responds to resend requests with data from the log of sent messages.
 *
 * This agent subscribes to the stream of incoming fix data messages. It parses
 * Resend Request messages and searches the log, using the replay index to find
 * relevant messages to resend.
 */
public class Replayer implements Agent, ControlledFragmentHandler
{
    public static final int MOST_RECENT_MESSAGE = 0;

    static final int MESSAGE_FRAME_BLOCK_LENGTH =
        MessageHeaderDecoder.ENCODED_LENGTH + FixMessageDecoder.BLOCK_LENGTH + FixMessageDecoder.bodyHeaderLength();
    static final int SIZE_OF_LENGTH_FIELD = FixMessageDecoder.bodyHeaderLength();
    private static final int POLL_LIMIT = 10;

    private final AsciiBuffer asciiBuffer = new MutableAsciiBuffer();
    private final BufferClaim bufferClaim;

    // FIX specific state.
    private final LongHashSet gapFillMessageTypes;
    private final FixSessionCodecsFactory fixSessionCodecsFactory;
    private final CharFormatter receivedResendFormatter = new CharFormatter(
        "Received Resend Request for range: [%s, %s]%n");
    private final CharFormatter alreadyDisconnectedFormatter = new CharFormatter(
        "Not processing Resend Request for %s because it has already disconnected %n");
    private final FixReplayerSession.Formatters formatters = new FixReplayerSession.Formatters();

    // ILink specific state
    private final LongHashSet iLinkConnectionIds = new LongHashSet();
    private final ILinkConnectDecoder iLinkConnect = new ILinkConnectDecoder();
    private final RequestDisconnectDecoder requestDisconnect = new RequestDisconnectDecoder();

    private final ArrayList<ReplayerSession> replayerSessions = new ArrayList<>();
    private final MessageHeaderDecoder messageHeader = new MessageHeaderDecoder();
    private final ValidResendRequestDecoder validResendRequest = new ValidResendRequestDecoder();


    private final int maxBytesInBuffer;
    private final ReplayerCommandQueue replayerCommandQueue;
    private final ReplayQuery outboundReplayQuery;
    private final ExclusivePublication publication;
    private final IdleStrategy idleStrategy;
    private final ErrorHandler errorHandler;
    private final int maxClaimAttempts;
    private final Subscription inboundSubscription;
    private final String agentNamePrefix;
    private final EpochClock clock;
    private final ReplayHandler replayHandler;
    private final SenderSequenceNumbers senderSequenceNumbers;
    private final UtcTimestampEncoder utcTimestampEncoder;

    public Replayer(
        final ReplayQuery outboundReplayQuery,
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
        final FixSessionCodecsFactory fixSessionCodecsFactory,
        final int maxBytesInBuffer,
        final ReplayerCommandQueue replayerCommandQueue,
        final EpochFractionFormat epochFractionFormat)
    {
        this.outboundReplayQuery = outboundReplayQuery;
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
        this.maxBytesInBuffer = maxBytesInBuffer;
        this.replayerCommandQueue = replayerCommandQueue;

        gapFillMessageTypes = new LongHashSet();
        gapfillOnReplayMessageTypes.forEach(messageTypeAsString ->
            gapFillMessageTypes.add(GenerationUtil.packMessageType(messageTypeAsString)));
        utcTimestampEncoder = new UtcTimestampEncoder(epochFractionFormat);
    }

    public Action onFragment(
        final DirectBuffer buffer, final int start, final int length, final Header header)
    {
        messageHeader.wrap(buffer, start);
        final int templateId = messageHeader.templateId();
        final int offset = start + MessageHeaderDecoder.ENCODED_LENGTH;
        final int blockLength = messageHeader.blockLength();
        final int version = messageHeader.version();

        if (templateId == ValidResendRequestDecoder.TEMPLATE_ID)
        {
            validResendRequest.wrap(
                buffer,
                offset,
                blockLength,
                version);

            final long sessionId = validResendRequest.session();
            final long connectionId = validResendRequest.connection();
            final long beginSeqNo = validResendRequest.beginSequenceNumber();
            final long endSeqNo = validResendRequest.endSequenceNumber();
            final int sequenceIndex = validResendRequest.sequenceIndex();
            validResendRequest.wrapBody(asciiBuffer);

            return onResendRequest(sessionId, connectionId, beginSeqNo, endSeqNo, sequenceIndex, asciiBuffer);
        }
        else if (templateId == ILinkConnectDecoder.TEMPLATE_ID)
        {
            iLinkConnect.wrap(
                buffer,
                offset,
                blockLength,
                version);

            iLinkConnectionIds.add(iLinkConnect.connection());

            return CONTINUE;
        }
        else if (templateId == RequestDisconnectDecoder.TEMPLATE_ID)
        {
            requestDisconnect.wrap(
                buffer,
                offset,
                blockLength,
                version);

            iLinkConnectionIds.remove(requestDisconnect.connection());

            return CONTINUE;
        }
        else
        {
            return fixSessionCodecsFactory.onFragment(buffer, start, length, header);
        }
    }

    Action onResendRequest(
        final long sessionId,
        final long connectionId,
        final long beginSeqNo,
        final long endSeqNo,
        final int sequenceIndex,
        final AsciiBuffer asciiBuffer)
    {
        if (senderSequenceNumbers.hasDisconnected(connectionId))
        {
            DebugLogger.log(REPLAY,
                alreadyDisconnectedFormatter,
                connectionId);

            return CONTINUE;
        }

        final FixReplayerCodecs sessionCodecs = fixSessionCodecsFactory.get(sessionId);
        if (sessionCodecs != null)
        {
            if (processFixResendRequest(
                sessionId, connectionId, (int)beginSeqNo, (int)endSeqNo, sequenceIndex, asciiBuffer, sessionCodecs))
            {
                return ABORT;
            }
        }
        else if (iLinkConnectionIds.contains(connectionId))
        {
            DebugLogger.log(REPLAY,
                receivedResendFormatter,
                beginSeqNo,
                endSeqNo);

            final ILinkReplayerSession session = new ILinkReplayerSession(
                connectionId, bufferClaim, idleStrategy, maxClaimAttempts, publication, outboundReplayQuery,
                (int)beginSeqNo, (int)endSeqNo, sessionId);

            session.query();

            replayerSessions.add(session);
        }

        return COMMIT;
    }

    private boolean processFixResendRequest(
        final long sessionId,
        final long connectionId,
        final int beginSeqNo,
        final int endSeqNo,
        final int sequenceIndex,
        final AsciiBuffer asciiBuffer,
        final FixReplayerCodecs sessionCodecs)
    {
        final AtomicCounter bytesInBuffer = senderSequenceNumbers.bytesInBufferCounter(connectionId);
        if (bytesInBuffer == null)
        {
            return true;
        }

        final AbstractResendRequestDecoder resendRequest = sessionCodecs.resendRequest();
        resendRequest.reset();
        resendRequest.decode(asciiBuffer, 0, asciiBuffer.capacity());

        DebugLogger.log(REPLAY,
            receivedResendFormatter,
            beginSeqNo,
            endSeqNo);

        final GapFillEncoder encoder = sessionCodecs.makeGapFillEncoder();
        encoder.setupMessage(resendRequest.header());

        final String message = asciiBuffer.getAscii(0, asciiBuffer.capacity());
        final FixReplayerSession fixReplayerSession = new FixReplayerSession(
            bufferClaim,
            idleStrategy,
            replayHandler,
            maxClaimAttempts,
            gapFillMessageTypes,
            publication,
            clock,
            beginSeqNo,
            endSeqNo,
            connectionId,
            sessionId,
            sequenceIndex,
            outboundReplayQuery,
            message,
            errorHandler,
            encoder,
            formatters,
            bytesInBuffer,
            maxBytesInBuffer,
            utcTimestampEncoder);

        fixReplayerSession.query();

        replayerSessions.add(fixReplayerSession);
        return false;
    }

    public int doWork()
    {
        int work = replayerCommandQueue.poll();
        work += pollReplayerSessions();
        return work + inboundSubscription.controlledPoll(this, POLL_LIMIT);
    }

    private int pollReplayerSessions()
    {
        final ArrayList<ReplayerSession> fixReplayerSessions = this.replayerSessions;
        final int size = fixReplayerSessions.size();

        for (int lastIndex = size - 1, i = lastIndex; i >= 0; i--)
        {
            final ReplayerSession fixReplayerSession = fixReplayerSessions.get(i);
            if (fixReplayerSession.attempReplay())
            {
                fastUnorderedRemove(fixReplayerSessions, i, lastIndex--);
            }
        }
        return size;
    }

    public void onClose()
    {
        replayerSessions.forEach(ReplayerSession::close);
        publication.close();
        outboundReplayQuery.close();
    }

    public String roleName()
    {
        return agentNamePrefix + "Replayer";
    }

}
