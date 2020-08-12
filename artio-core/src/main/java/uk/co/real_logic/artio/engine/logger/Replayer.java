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
import org.agrona.collections.IntHashSet;
import org.agrona.collections.Long2ObjectHashMap;
import org.agrona.collections.LongHashSet;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.status.AtomicCounter;
import uk.co.real_logic.artio.DebugLogger;
import uk.co.real_logic.artio.FixGatewayException;
import uk.co.real_logic.artio.decoder.AbstractResendRequestDecoder;
import uk.co.real_logic.artio.dictionary.generation.GenerationUtil;
import uk.co.real_logic.artio.engine.ILink3RetransmitHandler;
import uk.co.real_logic.artio.engine.ReplayHandler;
import uk.co.real_logic.artio.engine.ReplayerCommandQueue;
import uk.co.real_logic.artio.engine.SenderSequenceNumbers;
import uk.co.real_logic.artio.fields.EpochFractionFormat;
import uk.co.real_logic.artio.fields.UtcTimestampEncoder;
import uk.co.real_logic.artio.ilink.AbstractILink3Offsets;
import uk.co.real_logic.artio.ilink.AbstractILink3Parser;
import uk.co.real_logic.artio.ilink.AbstractILink3Proxy;
import uk.co.real_logic.artio.messages.*;
import uk.co.real_logic.artio.util.AsciiBuffer;
import uk.co.real_logic.artio.util.CharFormatter;
import uk.co.real_logic.artio.util.Lazy;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

import java.util.Set;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.*;
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

    // Safe to share between multiple ReplayerSession instances due to single threaded nature of the Replayer
    final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
    final ReplayCompleteEncoder replayCompleteEncoder = new ReplayCompleteEncoder();

    // FIX specific state.
    private final LongHashSet gapFillMessageTypes;
    private final FixSessionCodecsFactory fixSessionCodecsFactory;
    private final CharFormatter receivedResendFormatter = new CharFormatter(
        "Received Resend Request for range: [%s, %s]%n");
    private final CharFormatter alreadyDisconnectedFormatter = new CharFormatter(
        "Not processing Resend Request for %s because it has already disconnected %n");

    // For FixReplayerSession, safe to share rather than allocate for each FixReplayerSession
    final CharFormatter completeNotRecentFormatter = new CharFormatter(
        "ReplayerSession: completeReplay-!upToMostRecent replayedMessages=%s " +
        "endSeqNo=%s beginSeqNo=%s expectedCount=%s%n");
    final CharFormatter completeReplayGapfillFormatter = new CharFormatter(
        "ReplayerSession: completeReplay-sendGapFill action=%s, replayedMessages=%s, " +
        "beginGapFillSeqNum=%s, newSequenceNumber=%s%n");

    // ILink specific state
    private final IntHashSet gapfillOnRetransmitILinkTemplateIds;
    private final Lazy<AbstractILink3Parser> iLink3Parser;
    private final Lazy<AbstractILink3Proxy> iLink3Proxy;
    private final Lazy<AbstractILink3Offsets> iLink3Offsets;
    private final LongHashSet iLinkConnectionIds = new LongHashSet();
    private final ILinkConnectDecoder iLinkConnect = new ILinkConnectDecoder();
    private final ILinkMessageEncoder iLinkMessageEncoder = new ILinkMessageEncoder();

    private final Long2ObjectHashMap<ReplayChannel> connectionIdToReplayerChannel = new Long2ObjectHashMap<>();
    private final MessageHeaderDecoder messageHeader = new MessageHeaderDecoder();
    private final ValidResendRequestDecoder validResendRequest = new ValidResendRequestDecoder();
    private final RequestDisconnectDecoder requestDisconnect = new RequestDisconnectDecoder();
    private final DisconnectDecoder disconnect = new DisconnectDecoder();

    private final int maxBytesInBuffer;
    private final ReplayerCommandQueue replayerCommandQueue;
    private final AtomicCounter currentReplayCount;
    private final int maxConcurrentSessionReplays;
    private final ReplayQuery outboundReplayQuery;
    private final ExclusivePublication publication;
    private final IdleStrategy idleStrategy;
    private final ErrorHandler errorHandler;
    private final int maxClaimAttempts;
    private final Subscription inboundSubscription;
    private final String agentNamePrefix;
    private final EpochClock clock;
    private final ReplayHandler replayHandler;
    private final ILink3RetransmitHandler iLink3RetransmitHandler;
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
        final IntHashSet gapfillOnRetransmitILinkTemplateIds,
        final ReplayHandler replayHandler,
        final ILink3RetransmitHandler iLink3RetransmitHandler,
        final SenderSequenceNumbers senderSequenceNumbers,
        final FixSessionCodecsFactory fixSessionCodecsFactory,
        final int maxBytesInBuffer,
        final ReplayerCommandQueue replayerCommandQueue,
        final EpochFractionFormat epochFractionFormat,
        final AtomicCounter currentReplayCount,
        final int maxConcurrentSessionReplays)
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
        this.gapfillOnRetransmitILinkTemplateIds = gapfillOnRetransmitILinkTemplateIds;
        this.replayHandler = replayHandler;
        this.iLink3RetransmitHandler = iLink3RetransmitHandler;
        this.senderSequenceNumbers = senderSequenceNumbers;
        this.fixSessionCodecsFactory = fixSessionCodecsFactory;
        this.maxBytesInBuffer = maxBytesInBuffer;
        this.replayerCommandQueue = replayerCommandQueue;
        this.currentReplayCount = currentReplayCount;
        this.maxConcurrentSessionReplays = maxConcurrentSessionReplays;

        gapFillMessageTypes = new LongHashSet();
        gapfillOnReplayMessageTypes.forEach(messageTypeAsString ->
            gapFillMessageTypes.add(GenerationUtil.packMessageType(messageTypeAsString)));
        utcTimestampEncoder = new UtcTimestampEncoder(epochFractionFormat);

        iLink3Parser = new Lazy<>(() -> AbstractILink3Parser.make(null, errorHandler));
        iLink3Proxy = new Lazy<>(() -> AbstractILink3Proxy.make(publication, errorHandler));
        iLink3Offsets = new Lazy<>(() -> AbstractILink3Offsets.make(errorHandler));
    }

    public Action onFragment(
        final DirectBuffer buffer, final int start, final int length, final Header header)
    {
        messageHeader.wrap(buffer, start);
        final int templateId = messageHeader.templateId();
        final int offset = start + MessageHeaderDecoder.ENCODED_LENGTH;
        final int blockLength = messageHeader.blockLength();
        final int version = messageHeader.version();

        switch (templateId)
        {
            case ValidResendRequestDecoder.TEMPLATE_ID:
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

            case ILinkConnectDecoder.TEMPLATE_ID:
            {
                iLinkConnect.wrap(
                    buffer,
                    offset,
                    blockLength,
                    version);

                iLinkConnectionIds.add(iLinkConnect.connection());

                return CONTINUE;
            }

            case RequestDisconnectDecoder.TEMPLATE_ID:
            {
                requestDisconnect.wrap(
                    buffer,
                    offset,
                    blockLength,
                    version);

                final long connectionId = requestDisconnect.connection();

                onDisconnect(connectionId);

                return CONTINUE;
            }

            case DisconnectDecoder.TEMPLATE_ID:
            {
                disconnect.wrap(
                    buffer,
                    offset,
                    blockLength,
                    version);

                final long connectionId = disconnect.connection();

                onDisconnect(connectionId);

                return CONTINUE;
            }

            default:
            {
                return fixSessionCodecsFactory.onFragment(buffer, start, length, header);
            }
        }
    }

    private void onDisconnect(final long connectionId)
    {
        iLinkConnectionIds.remove(connectionId);

        final ReplayChannel replayChannel = connectionIdToReplayerChannel.remove(connectionId);
        if (replayChannel != null)
        {
            currentReplayCount.decrement();
            // replay was in progress at the time of disconnect
            replayChannel.close();
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

        final ReplayChannel replayChannel = connectionIdToReplayerChannel.get(connectionId);
        if (replayChannel != null)
        {
            final int enqueuedReplayCount = replayChannel.enqueuedReplayCount();
            if (enqueuedReplayCount >= maxConcurrentSessionReplays)
            {
                errorHandler.onError(new FixGatewayException(String.format(
                    "Ignore resend request for sessionId=%d,connectionId=%d as %d requests in flight",
                    sessionId,
                    connectionId,
                    enqueuedReplayCount)));
                return CONTINUE;
            }

            // Existing replay in progress
            final int length = asciiBuffer.capacity();
            final MutableAsciiBuffer copiedBuffer = new MutableAsciiBuffer(new byte[length]);
            copiedBuffer.putBytes(0, asciiBuffer, 0, length);

            replayChannel.enqueueReplay(
                new EnqueuedReplay(sessionId, connectionId, beginSeqNo, endSeqNo, sequenceIndex, copiedBuffer));

            return COMMIT;
        }
        else
        {
            // New replay
            try
            {
                final ReplayerSession session = processResendRequest(
                    sessionId, connectionId, beginSeqNo, endSeqNo, sequenceIndex, asciiBuffer);
                if (session == null)
                {
                    return ABORT;
                }

                final ReplayChannel channel = new ReplayChannel(session);
                connectionIdToReplayerChannel.put(connectionId, channel);
                currentReplayCount.increment();

                return COMMIT;
            }
            catch (final IllegalStateException e)
            {
                errorHandler.onError(e);
                return CONTINUE;
            }
        }
    }

    private ReplayerSession processResendRequest(
        final long sessionId,
        final long connectionId,
        final long beginSeqNo,
        final long endSeqNo,
        final int sequenceIndex,
        final AsciiBuffer asciiBuffer)
    {
        final FixReplayerCodecs sessionCodecs = fixSessionCodecsFactory.get(sessionId);
        if (sessionCodecs != null)
        {
            return processFixResendRequest(
                sessionId, connectionId, (int)beginSeqNo, (int)endSeqNo, sequenceIndex, asciiBuffer, sessionCodecs);
        }
        else if (iLinkConnectionIds.contains(connectionId))
        {
            DebugLogger.log(REPLAY,
                receivedResendFormatter,
                beginSeqNo,
                endSeqNo);

            final ILinkReplayerSession session = new ILinkReplayerSession(
                connectionId, bufferClaim, idleStrategy, maxClaimAttempts, publication, outboundReplayQuery,
                (int)beginSeqNo, (int)endSeqNo, sessionId, this, gapfillOnRetransmitILinkTemplateIds,
                iLinkMessageEncoder, iLink3Parser.get(), iLink3Proxy.get(), iLink3Offsets.get(),
                iLink3RetransmitHandler);

            session.query();

            return session;
        }

        throw new IllegalStateException("Unknown session: sessionId=" + sessionId + ",connectionId=" + connectionId);
    }

    private FixReplayerSession processFixResendRequest(
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
            return null;
        }

        DebugLogger.log(REPLAY,
            receivedResendFormatter,
            beginSeqNo,
            endSeqNo);

        final AbstractResendRequestDecoder resendRequest = sessionCodecs.resendRequest();
        resendRequest.reset();
        resendRequest.decode(asciiBuffer, 0, asciiBuffer.capacity());

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
            bytesInBuffer,
            maxBytesInBuffer,
            utcTimestampEncoder,
            this);

        fixReplayerSession.query();

        return fixReplayerSession;
    }

    public int doWork()
    {
        int work = replayerCommandQueue.poll();
        work += pollReplayerChannels();
        return work + inboundSubscription.controlledPoll(this, POLL_LIMIT);
    }

    private int pollReplayerChannels()
    {
        final Long2ObjectHashMap<ReplayChannel>.EntryIterator replayerChannels =
            connectionIdToReplayerChannel.entrySet().iterator();
        final int size = connectionIdToReplayerChannel.size();

        while (replayerChannels.hasNext())
        {
            final ReplayChannel channel = replayerChannels.next().getValue();
            if (channel.attemptReplay())
            {
                // Replay complete
                final EnqueuedReplay enqueuedReplay = channel.pollReplay();
                if (enqueuedReplay == null)
                {
                    currentReplayCount.decrementOrdered();
                    replayerChannels.remove();
                }
                else
                {
                    try
                    {
                        final ReplayerSession session = processResendRequest(
                            enqueuedReplay.sessionId(),
                            enqueuedReplay.connectionId(),
                            enqueuedReplay.beginSeqNo(),
                            enqueuedReplay.endSeqNo(),
                            enqueuedReplay.sequenceIndex(),
                            enqueuedReplay.asciiBuffer());

                        channel.startReplay(session);
                    }
                    catch (final IllegalStateException e)
                    {
                        errorHandler.onError(e);
                    }
                }
            }
        }

        return size;
    }

    public void onClose()
    {
        connectionIdToReplayerChannel.values().forEach(ReplayChannel::close);
        connectionIdToReplayerChannel.clear();
        currentReplayCount.set(0);
        currentReplayCount.close();
        publication.close();
        outboundReplayQuery.close();
    }

    public String roleName()
    {
        return agentNamePrefix + "Replayer";
    }

}
