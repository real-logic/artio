/*
 * Copyright 2015-2022 Real Logic Limited.
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

import io.aeron.ExclusivePublication;
import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import io.aeron.logbuffer.Header;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.collections.LongArrayQueue;
import org.agrona.concurrent.status.AtomicCounter;
import uk.co.real_logic.artio.DebugLogger;
import uk.co.real_logic.artio.LogTag;
import uk.co.real_logic.artio.dictionary.FixDictionary;
import uk.co.real_logic.artio.engine.ByteBufferUtil;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.MessageTimingHandler;
import uk.co.real_logic.artio.engine.SenderSequenceNumber;
import uk.co.real_logic.artio.engine.logger.ArchiveDescriptor;
import uk.co.real_logic.artio.fields.UtcTimestampEncoder;
import uk.co.real_logic.artio.messages.*;
import uk.co.real_logic.artio.session.CompositeKey;
import uk.co.real_logic.artio.util.CharFormatter;

import java.io.IOException;
import java.nio.ByteBuffer;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;
import static io.aeron.logbuffer.FrameDescriptor.UNFRAGMENTED;
import static uk.co.real_logic.artio.DebugLogger.IS_REPLAY_LOG_TAG_ENABLED;
import static uk.co.real_logic.artio.LogTag.FIX_MESSAGE_TCP;
import static uk.co.real_logic.artio.messages.DisconnectReason.EXCEPTION;
import static uk.co.real_logic.artio.messages.DisconnectReason.SLOW_CONSUMER;
import static uk.co.real_logic.artio.messages.ThrottleRejectDecoder.businessRejectRefIDHeaderLength;
import static uk.co.real_logic.artio.protocol.GatewayPublication.FRAME_SIZE;
import static uk.co.real_logic.artio.session.Session.NO_REPLAY_CORRELATION_ID;

class FixSenderEndPoint extends SenderEndPoint
{
    static class Formatters
    {
        final CharFormatter replayPaused = new CharFormatter("connId=%s, sessId=%s, replayPaused=%s");
        final CharFormatter replayComplete = new CharFormatter(
            "SEP.replayComplete, connId=%s, corrId=%s, slow=%s, replayInFlight=%s, partiallySent=%s," +
            " skipPosition=%s");
        final CharFormatter validResendRequest = new CharFormatter(
            "SEP.validResendRequest, connId=%s, corrId=%s, slow=%s, replayInFlight=%s, queue=%s");
        final CharFormatter checkStartReplay = new CharFormatter(
            "SEP.checkStartReplay, connId=%s, corrId=%s, slow=%s, replayInFlight=%s, queue=%s");
    }

    private static final int HEADER_LENGTH = MessageHeaderDecoder.ENCODED_LENGTH;
    static final int START_REPLAY_LENGTH = HEADER_LENGTH + StartReplayDecoder.BLOCK_LENGTH;
    // Need to give Aeron the start position of the previous message, so include the DHF, naturally term aligned
    static final int TOTAL_START_REPLAY_LENGTH = START_REPLAY_LENGTH + DataHeaderFlyweight.HEADER_LENGTH;
    private static final int REPLAY_MESSAGE = -1;
    public static final int THROTTLE_BUSINESS_REJECT_REASON = 99;

    private final long connectionId;
    private final AtomicCounter invalidLibraryAttempts;
    private final long slowConsumerTimeoutInMs;
    private final StreamTracker outboundTracker;
    private final StreamTracker replayTracker;
    private final SenderSequenceNumber senderSequenceNumber;
    private final MessageTimingHandler messageTimingHandler;
    private final FixReceiverEndPoint receiverEndPoint;
    private final LongArrayQueue replayQueue;
    private final Formatters formatters;

    private long sessionId;
    private long sendingTimeoutTimeInMs;
    private boolean replayPaused;

    private FixThrottleRejectBuilder throttleRejectBuilder;
    private FixDictionary fixDictionary;
    private CompositeKey sessionKey;
    private EngineConfiguration configuration;
    private long replayInFlight;

    FixSenderEndPoint(
        final long connectionId,
        final int libraryId,
        final BlockablePosition outboundBlockablePosition,
        final ExclusivePublication inboundPublication,
        final BlockablePosition replayBlockablePosition,
        final TcpChannel channel,
        final AtomicCounter bytesInBuffer,
        final AtomicCounter invalidLibraryAttempts,
        final ErrorHandler errorHandler,
        final Framer framer,
        final int maxBytesInBuffer,
        final long slowConsumerTimeoutInMs,
        final long timeInMs,
        final SenderSequenceNumber senderSequenceNumber,
        final MessageTimingHandler messageTimingHandler,
        final int maxConcurrentSessionReplays,
        final FixReceiverEndPoint receiverEndPoint,
        final Formatters formatters)
    {
        super(connectionId, inboundPublication, libraryId, channel, bytesInBuffer, maxBytesInBuffer, errorHandler,
            framer);
        this.connectionId = connectionId;
        this.invalidLibraryAttempts = invalidLibraryAttempts;

        this.slowConsumerTimeoutInMs = slowConsumerTimeoutInMs;
        this.senderSequenceNumber = senderSequenceNumber;

        outboundTracker = new StreamTracker(outboundBlockablePosition);
        replayTracker = new StreamTracker(replayBlockablePosition);
        this.messageTimingHandler = messageTimingHandler;
        this.receiverEndPoint = receiverEndPoint;
        this.formatters = formatters;
        sendingTimeoutTimeInMs = timeInMs + slowConsumerTimeoutInMs;
        replayQueue = new LongArrayQueue(
            Math.max(LongArrayQueue.MIN_CAPACITY, maxConcurrentSessionReplays), NO_REPLAY_CORRELATION_ID);
    }

    void onOutboundMessage(
        final int libraryId,
        final DirectBuffer directBuffer,
        final int offset,
        final int bodyLength,
        final int sequenceNumber,
        final Header header,
        final long timeInMs,
        final int metaDataLength)
    {
        if (isWrongLibraryId(libraryId))
        {
            invalidLibraryAttempts.increment();
            return;
        }

        if (replayPaused)
        {
            dropFurtherBehind(bodyLength);

            return;
        }

        final MessageTimingHandler messageTimingHandler = this.messageTimingHandler;
        if (attemptFramedMessage(directBuffer, offset, bodyLength, timeInMs, header, outboundTracker) &&
            messageTimingHandler != null)
        {
            final int metaDataOffset = offset - FixMessageDecoder.bodyHeaderLength() - metaDataLength;

            messageTimingHandler.onMessage(sequenceNumber, connectionId, directBuffer, metaDataOffset, metaDataLength);
        }

        senderSequenceNumber.onNewMessage(sequenceNumber);
    }

    public void onThrottleReject(
        final int libraryId,
        final long refMsgType,
        final int refSeqNum,
        final int sequenceNumber,
        final DirectBuffer businessRejectRefIDBuffer,
        final int businessRejectRefIDOffset,
        final int businessRejectRefIDLength,
        final Header header,
        final long timeInMs)
    {
        if (isWrongLibraryId(libraryId))
        {
            invalidLibraryAttempts.increment();
            return;
        }

        final FixThrottleRejectBuilder throttleRejectBuilder = throttleRejectBuilder();
        if (!throttleRejectBuilder.build(
            refMsgType,
            refSeqNum,
            sequenceNumber,
            businessRejectRefIDBuffer,
            businessRejectRefIDOffset,
            businessRejectRefIDLength,
            false))
        {
            // failed to build reject due to configuration error
            return;
        }

        onOutboundMessage(
            libraryId,
            throttleRejectBuilder.buffer(),
            throttleRejectBuilder.offset(),
            throttleRejectBuilder.length(),
            sequenceNumber,
            header,
            timeInMs,
            0);
    }

    public Action onSlowThrottleReject(
        final int libraryId,
        final long refMsgType,
        final int refSeqNum,
        final int sequenceNumber,
        final DirectBuffer businessRejectRefIDBuffer,
        final int businessRejectRefIDOffset,
        final int businessRejectRefIDLength,
        final Header header,
        final long timeInMs)
    {
        if (isWrongLibraryId(libraryId))
        {
            invalidLibraryAttempts.increment();
            return CONTINUE;
        }

        // We hoist these next two steps early on this path to avoid building the throttle reject
        if (!isSlowConsumer())
        {
            return CONTINUE;
        }

        // Skip all messages beyond the skip position, since this endpoint has been blocked but others
        // Scanning forward.
        final long skipPosition = outboundTracker.skipPosition;
        final long position = header.position();
        if (position > skipPosition)
        {
            return CONTINUE;
        }

        final FixThrottleRejectBuilder throttleRejectBuilder = throttleRejectBuilder();
        if (!throttleRejectBuilder.build(
            refMsgType,
            refSeqNum,
            sequenceNumber,
            businessRejectRefIDBuffer,
            businessRejectRefIDOffset,
            businessRejectRefIDLength,
            false))
        {
            // failed to build reject due to configuration error
            return CONTINUE;
        }

        // fake the data offset after header position in order to make it look like a normally framed FIX message.
        final int fakeOffsetAfterHeader = throttleRejectBuilder.offset() - FRAME_SIZE;
        return attemptSlowMessage(
            throttleRejectBuilder.buffer(),
            fakeOffsetAfterHeader,
            throttleRejectLength(businessRejectRefIDLength),
            header,
            throttleRejectBuilder.length(),
            timeInMs,
            outboundTracker,
            0,
            sequenceNumber);
    }

    private FixThrottleRejectBuilder throttleRejectBuilder()
    {
        if (throttleRejectBuilder == null)
        {
            throttleRejectBuilder = new FixThrottleRejectBuilder(
                fixDictionary,
                errorHandler,
                sessionId,
                connectionId,
                new UtcTimestampEncoder(configuration.sessionEpochFractionFormat()),
                configuration.epochNanoClock(),
                configuration.throttleWindowInMs(), configuration.throttleLimitOfMessages()
            );
            configuration.sessionIdStrategy().setupSession(sessionKey, throttleRejectBuilder.header());
        }

        return throttleRejectBuilder;
    }

    boolean configureThrottle(final int throttleWindowInMs, final int throttleLimitOfMessages)
    {
        return throttleRejectBuilder().configureThrottle(throttleWindowInMs, throttleLimitOfMessages);
    }

    private int throttleRejectLength(final int businessRejectRefIDLength)
    {
        return ThrottleRejectDecoder.BLOCK_LENGTH + businessRejectRefIDHeaderLength() + businessRejectRefIDLength;
    }

    Action onReplayMessage(
        final DirectBuffer directBuffer,
        final int offset,
        final int bodyLength,
        final long timeInMs,
        final Header header)
    {
        if (!isSlowConsumer())
        {
            replayPaused(true);
        }

        attemptFramedMessage(directBuffer, offset, bodyLength, timeInMs, header, replayTracker);

        return CONTINUE;
    }

    Action onSlowReplayMessage(
        final DirectBuffer buffer,
        final int offset,
        final int bodyLength,
        final long timeInMs,
        final Header header,
        final int metaDataLength)
    {
        if (replayInFlight == NO_REPLAY_CORRELATION_ID)
        {
            // Invariant: already blocked here by start replay message, skip this message until ready to send replay
            return CONTINUE;
        }

        if (!outboundTracker.partiallySentMessage)
        {
            replayPaused(true);
        }

        final int totalFrameSize = FRAME_SIZE + metaDataLength;
        final int offsetAfterHeader = offset - totalFrameSize;
        final int length = bodyLength + totalFrameSize;

        return attemptSlowMessage(buffer, offsetAfterHeader, length, header, bodyLength, timeInMs, replayTracker,
            metaDataLength, REPLAY_MESSAGE);
    }

    private boolean attemptFramedMessage(
        final DirectBuffer directBuffer,
        final int offset,
        final int bodyLength,
        final long timeInMs,
        final Header header,
        final StreamTracker tracker)
    {
        if (isSlowConsumer())
        {
            dropFurtherBehind(bodyLength);

            return false;
        }

        try
        {
            final int written = writeFramedMessage(directBuffer, offset, bodyLength, timeInMs);

            if (written != bodyLength)
            {
                becomeSlowConsumer(written, bodyLength, header, tracker);
            }
            else
            {
                tracker.sentPosition = header.position();
                return true;
            }
        }
        catch (final IOException ex)
        {
            onError(ex);
        }

        return false;
    }

    private void dropFurtherBehind(final int bodyLength)
    {
        final long bytesInBuffer = bytesInBufferWeak() + bodyLength;
        if (bytesInBuffer > maxBytesInBuffer)
        {
            disconnectEndpoint(SLOW_CONSUMER);
        }

        this.bytesInBuffer.setOrdered(bytesInBuffer);
    }

    private int writeFramedMessage(
        final DirectBuffer directBuffer,
        final int offset,
        final int length,
        final long timeInMs)
        throws IOException
    {
        final ByteBuffer buffer = directBuffer.byteBuffer();
        final int startLimit = buffer.limit();
        final int startPosition = buffer.position();

        ByteBufferUtil.limit(buffer, offset + length);
        ByteBufferUtil.position(buffer, offset);

        final int written = channel.write(buffer);
        ByteBufferUtil.position(buffer, offset);
        DebugLogger.log(FIX_MESSAGE_TCP, "Written  ", buffer, written);
        updateSendingTimeoutTimeInMs(timeInMs, written);

        buffer.limit(startLimit).position(startPosition);

        return written;
    }

    private void updateSendingTimeoutTimeInMs(final long timeInMs, final int written)
    {
        if (written > 0)
        {
            sendingTimeoutTimeInMs = timeInMs + slowConsumerTimeoutInMs;
        }
    }

    private void onError(final Exception ex)
    {
        errorHandler.onError(new Exception(String.format(
            "Exception reported for sessionId=%d,connectionId=%d", sessionId, connectionId), ex));
        disconnectEndpoint(EXCEPTION);
    }

    private void becomeSlowConsumer(
        final int written, final int bodyLength, final Header header, final StreamTracker tracker)
    {
        final int remainingBytes = bodyLength - written;
        bytesInBuffer.setOrdered(remainingBytes);
        sendSlowStatus(true);
        tracker.sentPosition = header.position() - remainingBytes;
        tracker.partiallySentMessage = true;
    }

    public void libraryId(final int libraryId, final BlockablePosition blockablePosition)
    {
        libraryId(libraryId);
        this.outboundTracker.blockablePosition = blockablePosition;
    }

    public void close()
    {
        senderSequenceNumber.close();
        invalidLibraryAttempts.close();
        super.close();
    }

    Action onSlowOutboundMessage(
        final DirectBuffer directBuffer,
        final int offsetAfterHeader,
        final int length,
        final Header header,
        final int bodyLength,
        final int libraryId,
        final long timeInMs,
        final int metaDataLength,
        final int sequenceNumber)
    {
        if (isWrongLibraryId(libraryId))
        {
            invalidLibraryAttempts.increment();
            return CONTINUE;
        }

        if (replayPaused)
        {
            return blockPositionFixMessage(header, length, outboundTracker, false);
        }

        return attemptSlowMessage(
            directBuffer, offsetAfterHeader, length, header, bodyLength, timeInMs, outboundTracker, metaDataLength,
            sequenceNumber);
    }

    private Action attemptSlowMessage(
        final DirectBuffer directBuffer,
        final int offsetAfterHeader,
        final int length,
        final Header header,
        final int bodyLength,
        final long timeInMs,
        final StreamTracker tracker,
        final int metaDataLength,
        final int sequenceNumber)
    {
        if (!isSlowConsumer())
        {
            return CONTINUE;
        }

        // Skip all messages beyond the skip position, since this endpoint has been blocked but others
        // Scanning forward.
        final long skipPosition = tracker.skipPosition;
        final long position = header.position();
        if (position > skipPosition)
        {
            return CONTINUE;
        }

        // Skip messages where the end point has become a slow consumer, but
        // the slow consumer stream hasn't polled up to update with the regular stream
        final long sentPosition = tracker.sentPosition;
        if (position <= sentPosition)
        {
            return CONTINUE;
        }

        if (partiallySentOtherStream(tracker))
        {
            return blockPositionFixMessage(header, length, tracker, true);
        }

        try
        {
            final long startOfMessage = position - length;
            final int remainingLength;
            final int bytesPreviouslySent;

            // You've completed the stream and there's another message in between.
            if (sentPosition < startOfMessage)
            {
                remainingLength = bodyLength;
                bytesPreviouslySent = 0;
            }
            else
            {
                remainingLength = (int)(position - sentPosition);
                bytesPreviouslySent = bodyLength - remainingLength;
            }

            final int metaDataOffset = offsetAfterHeader + FixMessageEncoder.BLOCK_LENGTH +
                FixMessageDecoder.metaDataHeaderLength();
            final int dataOffset = metaDataOffset + FixMessageDecoder.bodyHeaderLength() + metaDataLength +
                bytesPreviouslySent;
            final ByteBuffer buffer = directBuffer.byteBuffer();

            ByteBufferUtil.limit(buffer, dataOffset + remainingLength);
            ByteBufferUtil.position(buffer, dataOffset);

            final int written = channel.write(buffer);
            bytesInBuffer.getAndAddOrdered(-written);

            updateSendingTimeoutTimeInMs(timeInMs, written);

            if (bodyLength > (written + bytesPreviouslySent))
            {
                tracker.sentPosition = (position - remainingLength) + written;
                return blockPositionFixMessage(header, length, tracker, false);
            }
            else
            {
                tracker.sentPosition = position;
                tracker.partiallySentMessage = false;
                tracker.skipPosition = Long.MAX_VALUE;

                final MessageTimingHandler messageTimingHandler = this.messageTimingHandler;
                if (sequenceNumber != REPLAY_MESSAGE && messageTimingHandler != null)
                {
                    messageTimingHandler.onMessage(
                        sequenceNumber, connectionId, directBuffer, metaDataOffset, metaDataLength);
                }

                if (!isSlowConsumer())
                {
                    becomeNormalConsumer();
                }
            }
        }
        catch (final IOException ex)
        {
            onError(ex);
        }

        return CONTINUE;
    }

    private Action blockPositionFixMessage(
        final Header header, final int fragLengthWithoutHeader, final StreamTracker tracker,
        final boolean partiallySentOther)
    {
        final BlockablePosition blockablePosition = tracker.blockablePosition;

        final int aeronHeaderLength;
        if ((header.flags() & UNFRAGMENTED) == UNFRAGMENTED)
        {
            aeronHeaderLength = DataHeaderFlyweight.HEADER_LENGTH;
        }
        else
        {
            final int fragmentCount = fragLengthWithoutHeader / blockablePosition.maxPayload + 1;
            aeronHeaderLength = fragmentCount * DataHeaderFlyweight.HEADER_LENGTH;
        }
        final int frameLength = aeronHeaderLength + fragLengthWithoutHeader + HEADER_LENGTH;
        final int alignedLength = ArchiveDescriptor.alignTerm(frameLength);
        final long messagePosition = header.position();
        final long messageStartPosition = messagePosition - alignedLength;

        blockablePosition.blockPosition(messageStartPosition);
        tracker.skipPosition = messagePosition;
        return Action.CONTINUE;
    }

    private void blockPositionOther(
        final long messageStartPosition, final long messagePosition, final StreamTracker tracker)
    {
        // Assumes non-fragmented message
        final BlockablePosition blockablePosition = tracker.blockablePosition;
        blockablePosition.blockPosition(messageStartPosition);
        tracker.skipPosition = messagePosition;
    }

    private boolean partiallySentOtherStream(final StreamTracker tracker)
    {
        return tracker == outboundTracker ?
            replayTracker.partiallySentMessage :
            outboundTracker.partiallySentMessage;
    }

    private boolean isWrongLibraryId(final int libraryId)
    {
        return libraryId != this.libraryId;
    }

    // Only access on Framer thread
    boolean isSlowConsumer()
    {
        return bytesInBufferWeak() > 0;
    }

    long bytesInBuffer()
    {
        return bytesInBuffer.get();
    }

    private long bytesInBufferWeak()
    {
        return bytesInBuffer.getWeak();
    }

    void sessionId(final long sessionId)
    {
        this.sessionId = sessionId;
    }

    long sessionId()
    {
        return sessionId;
    }

    boolean checkTimeouts(final long timeInMs)
    {
        if (isSlowConsumer() && timeInMs > sendingTimeoutTimeInMs)
        {
            errorHandler.onError(new IllegalStateException(String.format(
                "Slow Consumer Disconnected conn=%d,sess=%d @ time %d, Due to not being able to write since %d",
                connectionId,
                sessionId,
                timeInMs,
                sendingTimeoutTimeInMs - slowConsumerTimeoutInMs)));
            disconnectEndpoint(SLOW_CONSUMER);

            return true;
        }

        return false;
    }

    private void disconnectEndpoint(final DisconnectReason reason)
    {
        receiverEndPoint.completeDisconnect(reason);
    }

    public Action onReplayComplete(final long correlationId, final boolean slow)
    {
        // We don't do a slow check here because you may catch up during the replay if you only became replay slow
        // and thus miss your slow replay complete message, just check the correlation ids below in order to avoid
        // duplicates

        final StreamTracker replayTracker = this.replayTracker;
        final long replayInFlight = this.replayInFlight;
        final boolean partiallySentMessage = replayTracker.partiallySentMessage;
        final long skipPosition = replayTracker.skipPosition;

        if (IS_REPLAY_LOG_TAG_ENABLED)
        {
            DebugLogger.log(LogTag.REPLAY,
                formatters.replayComplete.clear().with(connectionId).with(correlationId).with(slow)
                .with(replayInFlight).with(partiallySentMessage).with(skipPosition));
        }

        if (correlationId == replayInFlight && // deduplicate by checking the correlation id
            !partiallySentMessage &&
            skipPosition == Long.MAX_VALUE) // check we don't have a replay still in progress
        {
            replayPaused(false);

            this.replayInFlight = NO_REPLAY_CORRELATION_ID;
            return super.onReplayComplete(correlationId, slow);
        }

        return CONTINUE;
    }

    void fixDictionary(final FixDictionary fixDictionary)
    {
        this.fixDictionary = fixDictionary;
    }

    void onLogon(final CompositeKey sessionKey, final EngineConfiguration configuration)
    {
        this.sessionKey = sessionKey;
        this.configuration = configuration;
    }

    public void onValidResendRequest(final long correlationId, final boolean slow)
    {
        final long replayInFlight = this.replayInFlight;
        final LongArrayQueue replayQueue = this.replayQueue;

        if (IS_REPLAY_LOG_TAG_ENABLED)
        {
            DebugLogger.log(LogTag.REPLAY, formatters.validResendRequest.clear()
                .with(connectionId).with(correlationId).with(slow).with(replayInFlight).with(replayQueue.toString()));
        }

        // Can potentially flip from slow to normal, so instead of checking
        // that slow == isSlowConsumer() we just add and dedup
        if (!contains(replayQueue, correlationId) && correlationId > replayInFlight)
        {
            replayQueue.addLong(correlationId);
        }
    }

    private boolean contains(final LongArrayQueue replayQueue, final long correlationId)
    {
        final LongArrayQueue.LongIterator it = replayQueue.iterator();
        while (it.hasNext())
        {
            if (it.nextValue() == correlationId)
            {
                return true;
            }
        }
        return false;
    }

    public void onStartReplay(final long correlationId, final long msgPosition, final boolean slow)
    {
        if (slow == isSlowConsumer())
        {
            checkStartReplay(correlationId, msgPosition, slow);
        }
    }

    private void checkStartReplay(
        final long correlationId, final long msgPosition, final boolean slow)
    {
        if (IS_REPLAY_LOG_TAG_ENABLED)
        {
            DebugLogger.log(LogTag.REPLAY, formatters.checkStartReplay.clear()
                .with(connectionId).with(correlationId).with(slow).with(replayInFlight).with(replayQueue.toString()));
        }

        final long nextReplayCorrelationId = replayQueue.peekLong();
        if (nextReplayCorrelationId == NO_REPLAY_CORRELATION_ID)
        {
            blockStartReplay(msgPosition, slow);
            return;
        }

        if (replayInFlight != NO_REPLAY_CORRELATION_ID)
        {
            errorHandler.onError(new IllegalStateException("invariant fail: replayInFlight = " + replayInFlight +
                " when trying to process " + correlationId + ", slow = " + slow + ",connectionId=" + connectionId));
        }

        if (nextReplayCorrelationId == correlationId)
        {
            replayInFlight = nextReplayCorrelationId;
            replayTracker.skipPosition = Long.MAX_VALUE;
            replayQueue.removeLong();

            if (slow)
            {
                dropFurtherBehind(-START_REPLAY_LENGTH);
            }
        }
        else
        {
            if (slow && (correlationId < nextReplayCorrelationId))
            {
                // We're just seeing the same message that we've already processed before on a controlledPeek of the
                // slow stream, which can give us messages twice.
                return;
            }

            errorHandler.onError(new IllegalStateException("invariant fail: concurrent replays, next replay = " +
                nextReplayCorrelationId + " when received event for " + correlationId + ", slow = " + slow +
                ",connectionId=" + connectionId));
        }
    }

    private void blockStartReplay(final long msgPosition, final boolean slow)
    {
        final StreamTracker replayTracker = this.replayTracker;
        final boolean slowBecauseOfNormalStream = replayTracker.sentPosition == 0;
        final long msgStartPosition = msgPosition - TOTAL_START_REPLAY_LENGTH;
        blockPositionOther(msgStartPosition, msgPosition, replayTracker);

        if (!slow)
        {
            // become slow consumer
            bytesInBuffer.setOrdered(START_REPLAY_LENGTH);
            replayTracker.sentPosition = msgStartPosition;
            sendSlowStatus(true);
        }
        else if (slowBecauseOfNormalStream)
        {
            // if slow due to us, we've already seen this message and added it to the bytes in buffer total
            bytesInBuffer.getAndAddOrdered(START_REPLAY_LENGTH);
        }
    }

    // Struct for tracking the slow state of the replay and outbound streams
    static class StreamTracker
    {
        // All messages with a position < sentPosition have been sent.
        private long sentPosition;
        // Any messages with a position > skipPosition should be skipped
        private long skipPosition = Long.MAX_VALUE;
        private boolean partiallySentMessage = false;
        private BlockablePosition blockablePosition;

        StreamTracker(final BlockablePosition blockablePosition)
        {
            this.blockablePosition = blockablePosition;
        }
    }

    private void replayPaused(final boolean replayPaused)
    {
        if (this.replayPaused != replayPaused)
        {
            if (IS_REPLAY_LOG_TAG_ENABLED)
            {
                DebugLogger.log(LogTag.REPLAY,
                    formatters.replayPaused.clear().with(connectionId).with(sessionId).with(replayPaused));
            }

            this.replayPaused = replayPaused;
            if (replayPaused)
            {
                receiverEndPoint.pause();
            }
            else
            {
                receiverEndPoint.play();
            }
        }
    }

    boolean replayPaused()
    {
        return replayPaused;
    }

    public long replayInFlight()
    {
        return replayInFlight;
    }

    public LongArrayQueue queuedReplay()
    {
        return replayQueue;
    }

    public String toString()
    {
        return "FixSenderEndPoint{" +
            "connectionId=" + connectionId +
            ", sessionId=" + sessionId +
            ", sessionKey=" + sessionKey +
            "} " + super.toString();
    }
}
