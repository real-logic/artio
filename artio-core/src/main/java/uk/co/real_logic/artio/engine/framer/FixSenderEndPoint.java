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
import org.agrona.ExpandableDirectByteBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import uk.co.real_logic.artio.DebugLogger;
import uk.co.real_logic.artio.LogTag;
import uk.co.real_logic.artio.dictionary.FixDictionary;
import uk.co.real_logic.artio.engine.ByteBufferUtil;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.MessageTimingHandler;
import uk.co.real_logic.artio.engine.SenderSequenceNumber;
import uk.co.real_logic.artio.fields.UtcTimestampEncoder;
import uk.co.real_logic.artio.messages.*;
import uk.co.real_logic.artio.session.CompositeKey;
import uk.co.real_logic.artio.util.CharFormatter;

import java.io.IOException;
import java.nio.ByteBuffer;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.ABORT;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.agrona.BitUtil.SIZE_OF_LONG;
import static uk.co.real_logic.artio.DebugLogger.IS_REPLAY_LOG_TAG_ENABLED;
import static uk.co.real_logic.artio.LogTag.FIX_MESSAGE_TCP;
import static uk.co.real_logic.artio.messages.DisconnectReason.EXCEPTION;
import static uk.co.real_logic.artio.messages.DisconnectReason.SLOW_CONSUMER;
import static uk.co.real_logic.artio.messages.ThrottleRejectDecoder.businessRejectRefIDHeaderLength;

class FixSenderEndPoint extends SenderEndPoint
{
    private static final int ENQ_MSG = 1;
    private static final int ENQ_MESSAGE_BLOCK_LEN = SIZE_OF_INT + SIZE_OF_INT + SIZE_OF_INT + SIZE_OF_INT;

    private static final int ENQ_REPLAY_COMPLETE = 2;
    private static final int ENQ_REPLAY_COMPLETE_LEN = SIZE_OF_INT + SIZE_OF_LONG;
    private static final int ENQ_START_REPLAY = 3;
    private static final int ENQ_START_REPLAY_LEN = ENQ_REPLAY_COMPLETE_LEN;

    protected static final int NO_REATTEMPT = 0;


    static class Formatters
    {
        final CharFormatter replayComplete = new CharFormatter(
            "SEP.replayComplete, connId=%s, corrId=%s");
        final CharFormatter validResendRequest = new CharFormatter(
            "SEP.validResendRequest, connId=%s, corrId=%s");
        final CharFormatter checkStartReplay = new CharFormatter(
            "SEP.onStartReplay, connId=%s, corrId=%s");
    }

    private static final int HEADER_LENGTH = MessageHeaderDecoder.ENCODED_LENGTH;
    static final int START_REPLAY_LENGTH = HEADER_LENGTH + StartReplayDecoder.BLOCK_LENGTH;
    // Need to give Aeron the start position of the previous message, so include the DHF, naturally term aligned
    static final int TOTAL_START_REPLAY_LENGTH = START_REPLAY_LENGTH + DataHeaderFlyweight.HEADER_LENGTH;
    public static final int THROTTLE_BUSINESS_REJECT_REASON = 99;

    private final long connectionId;
    private final AtomicCounter invalidLibraryAttempts;
    private final long slowConsumerTimeoutInMs;
    private final SenderSequenceNumber senderSequenceNumber;
    private final MessageTimingHandler messageTimingHandler;
    private final FixReceiverEndPoint receiverEndPoint;
    private final Formatters formatters;

    private long sessionId;
    private long sendingTimeoutTimeInMs;

    private FixThrottleRejectBuilder throttleRejectBuilder;
    private FixDictionary fixDictionary;
    private CompositeKey sessionKey;
    private EngineConfiguration configuration;

    private final ReattemptState normalBuffer = new ReattemptState();
    private final ReattemptState replayBuffer = new ReattemptState();

    private boolean replaying;
    private boolean requiresReattempting;
    private int reattemptBytesWritten = NO_REATTEMPT;

    FixSenderEndPoint(
        final long connectionId,
        final int libraryId,
        final ExclusivePublication inboundPublication,
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

        this.messageTimingHandler = messageTimingHandler;
        this.receiverEndPoint = receiverEndPoint;
        this.formatters = formatters;
        sendingTimeoutTimeInMs = timeInMs + slowConsumerTimeoutInMs;
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

        onMessage(directBuffer, offset, bodyLength, metaDataLength, sequenceNumber, timeInMs, false);

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

    public void onMessage(
        final DirectBuffer directBuffer, final int offset, final int bodyLength, final int metaDataLength,
        final int seqNum, final long timeInMs, final boolean replay)
    {
        try
        {
            final int metaDataOffset = offset - FixMessageDecoder.bodyHeaderLength() - metaDataLength;

            if ((replaying && !replay) || (!replaying && replay) || reattemptBytesWritten > 0)
            {
                enqueueMessage(directBuffer, offset, bodyLength, metaDataOffset, metaDataLength, seqNum, replay);
                return;
            }

            final int written = writeBuffer(directBuffer, offset, bodyLength);
            final int totalWritten = reattemptBytesWritten + written;

            if (totalWritten < bodyLength)
            {
                reattemptBytesWritten = totalWritten;
                enqueueMessage(directBuffer, offset, bodyLength, metaDataOffset, metaDataLength, seqNum, replay);
            }
            else
            {
                reattemptBytesWritten = NO_REATTEMPT;

                final MessageTimingHandler messageTimingHandler = this.messageTimingHandler;
                if (messageTimingHandler != null && !replay)
                {
                    messageTimingHandler.onMessage(
                        seqNum, connectionId, directBuffer, metaDataOffset, metaDataLength);
                }
            }

            updateSendingTimeoutTimeInMs(timeInMs, written);
        }
        catch (final IOException e)
        {
            errorHandler.onError(e);
        }
    }

    private int writeBuffer(
        final DirectBuffer directBuffer, final int offset, final int messageSize) throws IOException
    {
        final ByteBuffer buffer = directBuffer.byteBuffer();
        final int startLimit = buffer.limit();
        final int startPosition = buffer.position();

        ByteBufferUtil.limit(buffer, offset + messageSize);
        ByteBufferUtil.position(buffer, reattemptBytesWritten + offset);

        final int written = channel.write(buffer);
        ByteBufferUtil.position(buffer, offset);
        DebugLogger.logBytes(FIX_MESSAGE_TCP, "Written  ", buffer, startPosition, written);

        buffer.limit(startLimit).position(startPosition);

        return written;
    }

    private void enqueueMessage(
        final DirectBuffer srcBuffer, final int srcOffst, final int bodyLength,
        final int metaDataOffset, final int metaDataLength, final int sequenceNumber, final boolean replay)
    {
        final int totalLength = ENQ_MESSAGE_BLOCK_LEN + bodyLength + metaDataLength;
        final ReattemptState reattemptState = enqueue(totalLength, replay);

        int reattemptOffset = reattemptState.usage - totalLength;
        final ExpandableDirectByteBuffer buffer = reattemptState.buffer();

        /*System.out.println("FixSenderEndPoint.enqueueMessage" + ", sequenceNumber = " + sequenceNumber +
            ", metaDataLength = " + metaDataLength +
            ", reattemptOffset = " + reattemptOffset);*/

        buffer.putInt(reattemptOffset, ENQ_MSG);
        reattemptOffset += SIZE_OF_INT;

        buffer.putInt(reattemptOffset, sequenceNumber);
        reattemptOffset += SIZE_OF_INT;

        buffer.putInt(reattemptOffset, bodyLength);
        reattemptOffset += SIZE_OF_INT;

        buffer.putBytes(reattemptOffset, srcBuffer, srcOffst, bodyLength);
        reattemptOffset += bodyLength;

        buffer.putInt(reattemptOffset, metaDataLength);
        reattemptOffset += SIZE_OF_INT;

        buffer.putBytes(reattemptOffset, srcBuffer, metaDataOffset, metaDataLength);
    }

    private void enqueueReplayComplete(final long correlationId)
    {
        enqueueCorrelation(correlationId, ENQ_REPLAY_COMPLETE);
    }

    private void enqueueStartReplay(final long correlationId)
    {
        enqueueCorrelation(correlationId, ENQ_START_REPLAY);
    }

    private void enqueueCorrelation(final long correlationId, final int messageType)
    {
        final ReattemptState reattemptState = enqueue(ENQ_REPLAY_COMPLETE_LEN, true);

        int reattemptOffset = reattemptState.usage;
        final ExpandableDirectByteBuffer buffer = reattemptState.buffer();

        buffer.putInt(reattemptOffset, messageType);
        reattemptOffset += SIZE_OF_INT;

        buffer.putLong(reattemptOffset, correlationId);
    }

    private ReattemptState enqueue(final int length, final boolean replay)
    {
//        System.out.println("FixSenderEndPoint.enqueue, length = " + length + ", replay = " + replay);
        // we only need re-attempting when we've got messages buffered for the current state
        final boolean currentStream = replay == replaying;
        if (!requiresReattempting && currentStream)
        {
            requiresReattempting = true;
            sendSlowStatus(true);
        }

        final ReattemptState reattemptState = reattemptState(replay);

        final int bufferUsage = reattemptState.usage + length;
        reattemptState.usage = bufferUsage;
        if (currentStream)
        {
            if (bufferUsage > maxBytesInBuffer)
            {
                removeEndpoint(SLOW_CONSUMER);
            }

            bytesInBuffer.setOrdered(bufferUsage);
        }
        return reattemptState;
    }

    private void removeEndpoint(final DisconnectReason reason)
    {
        framer.onDisconnect(libraryId, connectionId, reason);
    }

    private ReattemptState reattemptState(final boolean replay)
    {
        return replay ? replayBuffer : normalBuffer;
    }

    private boolean processReattemptBuffer(final boolean replay)
    {
        final ReattemptState reattemptState = reattemptState(replay);
        final ExpandableDirectByteBuffer buffer = reattemptState.buffer;
        final int reattemptBufferUsage = reattemptState.usage;
//        System.out.println("FixSenderEndPoint.processReattemptBuffer, reattemptBufferUsage = " +
//        reattemptBufferUsage);
        if (reattemptBufferUsage == 0)
        {
            return true;
        }

        int offset = 0;
        while (offset < reattemptBufferUsage)
        {
            try
            {
                final int enqueueType = buffer.getInt(offset);
//                System.out.println("enqueueType = " + enqueueType + ", offset = " + offset);
                if (enqueueType == ENQ_MSG)
                {
                    final int sequenceNumberOffset = offset + SIZE_OF_INT;
                    final int sequenceNumber = buffer.getInt(sequenceNumberOffset);
//                    System.out.println("sequenceNumber = " + sequenceNumber);

                    final int bodyLengthOffset = sequenceNumberOffset + SIZE_OF_INT;
                    final int bodyLength = buffer.getInt(bodyLengthOffset);

                    final int bodyOffset = bodyLengthOffset + SIZE_OF_INT;
                    final int written = writeBuffer(buffer, bodyOffset, bodyLength);
                    final int totalWritten = written + reattemptBytesWritten;
                    if (totalWritten < bodyLength)
                    {
                        this.reattemptBytesWritten = totalWritten;
                        break;
                    }
                    else
                    {
                        offset = onProcessMsgComplete(
                            replay, buffer, offset, sequenceNumber, bodyLength, bodyOffset, totalWritten);
                    }
                }
                else if (enqueueType == ENQ_REPLAY_COMPLETE)
                {
                    final int idOffset = offset + SIZE_OF_INT;
                    final long correlationId = buffer.getLong(idOffset);
                    this.reattemptBytesWritten = NO_REATTEMPT;
                    if (super.onReplayComplete(correlationId) == ABORT)
                    {
                        break; // leave it in the buffer
                    }
                    else
                    {
                        // Complete
                        final int endOfReplayEntry = idOffset + SIZE_OF_LONG;

                        // peek the next message to see if we need to continue replaying
                        // If not then we end the replay, otherwise we keep replaying
                        if (buffer.getInt(endOfReplayEntry) != ENQ_START_REPLAY)
                        {
                            replaying = false;
                            reattemptState.shuffleWritten(endOfReplayEntry);
                            bytesInBuffer.setOrdered(normalBuffer.usage);
                            return true;
                        }
                    }
                }
                else if (enqueueType == ENQ_START_REPLAY)
                {
                    // We just ensure that we're still replaying and skip these messages
                    offset += ENQ_START_REPLAY_LEN;
                }
                else
                {
                    throw new IllegalStateException(
                        "enqueueType = " + enqueueType + ", usage = " + reattemptState.usage + ", offset = " + offset +
                        ", replay = " + replay);
                }
            }
            catch (final Throwable e)
            {
                e.printStackTrace();
                onError(e);
                return true;
            }
        }

        final int usage = reattemptState.shuffleWritten(offset);
        bytesInBuffer.setOrdered(usage);
        return usage == 0;
    }

    private int onProcessMsgComplete(
        final boolean replay,
        final ExpandableDirectByteBuffer buffer,
        final int offset,
        final int sequenceNumber,
        final int bodyLength,
        final int bodyOffset,
        final int totalWritten)
    {
        final int metaDataLengthOffset = bodyOffset + bodyLength;
        final int metaDataLength = buffer.getInt(metaDataLengthOffset);

        final int metaDataOffset = metaDataLengthOffset + SIZE_OF_INT;

        final MessageTimingHandler messageTimingHandler = this.messageTimingHandler;
        if (messageTimingHandler != null && !replay)
        {
            messageTimingHandler.onMessage(
                sequenceNumber, connectionId, buffer, metaDataOffset, metaDataLength);
        }

        this.reattemptBytesWritten = NO_REATTEMPT;

        return offset + ENQ_MESSAGE_BLOCK_LEN + totalWritten + metaDataLength;
    }

    public boolean reattempt()
    {
        return reattempt(replaying);
    }

    private boolean reattempt(final boolean replaying)
    {
//        System.out.println("FixSenderEndPoint.reattempt");
        final boolean caughtUp = processReattemptBuffer(replaying);
        if (caughtUp)
        {
            if (requiresReattempting)
            {
                requiresReattempting = false;
                sendSlowStatus(false);
            }
        }
        return caughtUp;
    }

    Action onReplayMessage(
        final DirectBuffer directBuffer,
        final int offset,
        final int bodyLength,
        final long timeInMs,
        final Header header)
    {
        onMessage(directBuffer, offset, bodyLength, 0, 0, timeInMs, true);

        return CONTINUE;
    }

    private void updateSendingTimeoutTimeInMs(final long timeInMs, final int written)
    {
        if (written > 0)
        {
            sendingTimeoutTimeInMs = timeInMs + slowConsumerTimeoutInMs;
        }
    }

    private void onError(final Throwable ex)
    {
        errorHandler.onError(new Exception(String.format(
            "Exception reported for sessionId=%d,connectionId=%d", sessionId, connectionId), ex));
        disconnectEndpoint(EXCEPTION);
    }

    public void close()
    {
        senderSequenceNumber.close();
        invalidLibraryAttempts.close();
        super.close();
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
        reattempt();

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

    public Action onReplayComplete(final long correlationId)
    {
        if (IS_REPLAY_LOG_TAG_ENABLED)
        {
            DebugLogger.log(LogTag.REPLAY,
                formatters.replayComplete.clear().with(connectionId).with(correlationId));
        }

        if (!replaying || !reattempt(true))
        {
//            System.out.println("FixSenderEndPoint.onReplayComplete enqueued " + replaying);
            enqueueReplayComplete(correlationId);
            return CONTINUE;
        }

        final Action action = super.onReplayComplete(correlationId);
        if (action == ABORT)
        {
//            System.out.println("FixSenderEndPoint.onReplayComplete, action = " + action);
            enqueueReplayComplete(correlationId);
        }

        replaying = false;
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

    // Received on outbound publication when a replay starts
    public void onValidResendRequest(final long correlationId)
    {
        if (IS_REPLAY_LOG_TAG_ENABLED)
        {
            DebugLogger.log(LogTag.REPLAY, formatters.validResendRequest.clear()
                .with(connectionId).with(correlationId));
        }
    }

    // Receive from replayer
    public void onStartReplay(final long correlationId)
    {
        if (IS_REPLAY_LOG_TAG_ENABLED)
        {
            DebugLogger.log(LogTag.REPLAY, formatters.checkStartReplay.clear()
                .with(connectionId).with(correlationId));
        }

        // We start the replay with this message, rather than VRR because it doesn't race with replay complete.
        if (replaying)
        {
            // Always goes on the replaying buffer
            enqueueStartReplay(correlationId);
        }
        else
        {
            replaying = true;
        }
    }

    public String toString()
    {
        return "FixSenderEndPoint{" +
            "connectionId=" + connectionId +
            ", sessionId=" + sessionId +
            ", sessionKey=" + sessionKey +
            "} " + super.toString();
    }

    static class ReattemptState
    {
        ExpandableDirectByteBuffer buffer;
        int usage;

        ExpandableDirectByteBuffer buffer()
        {
            ExpandableDirectByteBuffer buffer = this.buffer;
            if (buffer == null)
            {
                buffer = this.buffer = new ExpandableDirectByteBuffer();
            }

            buffer.checkLimit(usage);

            return buffer;
        }

        int shuffleWritten(final int written)
        {
            int usage = this.usage;
            if (written > 0)
            {
                usage -= written;
                buffer.putBytes(0, buffer, written, usage);
                this.usage = usage;
            }
            return usage;
        }
    }
}
