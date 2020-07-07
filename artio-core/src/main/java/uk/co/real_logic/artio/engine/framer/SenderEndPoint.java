/*
 * Copyright 2015-2020 Real Logic Limited.
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

import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.status.AtomicCounter;
import uk.co.real_logic.artio.DebugLogger;
import uk.co.real_logic.artio.engine.ByteBufferUtil;
import uk.co.real_logic.artio.engine.MessageTimingHandler;
import uk.co.real_logic.artio.engine.SenderSequenceNumber;
import uk.co.real_logic.artio.engine.logger.ArchiveDescriptor;
import uk.co.real_logic.artio.messages.DisconnectReason;
import uk.co.real_logic.artio.messages.MessageHeaderDecoder;

import java.io.IOException;
import java.nio.ByteBuffer;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;
import static uk.co.real_logic.artio.LogTag.FIX_MESSAGE_TCP;
import static uk.co.real_logic.artio.engine.FixEngine.ENGINE_LIBRARY_ID;
import static uk.co.real_logic.artio.messages.DisconnectReason.EXCEPTION;
import static uk.co.real_logic.artio.messages.DisconnectReason.SLOW_CONSUMER;
import static uk.co.real_logic.artio.protocol.GatewayPublication.FRAME_SIZE;

class SenderEndPoint
{
    private static final int HEADER_LENGTH = MessageHeaderDecoder.ENCODED_LENGTH;
    private static final int REPLAY_MESSAGE = -1;

    private final long connectionId;
    private final TcpChannel channel;
    private final AtomicCounter bytesInBuffer;
    private final AtomicCounter invalidLibraryAttempts;
    private final ErrorHandler errorHandler;
    private final Framer framer;
    private final int maxBytesInBuffer;
    private final long slowConsumerTimeoutInMs;
    private final StreamTracker outboundTracker;
    private final StreamTracker replayTracker;
    private final SenderSequenceNumber senderSequenceNumber;
    private final MessageTimingHandler messageTimingHandler;

    private int libraryId;
    private long sessionId;
    private long sendingTimeoutTimeInMs;
    private boolean replayPaused;

    SenderEndPoint(
        final long connectionId,
        final int libraryId,
        final BlockablePosition outboundBlockablePosition,
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
        final MessageTimingHandler messageTimingHandler)
    {
        this.connectionId = connectionId;
        this.libraryId = libraryId;
        this.channel = channel;
        this.bytesInBuffer = bytesInBuffer;
        this.invalidLibraryAttempts = invalidLibraryAttempts;
        this.errorHandler = errorHandler;
        this.framer = framer;
        this.maxBytesInBuffer = maxBytesInBuffer;
        this.slowConsumerTimeoutInMs = slowConsumerTimeoutInMs;
        this.senderSequenceNumber = senderSequenceNumber;

        outboundTracker = new StreamTracker(outboundBlockablePosition);
        replayTracker = new StreamTracker(replayBlockablePosition);
        this.messageTimingHandler = messageTimingHandler;
        sendingTimeoutTimeInMs = timeInMs + slowConsumerTimeoutInMs;
    }

    void onOutboundMessage(
        final int libraryId,
        final DirectBuffer directBuffer,
        final int offset,
        final int bodyLength,
        final int sequenceNumber,
        final long position,
        final long timeInMs)
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

        if (attemptFramedMessage(directBuffer, offset, bodyLength, timeInMs, position, outboundTracker) &&
            messageTimingHandler != null)
        {
            messageTimingHandler.onMessage(sequenceNumber, connectionId);
        }

        senderSequenceNumber.onNewMessage(sequenceNumber);
    }

    Action onReplayMessage(
        final DirectBuffer directBuffer,
        final int offset,
        final int bodyLength,
        final long timeInMs,
        final long position)
    {
        if (!isSlowConsumer())
        {
            replayPaused = true;
        }

        attemptFramedMessage(directBuffer, offset, bodyLength, timeInMs, position, replayTracker);

        return CONTINUE;
    }

    Action onSlowReplayMessage(
        final DirectBuffer buffer,
        final int offset,
        final int bodyLength,
        final long timeInMs,
        final long position,
        final int metaDataLength)
    {
        if (!outboundTracker.partiallySentMessage)
        {
            replayPaused = true;
        }

        final int totalFrameSize = FRAME_SIZE + metaDataLength;
        final int offsetAfterHeader = offset - totalFrameSize;
        final int length = bodyLength + totalFrameSize;

        return attemptSlowMessage(buffer, offsetAfterHeader, length, position, bodyLength, timeInMs, replayTracker,
            metaDataLength, REPLAY_MESSAGE);
    }

    private boolean attemptFramedMessage(
        final DirectBuffer directBuffer,
        final int offset,
        final int bodyLength,
        final long timeInMs,
        final long position,
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
                becomeSlowConsumer(written, bodyLength, position, tracker);
            }
            else
            {
                tracker.sentPosition = position;
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
            removeEndpoint(SLOW_CONSUMER);
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
        if (written > 0)
        {
            ByteBufferUtil.position(buffer, offset);
            DebugLogger.log(FIX_MESSAGE_TCP, "Written  ", buffer, written);
            updateSendingTimeoutTimeInMs(timeInMs, written);

            buffer.limit(startLimit).position(startPosition);
        }

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
        removeEndpoint(EXCEPTION);
    }

    private void becomeSlowConsumer(
        final int written, final int bodyLength, final long position, final StreamTracker tracker)
    {
        final int remainingBytes = bodyLength - written;
        bytesInBuffer.setOrdered(remainingBytes);
        sendSlowStatus(true);
        tracker.sentPosition = position - remainingBytes;
        tracker.partiallySentMessage = true;
    }

    private void becomeNormalConsumer()
    {
        sendSlowStatus(false);
    }

    private void sendSlowStatus(final boolean hasBecomeSlow)
    {
        framer.slowStatus(libraryId, connectionId, hasBecomeSlow);
    }

    private void removeEndpoint(final DisconnectReason reason)
    {
        framer.onDisconnect(libraryId, connectionId, reason);
    }

    public long connectionId()
    {
        return connectionId;
    }

    public void libraryId(final int libraryId, final BlockablePosition blockablePosition)
    {
        this.libraryId = libraryId;
        this.outboundTracker.blockablePosition = blockablePosition;
    }

    public int libraryId()
    {
        return libraryId;
    }

    public void close()
    {
        senderSequenceNumber.close();
        bytesInBuffer.close();
        invalidLibraryAttempts.close();
    }

    Action onSlowOutboundMessage(
        final DirectBuffer directBuffer,
        final int offsetAfterHeader,
        final int length,
        final long position,
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
            return blockPosition(position, length, outboundTracker);
        }

        return attemptSlowMessage(
            directBuffer, offsetAfterHeader, length, position, bodyLength, timeInMs, outboundTracker, metaDataLength,
            sequenceNumber);
    }

    private Action attemptSlowMessage(
        final DirectBuffer directBuffer,
        final int offsetAfterHeader,
        final int length,
        final long position,
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
            return blockPosition(position, length, tracker);
        }

        try
        {
            final long startOfMessage = position - length;
            final int remainingLength;
            final int bytesPreviouslySent;

            // You've complete the stream and there's another message in between.
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

            final int dataOffset = offsetAfterHeader + FRAME_SIZE + metaDataLength + bytesPreviouslySent;
            final ByteBuffer buffer = directBuffer.byteBuffer();

            ByteBufferUtil.limit(buffer, dataOffset + remainingLength);
            ByteBufferUtil.position(buffer, dataOffset);

            final int written = channel.write(buffer);
            bytesInBuffer.getAndAddOrdered(-written);

            updateSendingTimeoutTimeInMs(timeInMs, written);

            if (bodyLength > (written + bytesPreviouslySent))
            {
                tracker.sentPosition = (position - remainingLength) + written;
                return blockPosition(position, length, tracker);
            }
            else
            {
                tracker.sentPosition = position;
                tracker.partiallySentMessage = false;
                tracker.skipPosition = Long.MAX_VALUE;

                if (sequenceNumber != REPLAY_MESSAGE && messageTimingHandler != null)
                {
                    messageTimingHandler.onMessage(sequenceNumber, connectionId);
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

    private Action blockPosition(final long messagePosition, final int messageLength, final StreamTracker tracker)
    {
        final int frameLength = DataHeaderFlyweight.HEADER_LENGTH + messageLength + HEADER_LENGTH;
        final int alignedLength = ArchiveDescriptor.alignTerm(frameLength);
        final long messageStartPosition = messagePosition - alignedLength;
        tracker.blockablePosition.blockPosition(messageStartPosition);
        tracker.skipPosition = messagePosition;
        return Action.CONTINUE;
    }

    private boolean partiallySentOtherStream(final StreamTracker tracker)
    {
        return tracker == outboundTracker ?
            replayTracker.partiallySentMessage :
            outboundTracker.partiallySentMessage;
    }

    private boolean isWrongLibraryId(final int libraryId)
    {
        // We allow the engine's messages to pass through in case the session
        // has been acquired by a library in the same duty cycle as an engine
        // sends a message, which would otherwise result in the message being dropped.
        return !(libraryId == ENGINE_LIBRARY_ID || libraryId == this.libraryId);
    }

    // Only access on Framer thread
    private boolean isSlowConsumer()
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
            removeEndpoint(SLOW_CONSUMER);

            return true;
        }

        return false;
    }

    Action onReplayComplete()
    {
        if (!replayTracker.partiallySentMessage)
        {
            replayPaused = false;
        }

        return CONTINUE;
    }

    // Struct for tracking the slow state of the replay and outbound streams
    static class StreamTracker
    {
        private long sentPosition;
        private long skipPosition = Long.MAX_VALUE;
        private boolean partiallySentMessage = false;
        private BlockablePosition blockablePosition;

        StreamTracker(final BlockablePosition blockablePosition)
        {
            this.blockablePosition = blockablePosition;
        }
    }

    boolean replayPaused()
    {
        return replayPaused;
    }
}
