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

import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.status.AtomicCounter;
import uk.co.real_logic.fix_gateway.DebugLogger;
import uk.co.real_logic.fix_gateway.engine.framer.SubscriptionSlowPeeker.LibrarySlowPeeker;
import uk.co.real_logic.fix_gateway.engine.logger.ArchiveDescriptor;
import uk.co.real_logic.fix_gateway.messages.DisconnectReason;

import java.io.IOException;
import java.nio.ByteBuffer;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;
import static uk.co.real_logic.fix_gateway.LogTag.FIX_MESSAGE;
import static uk.co.real_logic.fix_gateway.engine.FixEngine.ENGINE_LIBRARY_ID;
import static uk.co.real_logic.fix_gateway.messages.DisconnectReason.EXCEPTION;
import static uk.co.real_logic.fix_gateway.messages.DisconnectReason.SLOW_CONSUMER;
import static uk.co.real_logic.fix_gateway.protocol.GatewayPublication.FRAME_SIZE;

class SenderEndPoint implements AutoCloseable
{
    private final long connectionId;
    private final TcpChannel channel;
    private final AtomicCounter bytesInBuffer;
    private final AtomicCounter invalidLibraryAttempts;
    private final ErrorHandler errorHandler;
    private final Framer framer;
    private final int maxBytesInBuffer;
    private final long slowConsumerTimeoutInMs;

    private long sentOutboundPosition;
    private long sentReplayPosition;
    private boolean partiallySentOutboundMessage = false;
    private boolean partiallySentReplayMessage = false;
    private SlowPeeker outboundSlowPeeker;
    private final SlowPeeker replaySlowPeeker;
    private long skipOutboundPosition = Long.MAX_VALUE;
    private long skipReplayPosition = Long.MAX_VALUE;

    private int libraryId;
    private long sessionId;
    private long sendingTimeoutTimeInMs;

    SenderEndPoint(
        final long connectionId,
        final int libraryId,
        final SlowPeeker outboundSlowPeeker,
        final TcpChannel channel,
        final AtomicCounter bytesInBuffer,
        final AtomicCounter invalidLibraryAttempts,
        final ErrorHandler errorHandler,
        final Framer framer,
        final int maxBytesInBuffer,
        final long slowConsumerTimeoutInMs,
        final long timeInMs,
        final SlowPeeker replaySlowPeeker)
    {
        this.connectionId = connectionId;
        this.libraryId = libraryId;
        this.outboundSlowPeeker = outboundSlowPeeker;
        this.channel = channel;
        this.bytesInBuffer = bytesInBuffer;
        this.invalidLibraryAttempts = invalidLibraryAttempts;
        this.errorHandler = errorHandler;
        this.framer = framer;
        this.maxBytesInBuffer = maxBytesInBuffer;
        this.slowConsumerTimeoutInMs = slowConsumerTimeoutInMs;
        this.replaySlowPeeker = replaySlowPeeker;

        sendingTimeoutTimeInMs = timeInMs + slowConsumerTimeoutInMs;
    }

    void onOutboundMessage(
        final int libraryId,
        final DirectBuffer directBuffer,
        final int offset,
        final int bodyLength,
        final long position,
        final long timeInMs)
    {
        if (isWrongLibraryId(libraryId))
        {
            invalidLibraryAttempts.increment();
            return;
        }

        attemptFramedMessage(directBuffer, offset, bodyLength, timeInMs, position, true);
    }

    Action onReplayMessage(
        final DirectBuffer directBuffer,
        final int offset,
        final int bodyLength,
        final long timeInMs,
        final long position)
    {
        attemptFramedMessage(directBuffer, offset, bodyLength, timeInMs, position, false);

        return CONTINUE;
    }

    Action onSlowReplayMessage(
        final DirectBuffer buffer,
        final int offset,
        final int bodyLength,
        final long timeInMs,
        final long position)
    {
        final int offsetAfterHeader = offset - FRAME_SIZE;
        final int length = bodyLength + FRAME_SIZE;

        return attemptSlowMessage(buffer, offsetAfterHeader, length, position, bodyLength, timeInMs, false);
    }

    private void attemptFramedMessage(
        final DirectBuffer directBuffer,
        final int offset,
        final int bodyLength,
        final long timeInMs,
        final long position,
        final boolean outbound)
    {
        if (isSlowConsumer())
        {
            final long bytesInBuffer = bytesInBufferWeak() + bodyLength;
            if (bytesInBuffer > maxBytesInBuffer)
            {
                removeEndpoint(SLOW_CONSUMER);
            }

            this.bytesInBuffer.setOrdered(bytesInBuffer);

            return;
        }

        try
        {
            final int written = writeFramedMessage(directBuffer, offset, bodyLength, timeInMs);

            if (written != bodyLength)
            {
                becomeSlowConsumer(written, bodyLength, position, outbound);
            }
            else
            {
                setPosition(outbound, position);
            }
        }
        catch (final IOException ex)
        {
            onError(ex);
        }
    }

    private int writeFramedMessage(
        final DirectBuffer directBuffer,
        final int offset,
        final int length,
        final long timeInMs)
        throws IOException
    {
        final int wrapAdjustment = directBuffer.wrapAdjustment();
        final ByteBuffer buffer = directBuffer.byteBuffer();
        buffer.limit(wrapAdjustment + offset + length);
        buffer.position(wrapAdjustment + offset);

        final int written = channel.write(buffer);
        DebugLogger.log(FIX_MESSAGE, "Written  %s\n", buffer, written);
        updateSendingTimeoutTimeInMs(timeInMs, written);
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
        errorHandler.onError(new Exception(
            String.format("Exception reported for sessionId=%d,connectionId=%d", sessionId, connectionId), ex));
        removeEndpoint(EXCEPTION);
    }

    void becomeSlowConsumer(
        final int written, final int bodyLength, final long position, final boolean outbound)
    {
        final int remainingBytes = bodyLength - written;
        bytesInBuffer.setOrdered(remainingBytes);
        sendSlowStatus(true);
        setPosition(outbound, position - remainingBytes);
        setPartiallySent(outbound, true);
    }

    private void becomeNormalConsumer()
    {
        sendSlowStatus(false);
    }

    private void setPartiallySent(final boolean outbound, final boolean value)
    {
        if (outbound)
        {
            partiallySentOutboundMessage = value;
        }
        else
        {
            partiallySentReplayMessage = value;
        }
    }

    private void setPosition(final boolean outbound, final long newPosition)
    {
        if (outbound)
        {
            sentOutboundPosition = newPosition;
        }
        else
        {
            sentReplayPosition = newPosition;
        }
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

    public void libraryId(final int libraryId, final LibrarySlowPeeker librarySlowPeeker)
    {
        this.libraryId = libraryId;
        this.outboundSlowPeeker = librarySlowPeeker;
    }

    public void close()
    {
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
        final long timeInMs)
    {
        if (isWrongLibraryId(libraryId))
        {
            invalidLibraryAttempts.increment();
            return CONTINUE;
        }

        return attemptSlowMessage(directBuffer, offsetAfterHeader, length, position, bodyLength, timeInMs, true);
    }

    private Action attemptSlowMessage(
        final DirectBuffer directBuffer,
        final int offsetAfterHeader,
        final int length,
        final long position,
        final int bodyLength,
        final long timeInMs,
        final boolean outbound)
    {
        if (!isSlowConsumer())
        {
            return CONTINUE;
        }

        // Skip all messages beyond the skip position, since this endpoint has been blocked but others
        // Scanning forward.
        final long skipPosition = getSkipPosition(outbound);
        if (position > skipPosition)
        {
            return CONTINUE;
        }

        // Skip messages where the end point has become a slow consumer, but
        // the slow consumer stream hasn't polled up to update with the regular stream
        final long sentPosition = getPosition(outbound);
        if (position <= sentPosition)
        {
            return CONTINUE;
        }

        if (partiallySentOtherStream(outbound))
        {
            return blockPosition(position, length, outbound);
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
                remainingLength = (int) (position - sentPosition);
                bytesPreviouslySent = bodyLength - remainingLength;
            }

            final int dataOffset = offsetAfterHeader + FRAME_SIZE + bytesPreviouslySent;
            final ByteBuffer buffer = directBuffer.byteBuffer();

            final int wrapAdjustment = directBuffer.wrapAdjustment();
            buffer.limit(wrapAdjustment + dataOffset + remainingLength);
            ByteBufferUtil.position(buffer, wrapAdjustment + dataOffset);

            final int written = channel.write(buffer);
            bytesInBuffer.addOrdered(-written);

            updateSendingTimeoutTimeInMs(timeInMs, written);

            if (bodyLength > (written + bytesPreviouslySent))
            {
                movePosition(outbound, written);
                return blockPosition(position, length, outbound);
            }
            else
            {
                setPosition(outbound, position);
                setPartiallySent(outbound, false);
                setSkipPosition(Long.MAX_VALUE, outbound);

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

    private Action blockPosition(final long position, final int length, final boolean outbound)
    {
        final SlowPeeker slowPeeker = outbound ? outboundSlowPeeker : replaySlowPeeker;
        final int alignedLength = ArchiveDescriptor.alignTerm(length);
        final long startPosition = position - (alignedLength + DataHeaderFlyweight.HEADER_LENGTH);
        slowPeeker.blockPosition(startPosition);
        setSkipPosition(position, outbound);
        return Action.CONTINUE;
    }

    private boolean partiallySentOtherStream(final boolean outbound)
    {
        return outbound && partiallySentReplayMessage || !outbound && partiallySentOutboundMessage;
    }

    private long getPosition(final boolean outbound)
    {
        return outbound ? sentOutboundPosition : sentReplayPosition;
    }

    private long getSkipPosition(final boolean outbound)
    {
        return outbound ? skipOutboundPosition : skipReplayPosition;
    }

    private void setSkipPosition(final long position, final boolean outbound)
    {
        if (outbound)
        {
            skipOutboundPosition = position;
        }
        else
        {
            skipReplayPosition = position;
        }
    }

    private void movePosition(final boolean outbound, final int by)
    {
        if (outbound)
        {
            sentOutboundPosition += by;
        }
        else
        {
            sentReplayPosition += by;
        }
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
}
