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
import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.status.AtomicCounter;
import uk.co.real_logic.fix_gateway.DebugLogger;
import uk.co.real_logic.fix_gateway.messages.DisconnectReason;

import java.io.IOException;
import java.nio.ByteBuffer;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.ABORT;
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

    private long sentPosition;
    private int libraryId;
    private long sessionId;

    SenderEndPoint(
        final long connectionId,
        final int libraryId,
        final TcpChannel channel,
        final AtomicCounter bytesInBuffer,
        final AtomicCounter invalidLibraryAttempts,
        final ErrorHandler errorHandler,
        final Framer framer,
        final int maxBytesInBuffer)
    {
        this.connectionId = connectionId;
        this.libraryId = libraryId;
        this.channel = channel;
        this.bytesInBuffer = bytesInBuffer;
        this.invalidLibraryAttempts = invalidLibraryAttempts;
        this.errorHandler = errorHandler;
        this.framer = framer;
        this.maxBytesInBuffer = maxBytesInBuffer;
    }

    void onNormalFramedMessage(
        final int libraryId,
        final DirectBuffer directBuffer,
        final int offset,
        final int length,
        final long position)
    {
        if (isWrongLibraryId(libraryId))
        {
            invalidLibraryAttempts.increment();
            return;
        }

        if (isSlowConsumer())
        {
            final long bytesInBuffer = bytesInBufferWeak() + length;
            if (bytesInBuffer > maxBytesInBuffer)
            {
                removeEndpoint(SLOW_CONSUMER);
            }

            this.bytesInBuffer.setOrdered(bytesInBuffer);

            return;
        }

        try
        {
            final int written = writeFramedMessage(directBuffer, offset, length);

            if (written != length)
            {
                becomeSlowConsumer(written, length, position);
            }
            else
            {
                sentPosition = position;
            }
        }
        catch (final IOException ex)
        {
            onError(ex);
        }
    }

    Action onReplayFramedMessage(
        final DirectBuffer directBuffer,
        final int offset,
        final int length)
    {
        try
        {
            final int written = writeFramedMessage(directBuffer, offset, length);
            if (written == 0)
            {
                return ABORT;
            }
            else if (written != length)
            {
                onError(new IllegalArgumentException(String.format(
                    "Failed to replay to (%2$d, %2$d) message writing down TCP connection, dropped [%3$s]",
                    connectionId,
                    sessionId,
                    directBuffer.getStringWithoutLengthUtf8(offset + written, length - written))));
            }
        }
        catch (final IOException ex)
        {
            onError(ex);
        }

        return CONTINUE;
    }

    private int writeFramedMessage(
        final DirectBuffer directBuffer,
        final int offset,
        final int length)
        throws IOException
    {
        final int wrapAdjustment = directBuffer.wrapAdjustment();
        final ByteBuffer buffer = directBuffer.byteBuffer();
        buffer.limit(wrapAdjustment + offset + length);
        buffer.position(wrapAdjustment + offset);

        final int written = channel.write(buffer);
        DebugLogger.log(FIX_MESSAGE, "Written  %s\n", buffer, written);
        return written;
    }

    private void onError(final Exception ex)
    {
        errorHandler.onError(ex);
        removeEndpoint(EXCEPTION);
    }

    private void becomeSlowConsumer(final int written, final int length, final long position)
    {
        final int remainingBytes = length - written;
        bytesInBuffer.setOrdered(remainingBytes);
        sentPosition = position - remainingBytes;
        sendSlowStatus(true);
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

    public void libraryId(final int libraryId)
    {
        this.libraryId = libraryId;
    }

    public void close()
    {
        bytesInBuffer.close();
        invalidLibraryAttempts.close();
    }

    Action onSlowConsumerMessageFragment(
        final DirectBuffer directBuffer,
        final int offsetAfterHeader,
        final int length,
        final long position,
        final int bodyLength,
        final int libraryId)
    {
        if (isWrongLibraryId(libraryId))
        {
            invalidLibraryAttempts.increment();
            return CONTINUE;
        }

        if (!isSlowConsumer())
        {
            return CONTINUE;
        }

        // Skip messages where the end point has become a slow consumer, but
        // the slow consumer stream hasn't polled up to update with the regular stream
        if (position <= sentPosition)
        {
            return CONTINUE;
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

            if (bodyLength > (written + bytesPreviouslySent))
            {
                sentPosition += written;
                return ABORT;
            }
            else
            {
                sentPosition = position;

                if (!isSlowConsumer())
                {
                    sendSlowStatus(false);
                }
            }
        }
        catch (final IOException ex)
        {
            onError(ex);
        }

        return CONTINUE;
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

    void onLogon(final long sessionId)
    {
        this.sessionId = sessionId;
    }

    public long onLogon()
    {
        return sessionId;
    }
}
