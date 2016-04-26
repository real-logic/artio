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
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import uk.co.real_logic.fix_gateway.DebugLogger;
import uk.co.real_logic.fix_gateway.messages.DisconnectReason;
import uk.co.real_logic.fix_gateway.messages.FixMessageDecoder;
import uk.co.real_logic.fix_gateway.messages.FixMessageEncoder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.ABORT;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;
import static uk.co.real_logic.fix_gateway.messages.DisconnectReason.EXCEPTION;
import static uk.co.real_logic.fix_gateway.messages.DisconnectReason.SLOW_CONSUMER;
import static uk.co.real_logic.fix_gateway.protocol.GatewayPublication.FRAME_SIZE;

class SenderEndPoint implements AutoCloseable
{
    private final FixMessageEncoder fixMessageEncoder = new FixMessageEncoder();
    private final long connectionId;
    private final SocketChannel channel;
    private final AtomicCounter messageWrites;
    private final ErrorHandler errorHandler;
    private final Framer framer;
    private final int maxBytesInBuffer;

    private int bytesInbuffer = 0;
    private int libraryId;

    SenderEndPoint(
        final long connectionId,
        final int libraryId,
        final SocketChannel channel,
        final AtomicCounter messageWrites,
        final ErrorHandler errorHandler,
        final Framer framer,
        final int maxBytesInBuffer)
    {
        this.connectionId = connectionId;
        this.libraryId = libraryId;
        this.channel = channel;
        this.messageWrites = messageWrites;
        this.errorHandler = errorHandler;
        this.framer = framer;
        this.maxBytesInBuffer = maxBytesInBuffer;
    }

    void onNormalFramedMessage(
        final DirectBuffer directBuffer,
        final int offset,
        final int length)
    {
        if (isSlowConsumer())
        {
            bytesInbuffer += length;

            if (bytesInbuffer > maxBytesInBuffer)
            {
                removeEndpoint(SLOW_CONSUMER);
            }

            return;
        }

        try
        {
            final ByteBuffer buffer = directBuffer.byteBuffer();
            buffer.limit(offset + length);
            buffer.position(offset);

            final int written = channel.write(buffer);
            DebugLogger.log("Written  %s\n", buffer, written);
            messageWrites.orderedIncrement();

            if (written != length)
            {
                becomeSlowConsumer(directBuffer, offset, written, length);
            }
        }
        catch (final IOException ex)
        {
            onError(ex);
        }
    }

    private void onError(final IOException ex)
    {
        errorHandler.onError(ex);
        removeEndpoint(EXCEPTION);
    }

    private void becomeSlowConsumer(final DirectBuffer buffer, final int offset, final int written, final int length)
    {
        bytesInbuffer = length - written;
        if (written > 0)
        {
            updateBytesSent(buffer, offset - FRAME_SIZE, written);
        }
    }

    private void updateBytesSent(final DirectBuffer buffer, final int offset, final int bytesSent)
    {
        fixMessageEncoder
            .wrap((MutableDirectBuffer) buffer, offset)
            .bytesSent(bytesSent);
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

    public int libraryId()
    {
        return libraryId;
    }

    public void close()
    {
        messageWrites.close();
    }

    Action onSlowMessageFragment(
        final FixMessageDecoder fixMessage,
        final DirectBuffer directBuffer,
        final int offset,
        final int length)
    {
        if (!isSlowConsumer())
        {
            return CONTINUE;
        }

        int bytesSent = fixMessage.bytesSent();
        final int bodyLength = fixMessage.bodyLength();
        if (bodyLength == bytesSent)
        {
            return CONTINUE;
        }

        try
        {
            final int dataOffset = offset + FRAME_SIZE + bytesSent;
            final ByteBuffer buffer = directBuffer.byteBuffer();
            buffer.limit(offset + length);
            buffer.position(dataOffset);

            final int written = channel.write(buffer);
            messageWrites.orderedIncrement();

            bytesSent += written;
            if (bodyLength > bytesSent)
            {
                updateBytesSent(directBuffer, fixMessage.offset(), bytesSent);
                return ABORT;
            }
        }
        catch (final IOException ex)
        {
            onError(ex);
        }

        return CONTINUE;
    }

    private boolean isSlowConsumer()
    {
        return bytesInbuffer > 0;
    }
}
