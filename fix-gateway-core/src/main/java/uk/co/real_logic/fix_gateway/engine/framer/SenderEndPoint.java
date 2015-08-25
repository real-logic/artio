/*
 * Copyright 2015 Real Logic Ltd.
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

import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.ErrorHandler;
import uk.co.real_logic.agrona.concurrent.AtomicCounter;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.fix_gateway.DebugLogger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

// TODO: make sender end points always, consistently, be removed when sender end points are
public class SenderEndPoint
{
    private final long connectionId;
    private final SocketChannel channel;
    private final IdleStrategy idleStrategy;
    private final AtomicCounter messagesWritten;
    private final ErrorHandler errorHandler;

    public SenderEndPoint(
        final long connectionId,
        final SocketChannel channel,
        final IdleStrategy idleStrategy,
        final AtomicCounter messagesWritten,
        final ErrorHandler errorHandler)
    {
        this.connectionId = connectionId;
        this.channel = channel;
        this.idleStrategy = idleStrategy;
        this.messagesWritten = messagesWritten;
        this.errorHandler = errorHandler;
    }

    public void onFramedMessage(final DirectBuffer directBuffer, final int offset, final int length)
    {
        final ByteBuffer buffer = directBuffer.byteBuffer();
        buffer.limit(offset + length);
        buffer.position(offset);

        try
        {
            int bytesWritten = 0;
            while (bytesWritten < length)
            {
                final int written = channel.write(buffer);
                DebugLogger.log("Written  %s\n", buffer, written);
                messagesWritten.orderedIncrement();
                bytesWritten += written;
                idleStrategy.idle(1);
            }
        }
        catch (final IOException ex)
        {
            errorHandler.onError(ex);
        }
    }

    public long connectionId()
    {
        return connectionId;
    }
}
