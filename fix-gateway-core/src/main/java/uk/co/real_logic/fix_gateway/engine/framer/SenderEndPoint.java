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

import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.status.AtomicCounter;
import uk.co.real_logic.fix_gateway.DebugLogger;
import uk.co.real_logic.fix_gateway.ReliefValve;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

class SenderEndPoint implements AutoCloseable
{
    private final long connectionId;
    private final SocketChannel channel;
    private final IdleStrategy idleStrategy;
    private final AtomicCounter messageWrites;
    private final ErrorHandler errorHandler;
    private final Framer framer;
    private final ReliefValve reliefValve;

    private int libraryId;

    SenderEndPoint(
        final long connectionId,
        final int libraryId,
        final SocketChannel channel,
        final IdleStrategy idleStrategy,
        final AtomicCounter messageWrites,
        final ErrorHandler errorHandler,
        final Framer framer,
        final ReliefValve reliefValve)
    {
        this.connectionId = connectionId;
        this.libraryId = libraryId;
        this.channel = channel;
        this.idleStrategy = idleStrategy;
        this.messageWrites = messageWrites;
        this.errorHandler = errorHandler;
        this.framer = framer;
        this.reliefValve = reliefValve;
    }

    public void onFramedMessage(final DirectBuffer directBuffer, final int offset, final int length)
    {
        final SocketChannel channel = this.channel;
        final AtomicCounter messageWrites = this.messageWrites;
        final IdleStrategy idleStrategy = this.idleStrategy;
        final ReliefValve reliefValve = this.reliefValve;

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
                messageWrites.orderedIncrement();
                bytesWritten += written;
                if (written == 0)
                {
                    idleStrategy.idle(reliefValve.vent());
                }
            }
        }
        catch (final IOException ex)
        {
            errorHandler.onError(ex);
            removeEndpoint();
        }
    }

    private void removeEndpoint()
    {
        framer.onDisconnect(libraryId, connectionId, null);
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
}
