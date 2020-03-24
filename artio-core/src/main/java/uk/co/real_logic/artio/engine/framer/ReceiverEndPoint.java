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
package uk.co.real_logic.artio.engine.framer;

import org.agrona.ErrorHandler;
import uk.co.real_logic.artio.DebugLogger;
import uk.co.real_logic.artio.messages.DisconnectReason;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;

import static java.nio.channels.SelectionKey.OP_READ;
import static uk.co.real_logic.artio.LogTag.FIX_MESSAGE_TCP;
import static uk.co.real_logic.artio.messages.DisconnectReason.REMOTE_DISCONNECT;

public abstract class ReceiverEndPoint
{
    protected static final int SOCKET_DISCONNECTED = -1;

    protected final TcpChannel channel;
    protected final long connectionId;
    protected boolean hasDisconnected = false;
    protected final MutableAsciiBuffer buffer;
    protected final ByteBuffer byteBuffer;
    protected final ErrorHandler errorHandler;
    protected final Framer framer;

    protected int usedBufferData = 0;
    protected SelectionKey selectionKey;

    public ReceiverEndPoint(
        final TcpChannel channel,
        final long connectionId,
        final int bufferSize,
        final ErrorHandler errorHandler,
        final Framer framer)
    {
        this.channel = channel;
        this.connectionId = connectionId;
        this.errorHandler = errorHandler;
        this.framer = framer;

        byteBuffer = ByteBuffer.allocateDirect(bufferSize);
        buffer = new MutableAsciiBuffer(byteBuffer);
    }

    int readData() throws IOException
    {
        final int dataRead = channel.read(byteBuffer);
        if (dataRead != SOCKET_DISCONNECTED)
        {
            if (dataRead > 0)
            {
                DebugLogger.log(FIX_MESSAGE_TCP, "Read     ", buffer, 0, dataRead);
            }
            usedBufferData += dataRead;
        }
        else
        {
            onDisconnectDetected();
        }

        return dataRead;
    }

    long connectionId()
    {
        return connectionId;
    }

    void register(final Selector selector) throws IOException
    {
        selectionKey = channel.register(selector, OP_READ, this);
    }

    void onDisconnectDetected()
    {
        completeDisconnect(REMOTE_DISCONNECT);
    }

    void close(final DisconnectReason reason)
    {
        closeResources();

        if (!hasDisconnected)
        {
            disconnectEndpoint(reason);
        }
    }

    void completeDisconnect(final DisconnectReason reason)
    {
        disconnectEndpoint(reason);
        removeEndpointFromFramer();
    }

    abstract void removeEndpointFromFramer();

    abstract void disconnectEndpoint(DisconnectReason reason);

    abstract int poll();

    abstract boolean retryFrameMessages();

    abstract boolean requiresAuthentication();

    abstract void closeResources();
}
