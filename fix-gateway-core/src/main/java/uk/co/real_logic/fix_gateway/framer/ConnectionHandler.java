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
package uk.co.real_logic.fix_gateway.framer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Handles incoming connections including setting up framers.
 *
 * Threadsafe.
 */
public class ConnectionHandler
{

    private final AtomicLong idSource = new AtomicLong(0);

    private final int bufferSize;
    private final MessageHandler messageHandler;

    public ConnectionHandler(final int bufferSize, final MessageHandler messageHandler)
    {
        this.bufferSize = bufferSize;
        this.messageHandler = messageHandler;
    }

    public Connection createConnection(final SocketChannel channel) throws IOException
    {
        final ReceiverEndPoint receiverEndPoint = new ReceiverEndPoint(channel, bufferSize, messageHandler);
        final SenderEndPoint senderEndPoint = new SenderEndPoint(channel);
        final long connectionId = idSource.getAndIncrement();
        final InetSocketAddress remoteAddress = (InetSocketAddress) channel.getRemoteAddress();
        return new Connection(connectionId, remoteAddress, receiverEndPoint, senderEndPoint);
    }

}
