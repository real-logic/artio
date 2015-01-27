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

import uk.co.real_logic.aeron.common.Agent;

import java.io.IOException;
import java.net.SocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;

import static java.net.StandardSocketOptions.TCP_NODELAY;
import static java.nio.channels.SelectionKey.OP_READ;

/**
 * Handles incoming connections from clients and outgoing connections to exchanges.
 */
public class ReceiveDispatcher implements Agent
{

    private final ServerSocketChannel listeningChannel;
    private final ConnectionHandler connectionHandler;
    private final Selector selector;

    // TODO: add hooks for receive and send buffer sizes
    public ReceiveDispatcher(final SocketAddress address, ConnectionHandler connectionHandler)
    {
        this.connectionHandler = connectionHandler;

        try
        {
            listeningChannel = ServerSocketChannel.open();
            listeningChannel.bind(address).configureBlocking(false);

            selector = Selector.open();
            listeningChannel.register(selector, SelectionKey.OP_ACCEPT);
        }
        catch (IOException e)
        {
            throw new IllegalArgumentException(e);
        }
    }

    @Override
    public int doWork() throws Exception
    {
        return pollSockets();
    }

    private int pollSockets() throws IOException
    {
        final int count = selector.selectNow();

        final Set<SelectionKey> keys = selector.selectedKeys();
        for (Iterator<SelectionKey> it = keys.iterator(); it.hasNext();)
        {
            final SelectionKey key = it.next();
            if (key.isAcceptable())
            {
                final SocketChannel channel = listeningChannel.accept();
                channel.configureBlocking(false);
                channel.setOption(TCP_NODELAY, false);

                ReceiverEndPoint endPoint = connectionHandler.onNewConnection(channel);
                channel.register(selector, OP_READ, endPoint);
            }
            else if (key.isReadable())
            {
                ((ReceiverEndPoint) key.attachment()).receiveData();
            }

            it.remove();
        }

        return count;
    }

    @Override
    public void onClose()
    {
        try
        {
            // JDK on Windows - sigh
            selector.selectNow();
            listeningChannel.close();
        }
        catch (IOException e)
        {
            throw new IllegalStateException(e);
        }
    }

    @Override
    public String roleName()
    {
        return "Dispatcher";
    }

}
