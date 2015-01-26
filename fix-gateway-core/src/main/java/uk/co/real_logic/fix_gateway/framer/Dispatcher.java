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
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.util.Iterator;
import java.util.Set;

/**
 * Handles incoming connections from clients and outgoing connections to exchanges.
 */
public class Dispatcher implements Agent
{

    private final ServerSocketChannel listeningChannel;
    private final ConnectionHandler connectionHandler;
    private final Selector selector;

    public Dispatcher(final SocketAddress address, ConnectionHandler connectionHandler)
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
        selector.selectNow();

        final Set<SelectionKey> keys = selector.selectedKeys();
        for (Iterator<SelectionKey> it = keys.iterator(); it.hasNext(); )
        {
            final SelectionKey key = it.next();
            if (key.isAcceptable())
            {
                ReceiveEndPoint endPoint = connectionHandler.onNewConnection(listeningChannel.accept());
                endPoint.register(selector);
            }
            /*else
            if (key.isReadable())
            {

            }*/

            it.remove();
        }

        return 0;
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
