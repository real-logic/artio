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

import uk.co.real_logic.agrona.LangUtil;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

public final class ServerSocketChannelTransport extends TcpChannelTransport implements AutoCloseable
{
    private final ServerSocketChannel channel;
    private final Framer framer;

    public ServerSocketChannelTransport(final InetSocketAddress address, final Framer framer) throws IOException
    {
        this.framer = framer;
        channel = ServerSocketChannel.open();
        channel.bind(address).configureBlocking(false);
    }

    public void register(final Selector selector) throws IOException
    {
        channel.register(selector, SelectionKey.OP_ACCEPT);
    }

    public int pollForData() throws IOException
    {
        final SocketChannel socketChannel = channel.accept();
        if (socketChannel != null)
        {
            framer.onAcceptConnection(socketChannel);
            return 1;
        }
        return 0;
    }

    public void close()
    {
        try
        {
            channel.close();
        }
        catch (IOException e)
        {
            LangUtil.rethrowUnchecked(e);
        }
    }
}
