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

import org.agrona.CloseHelper;
import uk.co.real_logic.fix_gateway.engine.EngineConfiguration;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;

import static java.net.StandardSocketOptions.SO_RCVBUF;
import static java.net.StandardSocketOptions.SO_SNDBUF;
import static java.net.StandardSocketOptions.TCP_NODELAY;

/**
 * Mockable class for intercepting network communications
 */
public class TcpChannelSupplier implements AutoCloseable
{
    private final boolean hasBindAddress;
    private final EngineConfiguration configuration;
    private final Selector selector;
    private final ServerSocketChannel listeningChannel;

    public TcpChannelSupplier(final EngineConfiguration configuration)
    {
        hasBindAddress = configuration.hasBindAddress();
        this.configuration = configuration;

        if (hasBindAddress)
        {
            try
            {
                listeningChannel = ServerSocketChannel.open();
                listeningChannel.bind(configuration.bindAddress()).configureBlocking(false);

                selector = Selector.open();
                listeningChannel.register(selector, SelectionKey.OP_ACCEPT);
            }
            catch (final IOException ex)
            {
                throw new IllegalArgumentException(ex);
            }
        }
        else
        {
            listeningChannel = null;
            selector = null;
        }
    }

    public int forEachChannel(final long timeInMs, final NewChannelHandler handler) throws IOException
    {
        if (!hasBindAddress)
        {
            return 0;
        }

        final int newConnections = selector.selectNow();
        if (newConnections > 0)
        {
            final Iterator<SelectionKey> it = selector.selectedKeys().iterator();
            while (it.hasNext())
            {
                it.next();

                final SocketChannel channel = listeningChannel.accept();
                configure(channel);

                handler.onNewChannel(timeInMs, newTcpChannel(channel));

                it.remove();
            }
        }

        return newConnections;
    }

    private void configure(final SocketChannel channel) throws IOException
    {
        channel.setOption(TCP_NODELAY, true);
        if (configuration.receiverSocketBufferSize() > 0)
        {
            channel.setOption(SO_RCVBUF, configuration.receiverSocketBufferSize());
        }
        if (configuration.senderSocketBufferSize() > 0)
        {
            channel.setOption(SO_SNDBUF, configuration.senderSocketBufferSize());
        }
        channel.configureBlocking(false);
    }

    public void close() throws Exception
    {
        CloseHelper.close(listeningChannel);
        CloseHelper.close(selector);
    }

    public TcpChannel open(final InetSocketAddress address) throws IOException
    {
        final SocketChannel channel = SocketChannel.open();
        channel.connect(address);
        configure(channel);
        return newTcpChannel(channel);
    }

    protected TcpChannel newTcpChannel(final SocketChannel channel) throws IOException
    {
        return new TcpChannel(channel);
    }

    @FunctionalInterface
    public interface NewChannelHandler
    {
        void onNewChannel(final long timeInMs, final TcpChannel socketChannel) throws IOException;
    }

}
