/*
 * Copyright 2015-2018 Real Logic Ltd, Adaptive Financial Consulting Ltd.
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
package uk.co.real_logic.artio.engine.framer;

import org.agrona.CloseHelper;
import org.agrona.LangUtil;
import uk.co.real_logic.artio.engine.EngineConfiguration;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;

import static java.net.StandardSocketOptions.*;
import static java.nio.channels.SelectionKey.OP_CONNECT;

/**
 * Mockable class for intercepting network communications
 */
public class TcpChannelSupplier implements AutoCloseable
{
    private final EngineConfiguration configuration;
    private final boolean hasBindAddress;

    private int opensInFlight = 0;
    private Selector selector;
    private ServerSocketChannel listeningChannel;

    public TcpChannelSupplier(final EngineConfiguration configuration)
    {
        hasBindAddress = configuration.hasBindAddress();
        this.configuration = configuration;
        try
        {
            selector = Selector.open();

            if (hasBindAddress)
            {

                listeningChannel = ServerSocketChannel.open();
                listeningChannel.bind(configuration.bindAddress()).configureBlocking(false);
                listeningChannel.register(selector, SelectionKey.OP_ACCEPT);
            }
            else
            {
                listeningChannel = null;
            }
        }
        catch (final IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }

    public int pollSelector(final long timeInMs, final NewChannelHandler handler) throws IOException
    {
        if (hasBindAddress || opensInFlight > 0)
        {
            selector.selectNow();
            final Set<SelectionKey> selectionKeys = selector.selectedKeys();
            final int unprocessedConnections = selectionKeys.size();
            if (unprocessedConnections > 0)
            {
                final Iterator<SelectionKey> it = selectionKeys.iterator();
                while (it.hasNext())
                {
                    final SelectionKey selectionKey = it.next();

                    if (selectionKey.isAcceptable())
                    {
                        final SocketChannel channel = listeningChannel.accept();
                        if (channel != null)
                        {
                            configure(channel);
                            channel.configureBlocking(false);

                            handler.onNewChannel(timeInMs, newTcpChannel(channel));
                        }

                        it.remove();
                    }
                    else if (selectionKey.isConnectable())
                    {
                        final InitiatedChannelHandler channelHandler =
                            (InitiatedChannelHandler)selectionKey.attachment();
                        final SocketChannel channel = (SocketChannel)selectionKey.channel();
                        try
                        {
                            if (channel.finishConnect())
                            {
                                channelHandler.onInitiatedChannel(newTcpChannel(channel), null);
                                it.remove();
                                opensInFlight--;
                            }
                        }
                        catch (final IOException e)
                        {
                            channelHandler.onInitiatedChannel(null, e);
                            it.remove();
                            opensInFlight--;
                        }
                    }
                }
            }

            return unprocessedConnections;
        }

        return 0;
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
    }

    public void close()
    {
        CloseHelper.close(listeningChannel);
        CloseHelper.close(selector);
    }

    public void open(final InetSocketAddress address, final InitiatedChannelHandler channelHandler) throws IOException
    {
        final SocketChannel channel = SocketChannel.open();
        channel.configureBlocking(false);
        channel.register(selector, OP_CONNECT, channelHandler);
        configure(channel);
        channel.connect(address);
        opensInFlight++;
    }

    protected TcpChannel newTcpChannel(final SocketChannel channel) throws IOException
    {
        return new TcpChannel(channel);
    }

    @FunctionalInterface
    public interface InitiatedChannelHandler
    {
        void onInitiatedChannel(TcpChannel socketChannel, IOException exception);
    }

    @FunctionalInterface
    public interface NewChannelHandler
    {
        void onNewChannel(long timeInMs, TcpChannel socketChannel) throws IOException;
    }
}
