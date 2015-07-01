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

import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.agrona.concurrent.Agent;
import uk.co.real_logic.fix_gateway.ConnectionHandler;
import uk.co.real_logic.fix_gateway.StaticConfiguration;
import uk.co.real_logic.fix_gateway.replication.DataSubscriber;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static java.net.StandardSocketOptions.SO_RCVBUF;
import static java.net.StandardSocketOptions.TCP_NODELAY;
import static java.nio.channels.SelectionKey.OP_READ;

/**
 * Handles incoming connections from clients and outgoing connections to exchanges.
 */
public class Framer implements Agent
{
    private final List<ReceiverEndPoint> receiverEndPoints = new ArrayList<>();
    private final DataSubscriber dataSubscriber;

    private final ServerSocketChannel listeningChannel;
    private final StaticConfiguration configuration;
    private final ConnectionHandler connectionHandler;
    private final Multiplexer multiplexer;
    private final Subscription outboundDataSubscription;
    private final Selector selector;

    private long connectionId = 0;

    public Framer(
        final StaticConfiguration configuration,
        final ConnectionHandler connectionHandler,
        final Multiplexer multiplexer,
        final Subscription outboundDataSubscription)
    {
        this.configuration = configuration;
        this.connectionHandler = connectionHandler;
        this.multiplexer = multiplexer;
        this.outboundDataSubscription = outboundDataSubscription;
        dataSubscriber = new DataSubscriber(multiplexer);

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

    @Override
    public int doWork() throws Exception
    {
        return pollSockets() + outboundDataSubscription.poll(dataSubscriber, 5);
    }

    private int pollSockets() throws IOException
    {
        final int count = selector.selectNow();

        final Set<SelectionKey> keys = selector.selectedKeys();
        for (Iterator<SelectionKey> iter = keys.iterator(); iter.hasNext(); )
        {
            final SelectionKey key = iter.next();
            if (key.isAcceptable())
            {
                final SocketChannel channel = listeningChannel.accept();
                setupConnection(channel);
            }
            else if (key.isReadable())
            {
                ((ReceiverEndPoint)key.attachment()).receiveData();
            }

            iter.remove();
        }

        return count;
    }

    private void setupConnection(final SocketChannel channel)
        throws IOException
    {
        channel.configureBlocking(false);
        channel.setOption(TCP_NODELAY, false);
        if (configuration.receiverSocketBufferSize() > 0)
        {
            channel.setOption(SO_RCVBUF, configuration.receiverSocketBufferSize());
        }
        if (configuration.senderSocketBufferSize() > 0)
        {
            channel.setOption(SO_RCVBUF, configuration.senderSocketBufferSize());
        }

        final long connectionId = this.connectionId++;
        final ReceiverEndPoint receiverEndPoint = connectionHandler.receiverEndPoint(channel, connectionId);
        receiverEndPoints.add(receiverEndPoint);
        channel.register(selector, OP_READ, receiverEndPoint);

        multiplexer.onNewConnection(connectionHandler.senderEndPoint(channel, connectionId));
    }

    public void onInitiateConnection(final int streamId,
                                     final int port,
                                     final String host,
                                     final String senderCompId,
                                     final String targetCompId)
    {
        try
        {
            final InetSocketAddress address = new InetSocketAddress(host, port);
            final SocketChannel channel = SocketChannel.open();
            channel.connect(address);
            setupConnection(channel);
            // TODO: Identify session
        }
        catch (final Exception e)
        {
            e.printStackTrace(); // TODO: reply to message
        }
    }

    public void onDisconnect(final long connectionId)
    {
        final Iterator<ReceiverEndPoint> it = receiverEndPoints.iterator();
        while (it.hasNext())
        {
            final ReceiverEndPoint endPoint = it.next();
            if (endPoint.connectionId() == connectionId)
            {
                endPoint.close();
                it.remove();
                break;
            }
        }
    }

    public void onClose()
    {
        try
        {
            receiverEndPoints.forEach(ReceiverEndPoint::close);
            if (selector.isOpen())
            {
                // JDK on Windows - sigh
                selector.selectNow();
                selector.close();
            }
            listeningChannel.close();
        }
        catch (final IOException ex)
        {
            throw new IllegalStateException(ex);
        }
    }

    public String roleName()
    {
        return "Framer";
    }

}
