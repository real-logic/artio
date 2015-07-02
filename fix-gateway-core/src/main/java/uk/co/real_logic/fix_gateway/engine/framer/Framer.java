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

import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.agrona.concurrent.Agent;
import uk.co.real_logic.fix_gateway.StaticConfiguration;
import uk.co.real_logic.fix_gateway.engine.ConnectionHandler;
import uk.co.real_logic.fix_gateway.library.session.SessionHandler;
import uk.co.real_logic.fix_gateway.replication.DataSubscriber;
import uk.co.real_logic.fix_gateway.replication.GatewayPublication;
import uk.co.real_logic.fix_gateway.session.SessionIdStrategy;

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
import static uk.co.real_logic.fix_gateway.library.session.Session.UNKNOWN_ID;
import static uk.co.real_logic.fix_gateway.messages.ConnectionType.ACCEPTOR;
import static uk.co.real_logic.fix_gateway.messages.ConnectionType.INITIATOR;

/**
 * Handles incoming connections from clients and outgoing connections to exchanges.
 */
public class Framer implements Agent, SessionHandler
{
    // TODO: remove this and identify sensible configuraion/load balancing for acceptor sessions
    public static final int ACCEPTOR_LIBRARY_ID = 42;

    private final List<ReceiverEndPoint> receiverEndPoints = new ArrayList<>();
    private final DataSubscriber dataSubscriber;

    private final ServerSocketChannel listeningChannel;
    private final StaticConfiguration configuration;
    private final ConnectionHandler connectionHandler;
    private final Multiplexer multiplexer;
    private final Subscription outboundDataSubscription;
    private final GatewayPublication inboundPublication;
    private final SessionIdStrategy sessionIdStrategy;
    private final SessionIds sessionIds;
    private final Selector selector;

    private long nextConnectionId = 0;

    public Framer(
        final StaticConfiguration configuration,
        final ConnectionHandler connectionHandler,
        final Multiplexer multiplexer,
        final Subscription outboundDataSubscription,
        final GatewayPublication inboundPublication,
        final SessionIdStrategy sessionIdStrategy,
        final SessionIds sessionIds)
    {
        this.configuration = configuration;
        this.connectionHandler = connectionHandler;
        this.multiplexer = multiplexer;
        this.outboundDataSubscription = outboundDataSubscription;
        this.inboundPublication = inboundPublication;
        this.sessionIdStrategy = sessionIdStrategy;
        this.sessionIds = sessionIds;
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
        return outboundDataSubscription.poll(dataSubscriber, 5) + pollSockets();
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
                onAcceptConnection(channel);
            }
            else if (key.isReadable())
            {
                ((ReceiverEndPoint)key.attachment()).receiveData();
            }

            iter.remove();
        }

        return count;
    }

    private void onAcceptConnection(final SocketChannel channel) throws IOException
    {
        final long connectionId = this.nextConnectionId++;
        setupConnection(channel, connectionId, UNKNOWN_ID);

        final String address = channel.getRemoteAddress().toString();
        inboundPublication.saveConnect(connectionId, address, ACCEPTOR_LIBRARY_ID, ACCEPTOR);
    }

    public void onInitiateConnection(final int libraryId,
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
            final long connectionId = this.nextConnectionId++;

            final Object sessionKey = sessionIdStrategy.onInitiatorLogon(senderCompId, targetCompId);
            final long sessionId = sessionIds.onLogon(sessionKey);

            setupConnection(channel, connectionId, sessionId);
            inboundPublication.saveConnect(connectionId, address.toString(), libraryId, INITIATOR);
            inboundPublication.saveLogon(connectionId, sessionId);
        }
        catch (final Exception e)
        {
            e.printStackTrace(); // TODO: reply to message
        }
    }

    private void setupConnection(final SocketChannel channel, final long connectionId, final long sessionId)
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

        final ReceiverEndPoint receiverEndPoint = connectionHandler.receiverEndPoint(channel, connectionId, sessionId);
        receiverEndPoints.add(receiverEndPoint);
        channel.register(selector, OP_READ, receiverEndPoint);

        multiplexer.onNewConnection(connectionHandler.senderEndPoint(channel, connectionId));
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
