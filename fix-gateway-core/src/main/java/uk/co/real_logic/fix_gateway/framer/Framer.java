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

import uk.co.real_logic.agrona.concurrent.Agent;
import uk.co.real_logic.agrona.concurrent.SequencedContainerQueue;
import uk.co.real_logic.fix_gateway.ConnectionHandler;
import uk.co.real_logic.fix_gateway.FixGateway;
import uk.co.real_logic.fix_gateway.SessionConfiguration;
import uk.co.real_logic.fix_gateway.replication.GatewaySubscription;
import uk.co.real_logic.fix_gateway.session.Session;
import uk.co.real_logic.fix_gateway.util.MilliClock;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import static java.net.StandardSocketOptions.TCP_NODELAY;
import static java.nio.channels.SelectionKey.OP_READ;

/**
 * Handles incoming connections from clients and outgoing connections to exchanges.
 */
public class Framer implements Agent
{
    private final Consumer<FramerCommand> onCommandFunc = this::onCommand;
    private final List<Session> sessions = new ArrayList<>();
    private final List<ReceiverEndPoint> receiverEndPoints = new ArrayList<>();

    private final ServerSocketChannel listeningChannel;
    private final MilliClock clock;
    private final ConnectionHandler connectionHandler;
    private final SequencedContainerQueue<FramerCommand> commandQueue;
    private final Multiplexer multiplexer;
    private final FixGateway gateway;
    private final GatewaySubscription dataSubscription;
    private final Selector selector;

    // TODO: add hooks for receive and send buffer sizes
    public Framer(
        final MilliClock clock,
        final SocketAddress address,
        final ConnectionHandler connectionHandler,
        final SequencedContainerQueue<FramerCommand> commandQueue,
        final Multiplexer multiplexer,
        final FixGateway gateway,
        final GatewaySubscription dataSubscription)
    {
        this.clock = clock;
        this.connectionHandler = connectionHandler;
        this.commandQueue = commandQueue;
        this.multiplexer = multiplexer;
        this.gateway = gateway;
        this.dataSubscription = dataSubscription;

        try
        {
            listeningChannel = ServerSocketChannel.open();
            listeningChannel.bind(address).configureBlocking(false);

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
        return commandQueue.drain(onCommandFunc) +
            pollSockets() +
            pollSessions() +
            dataSubscription.poll(5);
    }

    private void onCommand(final FramerCommand command)
    {
        command.execute(this);
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
                channel.configureBlocking(false);
                channel.setOption(TCP_NODELAY, false);

                onNewSession(channel, connectionHandler.acceptSession());
            }
            else if (key.isReadable())
            {
                ((ReceiverEndPoint)key.attachment()).receiveData();
            }

            iter.remove();
        }

        return count;
    }

    private void onNewSession(final SocketChannel channel, final Session session)
        throws ClosedChannelException
    {
        final long connectionId = session.connectionId();
        final ReceiverEndPoint receiverEndPoint = connectionHandler.receiverEndPoint(channel, connectionId, session);
        receiverEndPoints.add(receiverEndPoint);
        channel.register(selector, OP_READ, receiverEndPoint);

        sessions.add(session);
        multiplexer.onNewConnection(connectionHandler.senderEndPoint(channel, connectionId));
    }

    private int pollSessions()
    {
        final long time = clock.time();
        int stateChanges = 0;
        for (final Session session: sessions)
        {
            stateChanges += session.poll(time);
        }
        return stateChanges;
    }

    public void onConnect(final SessionConfiguration configuration)
    {
        try
        {
            final InetSocketAddress address = new InetSocketAddress(configuration.host(), configuration.port());
            final SocketChannel channel = SocketChannel.open();
            channel.connect(address);
            channel.configureBlocking(false);

            onNewSession(channel, connectionHandler.initiateSession(gateway, configuration));
        }
        catch (final Exception e)
        {
            gateway.onInitiationError(e);
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
                sessions.remove(endPoint.session());
                break;
            }
        }
    }

    @Override
    public void onClose()
    {
        try
        {
            sessions.forEach(Session::disconnect);
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

    @Override
    public String roleName()
    {
        return "Framer";
    }

}
