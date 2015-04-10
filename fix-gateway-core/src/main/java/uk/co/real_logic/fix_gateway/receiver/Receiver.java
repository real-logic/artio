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
package uk.co.real_logic.fix_gateway.receiver;

import uk.co.real_logic.agrona.concurrent.Agent;
import uk.co.real_logic.agrona.concurrent.SequencedContainerQueue;
import uk.co.real_logic.fix_gateway.ConnectionHandler;
import uk.co.real_logic.fix_gateway.sender.SenderProxy;
import uk.co.real_logic.fix_gateway.session.AcceptorSession;
import uk.co.real_logic.fix_gateway.session.Session;
import uk.co.real_logic.fix_gateway.session.SessionIds;
import uk.co.real_logic.fix_gateway.util.MilliClock;

import java.io.IOException;
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
public final class Receiver implements Agent
{
    private final Consumer<ReceiverCommand> onCommandFunc = this::onCommand;
    private final List<Session> sessions = new ArrayList<>();
    private final List<ReceiverEndPoint> endPoints = new ArrayList<>();

    private final ServerSocketChannel listeningChannel;
    private final MilliClock clock;
    private final ConnectionHandler connectionHandler;
    private final SequencedContainerQueue<ReceiverCommand> commandQueue;
    private final SenderProxy sender;
    private final SessionIds receiverSessions;
    private final Selector selector;

    // TODO: add hooks for receive and send buffer sizes
    public Receiver(
        final MilliClock clock,
        final SocketAddress address,
        final ConnectionHandler connectionHandler,
        final SequencedContainerQueue<ReceiverCommand> commandQueue,
        final SenderProxy sender,
        final SessionIds receiverSessions)
    {
        this.clock = clock;
        this.connectionHandler = connectionHandler;
        this.commandQueue = commandQueue;
        this.sender = sender;
        this.receiverSessions = receiverSessions;

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
        return commandQueue.drain(onCommandFunc) + pollSockets() + pollSessions();
    }

    private void onCommand(final ReceiverCommand command)
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

                final long connectionId = connectionHandler.onConnection();
                final AcceptorSession session = connectionHandler.acceptSession(connectionId);
                register(channel, connectionHandler.receiverEndPoint(channel, connectionId, session));
                sender.newAcceptedConnection(connectionHandler.senderEndPoint(channel, connectionId));
            }
            else if (key.isReadable())
            {
                ((ReceiverEndPoint)key.attachment()).receiveData();
            }

            iter.remove();
        }

        return count;
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

    public void onNewInitiatedConnection(final ReceiverEndPoint receiverEndPoint)
    {
        try
        {
            register(receiverEndPoint.channel(), receiverEndPoint);
        }
        catch (final ClosedChannelException ex)
        {
            // TODO
            ex.printStackTrace();
        }
    }

    private void register(final SocketChannel channel, final ReceiverEndPoint receiverEndPoint) throws ClosedChannelException
    {
        endPoints.add(receiverEndPoint);
        sessions.add(receiverEndPoint.session());
        channel.register(selector, OP_READ, receiverEndPoint);
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
        return "Dispatcher";
    }

    public void onDisconnect(final long connectionId)
    {
        final Iterator<ReceiverEndPoint> it = endPoints.iterator();
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

    public void onNewSessionId(final Object compositeId, final long surrogateId)
    {
        System.out.println("READ");
        receiverSessions.put(compositeId, surrogateId);
    }
}
