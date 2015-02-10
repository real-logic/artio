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
import uk.co.real_logic.agrona.concurrent.OneToOneConcurrentArrayQueue;
import uk.co.real_logic.fix_gateway.framer.commands.ReceiverCommand;
import uk.co.real_logic.fix_gateway.framer.commands.SenderProxy;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.*;
import java.util.Iterator;
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

    private final ServerSocketChannel listeningChannel;
    private final ConnectionHandler connectionHandler;
    private final OneToOneConcurrentArrayQueue<ReceiverCommand> commandQueue;
    private final SenderProxy sender;
    private final Selector selector;

    // TODO: add hooks for receive and send buffer sizes
    public Receiver(
            final SocketAddress address,
            final ConnectionHandler connectionHandler,
            final OneToOneConcurrentArrayQueue<ReceiverCommand> commandQueue,
            final SenderProxy sender)
    {
        this.connectionHandler = connectionHandler;
        this.commandQueue = commandQueue;
        this.sender = sender;

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
        return commandQueue.drain(onCommandFunc) + pollSockets();
    }

    private void onCommand(final ReceiverCommand command)
    {
        command.execute(this);
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

                final Connection connection = connectionHandler.createConnection(channel);
                register(channel, connection.receiverEndPoint());
                sender.newConnection(connection);
            }
            else if (key.isReadable())
            {
                ((ReceiverEndPoint) key.attachment()).receiveData();
            }

            it.remove();
        }

        return count;
    }

    public void onNewConnection(final Connection connection)
    {
        try
        {
            final ReceiverEndPoint receiverEndPoint = connection.receiverEndPoint();
            register(receiverEndPoint.channel(), receiverEndPoint);
        }
        catch (ClosedChannelException e)
        {
            // TODO
            e.printStackTrace();
        }
    }

    private void register(final SocketChannel channel, final ReceiverEndPoint receiverEndPoint) throws ClosedChannelException
    {
        channel.register(selector, OP_READ, receiverEndPoint);
    }

    @Override
    public void onClose()
    {
        try
        {
            // JDK on Windows - sigh
            selector.selectNow();
            selector.close();
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
