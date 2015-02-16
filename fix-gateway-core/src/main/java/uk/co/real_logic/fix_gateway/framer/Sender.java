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
import uk.co.real_logic.fix_gateway.framer.commands.ReceiverProxy;
import uk.co.real_logic.fix_gateway.framer.commands.SenderCommand;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.function.Consumer;

/**
 * Sends outbound data
 */
public final class Sender implements Agent
{
    private final Consumer<SenderCommand> onCommandFunc = this::onCommand;

    private final OneToOneConcurrentArrayQueue<SenderCommand> commandQueue;
    private final ConnectionHandler connectionHandler;
    private final ReceiverProxy receiver;
    private final Multiplexer multiplexer;

    public Sender(
        final OneToOneConcurrentArrayQueue<SenderCommand> commandQueue,
        final ConnectionHandler connectionHandler,
        final ReceiverProxy receiver,
        final Multiplexer multiplexer)
    {
        this.commandQueue = commandQueue;
        this.connectionHandler = connectionHandler;
        this.receiver = receiver;
        this.multiplexer = multiplexer;
    }

    public int doWork() throws Exception
    {
        return commandQueue.drain(onCommandFunc) + multiplexer.scanBuffers();
    }

    private void onCommand(final SenderCommand command)
    {
        command.execute(this);
    }

    public void onConnect(final InetSocketAddress address)
    {
        try
        {
            final SocketChannel channel = SocketChannel.open();
            channel.connect(address);
            channel.configureBlocking(false);

            final Connection connection = connectionHandler.createConnection(channel);
            onNewConnection(connection);
            receiver.newConnection(connection);
        }
        catch (final IOException ex)
        {
            // TODO
            ex.printStackTrace();
        }
    }

    public void onNewConnection(final Connection connection)
    {
        multiplexer.onNewConnection(connection.connectionId(), connection.senderEndPoint());
    }

    public void onClose()
    {
        // TODO: decide on whether the Connections should be closed from the sender or the receiver
    }

    public String roleName()
    {
        return "Sender";
    }
}
