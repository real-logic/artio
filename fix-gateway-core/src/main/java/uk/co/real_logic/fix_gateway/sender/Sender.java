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
package uk.co.real_logic.fix_gateway.sender;

import uk.co.real_logic.agrona.concurrent.Agent;
import uk.co.real_logic.agrona.concurrent.SequencedContainerQueue;
import uk.co.real_logic.fix_gateway.FixGateway;
import uk.co.real_logic.fix_gateway.SessionConfiguration;
import uk.co.real_logic.fix_gateway.ConnectionHandler;
import uk.co.real_logic.fix_gateway.receiver.ReceiverProxy;
import uk.co.real_logic.fix_gateway.replication.GatewaySubscription;
import uk.co.real_logic.fix_gateway.session.InitiatorSession;
import uk.co.real_logic.fix_gateway.session.SessionIds;

import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.function.Consumer;

/**
 * Sends outbound data
 */
public final class Sender implements Agent
{
    private final Consumer<SenderCommand> onCommandFunc = this::onCommand;

    private final SequencedContainerQueue<SenderCommand> commandQueue;
    private final ConnectionHandler connectionHandler;
    private final ReceiverProxy receiver;
    private final FixGateway gateway;
    private final Multiplexer multiplexer;
    private final GatewaySubscription dataSubscription;
    private final SessionIds senderSessions;

    public Sender(
        final SequencedContainerQueue<SenderCommand> commandQueue,
        final ConnectionHandler connectionHandler,
        final ReceiverProxy receiver,
        final FixGateway gateway,
        final Multiplexer multiplexer,
        final GatewaySubscription dataSubscription,
        final SessionIds senderSessions)
    {
        this.commandQueue = commandQueue;
        this.connectionHandler = connectionHandler;
        this.receiver = receiver;
        this.gateway = gateway;
        this.multiplexer = multiplexer;
        this.dataSubscription = dataSubscription;
        this.senderSessions = senderSessions;
    }

    public int doWork() throws Exception
    {
        return commandQueue.drain(onCommandFunc) + dataSubscription.poll(5);
    }

    private void onCommand(final SenderCommand command)
    {
        command.execute(this);
    }

    public void onConnect(final SessionConfiguration configuration)
    {
        try
        {
            final InetSocketAddress address = new InetSocketAddress(configuration.host(), configuration.port());
            final SocketChannel channel = SocketChannel.open();
            channel.connect(address);
            channel.configureBlocking(false);

            final long connectionId = connectionHandler.onConnection();
            onNewAcceptedConnection(connectionHandler.senderEndPoint(channel, connectionId));
            final InitiatorSession session = connectionHandler.initiateSession(connectionId, gateway, configuration);
            receiver.newInitiatedConnection(connectionHandler.receiverEndPoint(channel, connectionId, session));
        }
        catch (final Exception e)
        {
            gateway.onInitiationError(e);
        }
    }

    public void onNewAcceptedConnection(final SenderEndPoint senderEndPoint)
    {
        multiplexer.onNewConnection(senderEndPoint);
    }

    public String roleName()
    {
        return "Sender";
    }

    public void onNewSessionId(final Object compositeId, final long surrogateId)
    {
        senderSessions.put(compositeId, surrogateId);
    }
}
