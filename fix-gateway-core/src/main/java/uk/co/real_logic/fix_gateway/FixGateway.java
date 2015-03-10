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
package uk.co.real_logic.fix_gateway;

import uk.co.real_logic.aeron.common.AgentRunner;
import uk.co.real_logic.aeron.common.BackoffIdleStrategy;
import uk.co.real_logic.agrona.concurrent.OneToOneConcurrentArrayQueue;
import uk.co.real_logic.agrona.concurrent.Signal;
import uk.co.real_logic.fix_gateway.framer.*;
import uk.co.real_logic.fix_gateway.commands.ReceiverCommand;
import uk.co.real_logic.fix_gateway.commands.ReceiverProxy;
import uk.co.real_logic.fix_gateway.commands.SenderCommand;
import uk.co.real_logic.fix_gateway.commands.SenderProxy;
import uk.co.real_logic.fix_gateway.framer.session.InitiatorSession;
import uk.co.real_logic.fix_gateway.framer.session.SessionProxy;

import java.net.InetSocketAddress;

public class FixGateway implements AutoCloseable
{
    private final SenderProxy senderProxy;
    private final ReceiverProxy receiverProxy;
    private final Sender sender;
    private final Receiver receiver;
    private final AgentRunner senderRunner;
    private final AgentRunner receiverRunner;
    private final Signal signal = new Signal();
    private final long connectionTimeout;

    private InitiatorSession addedSession;

    FixGateway(final StaticConfiguration configuration)
    {
        connectionTimeout = configuration.connectionTimeout();

        // TODO: MPSC queue?
        final OneToOneConcurrentArrayQueue<SenderCommand> senderCommands = new OneToOneConcurrentArrayQueue<>(10);
        final OneToOneConcurrentArrayQueue<ReceiverCommand> receiverCommands = new OneToOneConcurrentArrayQueue<>(10);

        senderProxy = new SenderProxy(senderCommands);
        receiverProxy = new ReceiverProxy(receiverCommands);

        final MessageSource source = handler -> 0;
        final Multiplexer multiplexer = new Multiplexer(source);
        final SessionProxy sessionProxy = new SessionProxy();
        final MessageHandler messageHandler = (buffer, offset, length, sessionId) ->
        {
            System.out.printf("Message received from %d\n", sessionId);
        };

        final ConnectionHandler handler = new ConnectionHandler(
            System::currentTimeMillis,
            sessionProxy,
            configuration.receiverBufferSize(),
            configuration.defaultHeartbeatInterval(),
            messageHandler);

        sender = new Sender(senderCommands, handler, receiverProxy, this, multiplexer);
        receiver = new Receiver(configuration.bindAddress(), handler, receiverCommands, senderProxy);

        senderRunner = new AgentRunner(backoffIdleStrategy(), Throwable::printStackTrace, null, sender);
        receiverRunner = new AgentRunner(backoffIdleStrategy(), Throwable::printStackTrace, null, receiver);
    }

    private BackoffIdleStrategy backoffIdleStrategy()
    {
        return new BackoffIdleStrategy(0, 0, 1, 1000);
    }

    public static FixGateway launch(final StaticConfiguration configuration)
    {
        return new FixGateway(configuration).start();
    }

    private FixGateway start()
    {
        start(senderRunner);
        return this;
    }

    private void start(final AgentRunner runner)
    {
        Thread thread = new Thread(runner);
        thread.setName(runner.agent().roleName());
        thread.start();
    }

    // TODO: figure out correct type for dictionary
    public synchronized InitiatorSession initiate(final SessionConfiguration configuration, final Object dictionary)
    {
        final InetSocketAddress address = new InetSocketAddress(configuration.host(), configuration.port());
        senderProxy.connect(address);
        signal.await(connectionTimeout);
        if (addedSession == null)
        {
            throw new ConnectionTimeoutException("Connection timed out whilst connecting to: " + address);
        }
        return addedSession;
    }

    public synchronized void close() throws Exception
    {
        senderRunner.close();
        receiverRunner.close();

        sender.onClose();
        receiver.onClose();
    }

    public void onInitiatorSessionConnected(final InitiatorSession session)
    {
        addedSession = session;
        signal.signal();
    }
}
