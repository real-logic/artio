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

import uk.co.real_logic.agrona.concurrent.OneToOneConcurrentArrayQueue;
import uk.co.real_logic.fix_gateway.framer.*;
import uk.co.real_logic.fix_gateway.framer.commands.ReceiverCommand;
import uk.co.real_logic.fix_gateway.framer.commands.ReceiverProxy;
import uk.co.real_logic.fix_gateway.framer.commands.SenderCommand;
import uk.co.real_logic.fix_gateway.framer.commands.SenderProxy;
import uk.co.real_logic.fix_gateway.framer.session.InitiatorSession;
import uk.co.real_logic.fix_gateway.framer.session.SessionProxy;

public final class FixGateway implements AutoCloseable
{
    private final SenderProxy senderProxy;
    private final ReceiverProxy receiverProxy;
    private final Sender sender;
    private final Receiver receiver;

    private InitiatorSession addedSession;

    private FixGateway(final StaticConfiguration configuration)
    {
        // TODO: do we really need to make this an MPSC queue?
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

        sender = new Sender(senderCommands, handler, receiverProxy, multiplexer);
        receiver = new Receiver(configuration.bindAddress(), handler, receiverCommands, senderProxy);
    }

    public static FixGateway launch(final StaticConfiguration configuration)
    {
        return new FixGateway(configuration);
    }

    // TODO: figure out correct type for dictionary
    public synchronized InitiatorSession initiate(final SessionConfiguration host, final Object dictionary)
    {
        return null;
    }

    public synchronized void close() throws Exception
    {
    }

    public void onInitiatorSessionConnected(final InitiatorSession session)
    {
        // TODO: signal
        addedSession = session;
    }
}
