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

import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.agrona.LangUtil;
import uk.co.real_logic.agrona.concurrent.*;
import uk.co.real_logic.fix_gateway.commands.ReceiverCommand;
import uk.co.real_logic.fix_gateway.commands.ReceiverProxy;
import uk.co.real_logic.fix_gateway.commands.SenderCommand;
import uk.co.real_logic.fix_gateway.commands.SenderProxy;
import uk.co.real_logic.fix_gateway.dictionary.IntDictionary;
import uk.co.real_logic.fix_gateway.framer.*;
import uk.co.real_logic.fix_gateway.framer.session.InitiatorSession;
import uk.co.real_logic.fix_gateway.framer.session.SessionProxy;
import uk.co.real_logic.fix_gateway.otf.OtfMessageAcceptor;
import uk.co.real_logic.fix_gateway.otf.OtfParser;
import uk.co.real_logic.fix_gateway.replication.ReplicationStreams;
import uk.co.real_logic.fix_gateway.util.MilliClock;

import static uk.co.real_logic.fix_gateway.StaticConfiguration.DEBUG_PRINT_MESSAGES;

public class FixGateway implements AutoCloseable
{
    private static final MessageHandler EMPTY_HANDLER = (buffer, offset, length, sessionId, messageType) -> {};

    private final FixCounters fixCounters;

    private final Aeron aeron;
    private final ReplicationStreams streams;

    private final SenderProxy senderProxy;
    private final ReceiverProxy receiverProxy;

    private final Sender sender;
    private final Receiver receiver;

    private final AgentRunner senderRunner;
    private final AgentRunner receiverRunner;

    private final Signal signal = new Signal();
    private final long connectionTimeout;

    private InitiatorSession addedSession;
    private Exception exception;

    FixGateway(final StaticConfiguration configuration)
    {
        connectionTimeout = configuration.connectionTimeout();

        fixCounters = new FixCounters(CountersFileDescriptor.createCountersManager(configuration));

        Aeron.Context context = new Aeron.Context();
        aeron = Aeron.connect(context);
        streams = new ReplicationStreams(configuration.aeronChannel(), aeron, fixCounters.failedDataPublications());

        final SequencedContainerQueue<SenderCommand> senderCommands = new ManyToOneConcurrentArrayQueue<>(10);
        final SequencedContainerQueue<ReceiverCommand> receiverCommands = new ManyToOneConcurrentArrayQueue<>(10);

        senderProxy = new SenderProxy(senderCommands, fixCounters.senderProxyFails());
        receiverProxy = new ReceiverProxy(receiverCommands, fixCounters.receiverProxyFails());

        final Multiplexer multiplexer = new Multiplexer(receiverProxy);
        final Subscription dataSubscription = streams.dataSubscription(multiplexer);
        final SessionProxy sessionProxy = new SessionProxy(configuration.encoderBufferSize(),
            streams.fixPublication(), configuration.sessionIdStrategy());

        final MessageHandler messageHandler = messageHandler(configuration.fallbackAcceptor());

        final MilliClock systemClock = System::currentTimeMillis;

        final ConnectionHandler handler = new ConnectionHandler(
            systemClock,
            sessionProxy,
            configuration.receiverBufferSize(),
            configuration.defaultHeartbeatInterval(),
            configuration.sessionIdStrategy(),
            messageHandler,
            streams,
            configuration.authenticationStrategy(),
            configuration.sessionHandler());

        sender = new Sender(senderCommands, handler, receiverProxy, this, multiplexer, dataSubscription);

        receiver = new Receiver(systemClock, configuration.bindAddress(), handler, receiverCommands, senderProxy);

        senderRunner = new AgentRunner(backoffIdleStrategy(), Throwable::printStackTrace, null, sender);
        receiverRunner = new AgentRunner(backoffIdleStrategy(), Throwable::printStackTrace, null, receiver);
    }

    private MessageHandler messageHandler(final OtfMessageAcceptor fallbackAcceptor)
    {
        final MessageHandler handler = fallbackAcceptor  == null
                                     ? EMPTY_HANDLER
                                     : new OtfParser(fallbackAcceptor, new IntDictionary());
        return DEBUG_PRINT_MESSAGES ? new DebugMessageHandler(handler) : handler;
    }

    private BackoffIdleStrategy backoffIdleStrategy()
    {
        return new BackoffIdleStrategy(1, 1, 1, 1 << 20);
    }

    public static FixGateway launch(final StaticConfiguration configuration)
    {
        return new FixGateway(configuration.conclude()).start();
    }

    private FixGateway start()
    {
        start(senderRunner);
        start(receiverRunner);
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
        senderProxy.connect(configuration);
        signal.await(connectionTimeout);
        final InitiatorSession addedSession = this.addedSession;
        if (addedSession == null)
        {
            LangUtil.rethrowUnchecked(this.exception != null ? this.exception : timeout(configuration));
        }
        this.addedSession = null;
        return addedSession;
    }

    private ConnectionTimeoutException timeout(final SessionConfiguration configuration)
    {
        return new ConnectionTimeoutException(
            "Connection timed out connecting to: " + configuration.host() + ":" + configuration.port());
    }

    public synchronized void close() throws Exception
    {
        senderRunner.close();
        receiverRunner.close();

        sender.onClose();
        receiver.onClose();

        streams.dataPublication().close();
        streams.controlPublication().close();
        aeron.close();
    }

    public void onInitiatorSessionActive(final InitiatorSession session)
    {
        addedSession = session;
        signal.signal();
    }

    public void onInitiationError(final Exception exception)
    {
        this.exception = exception;
        signal.signal();
    }
}
