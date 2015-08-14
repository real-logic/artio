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
package uk.co.real_logic.fix_gateway.engine;

import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.agrona.concurrent.*;
import uk.co.real_logic.fix_gateway.*;
import uk.co.real_logic.fix_gateway.engine.framer.*;
import uk.co.real_logic.fix_gateway.engine.logger.Logger;
import uk.co.real_logic.fix_gateway.session.SessionIdStrategy;

import java.util.List;
import java.util.concurrent.locks.LockSupport;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static uk.co.real_logic.agrona.CloseHelper.quietClose;
import static uk.co.real_logic.agrona.concurrent.AgentRunner.startOnThread;

public final class FixEngine extends GatewayProcess
{
    public static final long COMMAND_QUEUE_IDLE = MILLISECONDS.toNanos(10);
    private QueuedPipe<AdminCommand> adminCommands;

    private final EngineConfiguration configuration;
    private AgentRunner errorPrinterRunner;
    private AgentRunner framerRunner;
    private Logger logger;

    public static FixEngine launch(final EngineConfiguration configuration)
    {
        return new FixEngine(configuration).start();
    }

    /**
     * Query the engine for the list of libraries currently active.
     *
     * @return a list of currently active libraries.
     */
    public List<LibraryInfo> libraries()
    {
        final QueryLibraries query = new QueryLibraries();
        while (!adminCommands.offer(query))
        {
            LockSupport.parkNanos(COMMAND_QUEUE_IDLE);
        }
        return query.awaitResponse();
    }

    private FixEngine(final EngineConfiguration configuration)
    {
        super(configuration);
        this.configuration = configuration;

        initFramer(configuration, fixCounters);
        initLogger(configuration);
        initErrorPrinter();
    }

    private void initLogger(final EngineConfiguration configuration)
    {
        logger = new Logger(
            configuration, inboundLibraryStreams, outboundLibraryStreams, errorBuffer, replayPublication());
        logger.init();
    }

    private Publication replayPublication()
    {
        return aeron.addPublication(configuration.aeronChannel(), OUTBOUND_REPLAY_STREAM);
    }

    private void initErrorPrinter()
    {
        if (configuration.printErrorMessages())
        {
            final ErrorPrinter printer = new ErrorPrinter(monitoringFile);
            final IdleStrategy idleStrategy = new BackoffIdleStrategy(1, 1, 1000, 1_000_000);
            errorPrinterRunner = new AgentRunner(idleStrategy, Throwable::printStackTrace, null, printer);
        }
    }

    private void initFramer(final EngineConfiguration configuration, final FixCounters fixCounters)
    {
        final SessionIds sessionIds = new SessionIds();

        final IdleStrategy idleStrategy = backoffIdleStrategy();
        final Subscription librarySubscription = outboundLibraryStreams.dataSubscription();
        final SessionIdStrategy sessionIdStrategy = configuration.sessionIdStrategy();

        final ConnectionHandler handler = new ConnectionHandler(
            configuration,
            sessionIdStrategy,
            sessionIds,
            inboundLibraryStreams,
            idleStrategy,
            fixCounters,
            errorBuffer);

        final Framer framer = new Framer(configuration, handler, librarySubscription, replaySubscription(),
            inboundLibraryStreams.gatewayPublication(), sessionIdStrategy, sessionIds, adminCommands);
        framerRunner = new AgentRunner(idleStrategy, errorBuffer, null, framer);
    }

    private Subscription replaySubscription()
    {
        return aeron.addSubscription(configuration.aeronChannel(), OUTBOUND_REPLAY_STREAM);
    }

    private FixEngine start()
    {
        startOnThread(framerRunner);
        logger.start();
        if (configuration.printErrorMessages())
        {
            startOnThread(errorPrinterRunner);
        }
        return this;
    }

    protected Aeron.Context aeronContext(final CommonConfiguration configuration)
    {
        adminCommands = new ManyToOneConcurrentArrayQueue<>(10);
        final boolean logInboundMessages = ((EngineConfiguration) configuration).logInboundMessages();
        final LibraryActivationHandler activationHandler =
            new LibraryActivationHandler(logInboundMessages, adminCommands);
        final Aeron.Context context = super.aeronContext(configuration);
        return context
            .newImageHandler(activationHandler)
            .inactiveImageHandler(activationHandler);
    }

    public synchronized void close()
    {
        quietClose(errorPrinterRunner);
        framerRunner.close();
        logger.close();
        super.close();
    }

}
