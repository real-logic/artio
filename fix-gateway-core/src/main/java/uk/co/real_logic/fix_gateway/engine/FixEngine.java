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

import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.agrona.concurrent.AgentRunner;
import uk.co.real_logic.agrona.concurrent.BackoffIdleStrategy;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.fix_gateway.EngineConfiguration;
import uk.co.real_logic.fix_gateway.ErrorPrinter;
import uk.co.real_logic.fix_gateway.FixCounters;
import uk.co.real_logic.fix_gateway.GatewayProcess;
import uk.co.real_logic.fix_gateway.engine.framer.Framer;
import uk.co.real_logic.fix_gateway.engine.framer.Multiplexer;
import uk.co.real_logic.fix_gateway.engine.framer.SessionIds;
import uk.co.real_logic.fix_gateway.engine.logger.LoggerModule;
import uk.co.real_logic.fix_gateway.session.SessionIdStrategy;

import static uk.co.real_logic.agrona.CloseHelper.quietClose;
import static uk.co.real_logic.agrona.concurrent.AgentRunner.startOnThread;

public class FixEngine extends GatewayProcess
{
    private final EngineConfiguration configuration;
    private AgentRunner errorPrinterRunner;
    private AgentRunner framerRunner;
    private LoggerModule logger;

    FixEngine(final EngineConfiguration configuration)
    {
        super(configuration);
        this.configuration = configuration;

        initFramer(configuration, fixCounters);
        logger = new LoggerModule(configuration, inboundStreams, outboundStreams, errorBuffer);
        logger.init();
        initErrorPrinter();
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
        final Multiplexer multiplexer = new Multiplexer();
        final Subscription dataSubscription = outboundStreams.dataSubscription();
        final SessionIdStrategy sessionIdStrategy = configuration.sessionIdStrategy();

        final ConnectionHandler handler = new ConnectionHandler(
            configuration,
            sessionIdStrategy,
            sessionIds,
            inboundStreams,
            idleStrategy,
            fixCounters,
            errorBuffer);

        final Framer framer = new Framer(configuration, handler, multiplexer, dataSubscription,
            inboundStreams.gatewayPublication(), sessionIdStrategy, sessionIds);
        multiplexer.framer(framer);
        framerRunner = new AgentRunner(idleStrategy, errorBuffer, null, framer);
    }

    private BackoffIdleStrategy backoffIdleStrategy()
    {
        return new BackoffIdleStrategy(1, 1, 1, 1 << 20);
    }

    public static FixEngine launch(final EngineConfiguration configuration)
    {
        return new FixEngine(configuration).start();
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

    public synchronized void close()
    {
        quietClose(errorPrinterRunner);
        framerRunner.close();
        logger.close();
        super.close();
    }

}
