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

import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.agrona.concurrent.*;
import uk.co.real_logic.fix_gateway.ErrorPrinter;
import uk.co.real_logic.fix_gateway.FixCounters;
import uk.co.real_logic.fix_gateway.GatewayProcess;
import uk.co.real_logic.fix_gateway.engine.framer.*;
import uk.co.real_logic.fix_gateway.engine.logger.Logger;
import uk.co.real_logic.fix_gateway.engine.logger.SequenceNumberIndex;
import uk.co.real_logic.fix_gateway.session.SessionIdStrategy;

import java.util.List;

import static uk.co.real_logic.agrona.CloseHelper.quietClose;
import static uk.co.real_logic.agrona.concurrent.AgentRunner.startOnThread;

/**
 * A FIX Engine is a process in the gateway that accepts or initiates FIX connections and
 * hands them off to different FixLibrary instances. The engine can replicate and/or durably
 * store streams of FIX messages for replay, archival, administrative or analytics purposes.
 * <p>
 * Each engine can have one or more associated libraries that manage sessions and perform business
 * logic. These may run in the same JVM process or a different JVM process.
 *
 * @see uk.co.real_logic.fix_gateway.library.FixLibrary
 */
public final class FixEngine extends GatewayProcess
{
    private QueuedPipe<AdminCommand> adminCommands = new ManyToOneConcurrentArrayQueue<>(16);

    private final EngineConfiguration configuration;

    private AgentRunner errorPrinterRunner;
    private AgentRunner framerRunner;
    private Logger logger;

    /**
     * Launch the engine. This method starts up the engine threads and then returns.
     *
     * @param configuration the configuration to use for this engine.
     * @return the new FIX engine instance.
     */
    public static FixEngine launch(final EngineConfiguration configuration)
    {
        configuration.conclude();

        return new FixEngine(configuration).launch();
    }

    /**
     * Query the engine for the list of libraries currently active.
     *
     * @return a list of currently active libraries.
     */
    public List<LibraryInfo> libraries(final IdleStrategy idleStrategy)
    {
        final QueryLibraries query = new QueryLibraries();
        while (!adminCommands.offer(query))
        {
            idleStrategy.idle();
        }
        idleStrategy.reset();

        return query.awaitResponse(idleStrategy);
    }

    private FixEngine(final EngineConfiguration configuration)
    {
        init(configuration);
        this.configuration = configuration;

        final SequenceNumberIndex writerIndex = SequenceNumberIndex.forWriting(
            configuration.sequenceNumberCacheBuffer(), errorBuffer);
        initFramer(configuration, fixCounters);
        initLogger(configuration, writerIndex);
        initErrorPrinter(configuration);
    }

    private void initLogger(final EngineConfiguration configuration, final SequenceNumberIndex writerIndex)
    {
        logger = new Logger(
            configuration, inboundLibraryStreams, outboundLibraryStreams, errorBuffer, replayPublication(),
            writerIndex);
        logger.init();
    }

    private Publication replayPublication()
    {
        return aeron.addPublication(configuration.aeronChannel(), OUTBOUND_REPLAY_STREAM);
    }

    private void initErrorPrinter(final EngineConfiguration configuration)
    {
        if (this.configuration.printErrorMessages())
        {
            final ErrorPrinter printer = new ErrorPrinter(monitoringFile, configuration.errorSlotSize());
            errorPrinterRunner = new AgentRunner(
                configuration.errorPrinterIdleStrategy(), Throwable::printStackTrace, null, printer);
        }
    }

    private void initFramer(final EngineConfiguration configuration, final FixCounters fixCounters)
    {
        final SessionIds sessionIds = new SessionIds();

        final IdleStrategy idleStrategy = configuration.framerIdleStrategy();
        final Subscription librarySubscription = outboundLibraryStreams.subscription();
        final SessionIdStrategy sessionIdStrategy = configuration.sessionIdStrategy();

        final ConnectionHandler handler = new ConnectionHandler(
            configuration,
            sessionIdStrategy,
            sessionIds,
            inboundLibraryStreams,
            idleStrategy,
            fixCounters,
            errorBuffer);

        final Framer framer = new Framer(
            new SystemEpochClock(), configuration, handler, librarySubscription, replaySubscription(),
            sessionIdStrategy, sessionIds, adminCommands,
            SequenceNumberIndex.forReading(configuration.sequenceNumberCacheBuffer(), errorBuffer),
            SequenceNumberIndex.forReading(configuration.sequenceNumberCacheBuffer(), errorBuffer));
        framerRunner = new AgentRunner(idleStrategy, errorBuffer, null, framer);
    }

    private Subscription replaySubscription()
    {
        return aeron.addSubscription(configuration.aeronChannel(), OUTBOUND_REPLAY_STREAM);
    }

    private FixEngine launch()
    {
        startOnThread(framerRunner);
        logger.start();
        if (configuration.printErrorMessages())
        {
            startOnThread(errorPrinterRunner);
        }
        return this;
    }

    /**
     * Close the engine down, including stopping other running threads.
     */
    public synchronized void close()
    {
        quietClose(errorPrinterRunner);
        framerRunner.close();
        logger.close();
        super.close();
    }

}
