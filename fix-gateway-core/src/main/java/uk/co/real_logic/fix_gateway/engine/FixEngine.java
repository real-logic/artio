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
import uk.co.real_logic.fix_gateway.engine.logger.*;
import uk.co.real_logic.fix_gateway.session.SessionIdStrategy;

import java.io.File;
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
     * @param idleStrategy the strategy to idle with whilst waiting for a response.
     * @return a list of currently active libraries.
     */
    public List<LibraryInfo> libraries(final IdleStrategy idleStrategy)
    {
        final QueryLibraries query = new QueryLibraries();
        sendAdminCommand(idleStrategy, query);

        return query.awaitResponse(idleStrategy);
    }

    /**
     * Resets the set of session ids.
     *
     * @param backupLocation the location to backup the current session ids file to.
     *                       Can be null to indicate that nobackup is required.
     * @throws IllegalStateException thrown in the case that there was an error in backing up,
     *                               or that there were currently connected sessions.
     */
    public void resetSessionIds(final File backupLocation, final IdleStrategy idleStrategy)
        throws IllegalStateException
    {
        final ResetSessionIds resetSessionIds = new ResetSessionIds(backupLocation);
        sendAdminCommand(idleStrategy, resetSessionIds);
    }

    private void sendAdminCommand(final IdleStrategy idleStrategy, final AdminCommand query)
    {
        while (!adminCommands.offer(query))
        {
            idleStrategy.idle();
        }
        idleStrategy.reset();
    }

    private FixEngine(final EngineConfiguration configuration)
    {
        init(configuration);
        this.configuration = configuration;

        final SequenceNumberIndexWriter sentSequenceNumberIndex = new SequenceNumberIndexWriter(
            configuration.sentSequenceNumberBuffer(), configuration.sentSequenceNumberIndex(), errorBuffer);
        final SequenceNumberIndexWriter receivedSequenceNumberIndex = new SequenceNumberIndexWriter(
            configuration.receivedSequenceNumberBuffer(), configuration.receivedSequenceNumberIndex(), errorBuffer);
        final IndexedPositionWriter indexedPositionWriter = new IndexedPositionWriter(
            configuration.indexedPositionBuffer().buffer(), errorBuffer);
        initFramer(configuration, fixCounters);
        initLogger(configuration, sentSequenceNumberIndex, receivedSequenceNumberIndex, indexedPositionWriter);
        initErrorPrinter(configuration);
    }

    private void initLogger(
        final EngineConfiguration configuration,
        final SequenceNumberIndexWriter sentSequenceNumberIndex,
        final SequenceNumberIndexWriter receivedSequenceNumberIndex,
        final IndexedPositionWriter indexedPositionWriter)
    {
        logger = new Logger(
            configuration, inboundLibraryStreams, outboundLibraryStreams, errorBuffer, replayPublication(),
            sentSequenceNumberIndex, receivedSequenceNumberIndex, indexedPositionWriter);
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
        final SessionIdStrategy sessionIdStrategy = configuration.sessionIdStrategy();
        final SessionIds sessionIds = new SessionIds(configuration.sessionIdBuffer(), sessionIdStrategy, errorBuffer);
        final IdleStrategy idleStrategy = configuration.framerIdleStrategy();
        final Subscription librarySubscription = outboundLibraryStreams.subscription();

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
            adminCommands, sessionIdStrategy, sessionIds,
            new SequenceNumberIndexReader(configuration.sentSequenceNumberBuffer()),
            new SequenceNumberIndexReader(configuration.receivedSequenceNumberBuffer()),
            new IndexedPositionReader(configuration.indexedPositionBuffer().buffer()));
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
        configuration.close();
        super.close();
    }

}
