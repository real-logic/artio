/*
 * Copyright 2015-2016 Real Logic Ltd.
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

import io.aeron.Publication;
import io.aeron.Subscription;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.fix_gateway.FixCounters;
import uk.co.real_logic.fix_gateway.GatewayProcess;
import uk.co.real_logic.fix_gateway.Reply;
import uk.co.real_logic.fix_gateway.dictionary.generation.Exceptions;
import uk.co.real_logic.fix_gateway.engine.framer.FramerContext;
import uk.co.real_logic.fix_gateway.engine.framer.LibraryInfo;
import uk.co.real_logic.fix_gateway.replication.ClusterableStreams;
import uk.co.real_logic.fix_gateway.timing.EngineTimers;

import java.io.File;
import java.util.List;

import static org.agrona.concurrent.AgentRunner.startOnThread;
import static uk.co.real_logic.fix_gateway.dictionary.generation.Exceptions.suppressingClose;

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
    public static final int ENGINE_LIBRARY_ID = 0;

    private final EngineTimers timers = new EngineTimers();
    private final EngineConfiguration configuration;
    private final EngineDescriptorStore engineDescriptorStore;

    private AgentRunner framerRunner;
    private FramerContext framerContext;
    private EngineContext engineContext;
    private ClusterableStreams streams;

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
    public Reply<List<LibraryInfo>> libraries()
    {
        return framerContext.libraries();
    }

    /**
     * Resets the set of session ids.
     *
     * @param backupLocation the location to backup the current session ids file to.
     *                       Can be null to indicate that no backup is required.
     */
    public void resetSessionIds(final File backupLocation, final IdleStrategy idleStrategy)
    {
        framerContext.resetSessionIds(backupLocation, idleStrategy);
    }

    /**
     * Resets the sequence number of a given session. Asynchronous method, the Reply instance
     * needs to be polled to ensure that it has completed.
     *
     * @param sessionId the id of the session that you want to reset
     *
     * @return the reply object, or null if the request hasn't been successfully enqueued.
     */
    public Reply<?> resetSequenceNumber(final long sessionId)
    {
        return framerContext.resetSequenceNumber(sessionId);
    }

    private FixEngine(final EngineConfiguration configuration)
    {
        try
        {
            init(configuration);
            this.configuration = configuration;
            engineDescriptorStore = new EngineDescriptorStore(errorHandler);

            engineContext = EngineContext.of(
                configuration,
                errorHandler,
                replayPublication(),
                fixCounters,
                aeron,
                engineDescriptorStore);
            streams = engineContext.streams();
            initFramer(configuration, fixCounters);
            initMonitoringAgent(timers.all(), configuration);
        }
        catch (final Exception e)
        {
            if (engineContext != null)
            {
                engineContext.completeDuringStartup();
            }

            suppressingClose(this, e);

            throw e;
        }
    }

    private Publication replayPublication()
    {
        return aeron.addPublication(configuration.libraryAeronChannel(), OUTBOUND_REPLAY_STREAM);
    }

    private void initFramer(final EngineConfiguration configuration, final FixCounters fixCounters)
    {
        framerContext = new FramerContext(
            configuration,
            fixCounters,
            engineContext,
            errorHandler,
            replaySubscription(),
            engineDescriptorStore,
            timers);
        framerRunner = new AgentRunner(
            configuration.framerIdleStrategy(), errorHandler, null, framerContext.framer());
    }

    /**
     * Check whether you're the leader of a cluster. NB: if you aren't running in a cluster
     * this will always return true.
     *
     * @return true if you're a cluster leader, false otherwise
     */
    public boolean isLeader()
    {
        return streams.isLeader();
    }

    private Subscription replaySubscription()
    {
        return aeron.addSubscription(configuration.libraryAeronChannel(), OUTBOUND_REPLAY_STREAM);
    }

    private FixEngine launch()
    {
        startOnThread(framerRunner);
        engineContext.start();
        start();
        return this;
    }

    /**
     * Close the engine down, including stopping other running threads.
     *
     * This does not remove files associated with the engine, that are persistent
     * over multiple runs of the engine.
     */
    public synchronized void close()
    {
        Exceptions.closeAll(framerRunner, engineContext, configuration, super::close);
    }

    public EngineConfiguration configuration()
    {
        return configuration;
    }
}
