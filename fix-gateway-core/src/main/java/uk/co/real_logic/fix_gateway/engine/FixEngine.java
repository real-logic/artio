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

import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.Subscription;
import org.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.fix_gateway.FixCounters;
import uk.co.real_logic.fix_gateway.GatewayProcess;
import uk.co.real_logic.fix_gateway.Reply;
import uk.co.real_logic.fix_gateway.StreamInformation;
import uk.co.real_logic.fix_gateway.engine.framer.FramerContext;
import uk.co.real_logic.fix_gateway.engine.framer.LibraryInfo;
import uk.co.real_logic.fix_gateway.replication.ClusterableStreams;
import uk.co.real_logic.fix_gateway.timing.EngineTimers;

import java.io.File;
import java.util.List;

import static io.aeron.CommonContext.IPC_CHANNEL;
import static uk.co.real_logic.fix_gateway.dictionary.generation.Exceptions.closeAll;
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

    private final EngineTimers timers;
    private final EngineConfiguration configuration;
    private final EngineDescriptorStore engineDescriptorStore;

    private EngineScheduler scheduler;
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
            timers = new EngineTimers(configuration.nanoClock());
            init(configuration);
            this.configuration = configuration;
            scheduler = configuration.scheduler();
            engineDescriptorStore = new EngineDescriptorStore(errorHandler);

            final ExclusivePublication replayPublication = replayPublication();
            engineContext = EngineContext.of(
                configuration,
                errorHandler,
                replayPublication,
                fixCounters,
                aeron,
                engineDescriptorStore);
            streams = engineContext.streams();
            initFramer(configuration, fixCounters, replayPublication.sessionId());
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

    private ExclusivePublication replayPublication()
    {
        final ExclusivePublication publication = aeron.addExclusivePublication(
            IPC_CHANNEL, OUTBOUND_REPLAY_STREAM);
        StreamInformation.print("replayPublication", publication, configuration);
        return publication;
    }

    private void initFramer(
        final EngineConfiguration configuration, final FixCounters fixCounters, final int replaySessionId)
    {
        framerContext = new FramerContext(
            configuration,
            fixCounters,
            engineContext,
            errorHandler,
            replayImage("replay", replaySessionId),
            replayImage("slow-replay", replaySessionId),
            engineDescriptorStore,
            timers);
    }

    /**
     * Check whether this node believes itself to bethe leader of a cluster. NB: if you aren't running in a cluster
     * this will always return true.
     *
     * NB: This is provided on a "best effort" basis. If this node has become netsplit from other members in a cluster
     * and they have timed it out, and elected a new leader, this method may return true when another node is actually
     * considered the leader by a quorum of members.
     *
     * @return true if this node believes itself to be a cluster leader, false otherwise
     */
    public boolean isLeader()
    {
        return streams.isLeader();
    }

    private Image replayImage(final String name, final int replaySessionId)
    {
        final Subscription subscription = aeron.addSubscription(
            IPC_CHANNEL, OUTBOUND_REPLAY_STREAM);
        StreamInformation.print(name, subscription, configuration);

        // Await replay publication
        while (true)
        {
            final Image image = subscription.imageBySessionId(replaySessionId);
            if (image != null)
            {
                return image;
            }

            Thread.yield();
        }
    }

    private FixEngine launch()
    {
        scheduler.launch(
            configuration,
            errorHandler,
            framerContext.framer(),
            engineContext.archivingAgent(),
            monitoringAgent);

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
        closeAll(scheduler, engineContext, configuration, super::close);
    }

    public EngineConfiguration configuration()
    {
        return configuration;
    }
}
