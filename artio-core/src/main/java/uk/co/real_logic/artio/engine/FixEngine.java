/*
 * Copyright 2015-2018 Real Logic Ltd, Adaptive Financial Consulting Ltd.
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
package uk.co.real_logic.artio.engine;

import io.aeron.ExclusivePublication;
import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.archive.client.AeronArchive;
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.artio.FixCounters;
import uk.co.real_logic.artio.GatewayProcess;
import uk.co.real_logic.artio.Reply;
import uk.co.real_logic.artio.StreamInformation;
import uk.co.real_logic.artio.engine.framer.FramerContext;
import uk.co.real_logic.artio.engine.framer.LibraryInfo;
import uk.co.real_logic.artio.timing.EngineTimers;

import java.io.File;
import java.util.List;

import static io.aeron.CommonContext.IPC_CHANNEL;
import static uk.co.real_logic.artio.dictionary.generation.Exceptions.closeAll;
import static uk.co.real_logic.artio.dictionary.generation.Exceptions.suppressingClose;

/**
 * A FIX Engine is a process in the gateway that accepts or initiates FIX connections and
 * hands them off to different FixLibrary instances. The engine can replicate and/or durably
 * store streams2 of FIX messages for replay, archival, administrative or analytics purposes.
 * <p>
 * Each engine can have one or more associated libraries that manage sessions and perform business
 * logic. These may run in the same JVM process or a different JVM process.
 *
 * @see uk.co.real_logic.artio.library.FixLibrary
 */
public final class FixEngine extends GatewayProcess
{
    private static final Object CLOSE_MUTEX = new Object();

    public static final int ENGINE_LIBRARY_ID = 0;

    private final EngineTimers timers;
    private final EngineConfiguration configuration;
    private final RecordingCoordinator recordingCoordinator;

    private EngineScheduler scheduler;
    private FramerContext framerContext;
    private EngineContext engineContext;

    /**
     * Launch the engine. This method starts up the engine threads and then returns.
     *
     * @param configuration the configuration to use for this engine.
     * @return the new FIX engine instance.
     */
    public static FixEngine launch(final EngineConfiguration configuration)
    {
        synchronized (CLOSE_MUTEX)
        {
            configuration.conclude();

            return new FixEngine(configuration).launch();
        }
    }

    /**
     * Query the engine for the list of libraries currently active.
     *
     * If the reply is <code>null</code> then the query hasn't been enqueued and the operation
     * should be retried on a duty cycle.
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
     * @param idleStrategy the idle strategy to use when polling this blocking operation.
     *
     * @return the reply object, or null if the request hasn't been successfully enqueued.
     */
    public Reply<?> resetSessionIds(final File backupLocation, final IdleStrategy idleStrategy)
    {
        return framerContext.resetSessionIds(backupLocation, idleStrategy);
    }

    /**
     * Resets the sequence number of a given session. Asynchronous method, the Reply instance
     * needs to be polled to ensure that it has completed.
     *
     * If the reply is <code>null</code> then the query hasn't been enqueued and the operation
     * should be retried on a duty cycle.
     *
     * @param sessionId the id of the session that you want to reset
     *
     * @return the reply object, or null if the request hasn't been successfully enqueued.
     */
    public Reply<?> resetSequenceNumber(final long sessionId)
    {
        return framerContext.resetSequenceNumber(sessionId);
    }

    /**
     * Gets the session id associated with some combination of id fields
     *
     * @param localCompId the senderCompId of messages sent by the gateway on this session.
     * @param remoteCompId the senderCompId of messages received by the gateway on this session.
     * @param localSubId the senderSubId of messages sent by the gateway on this session
     *                   or <code>null</code> if not used in session identification.
     * @param remoteSubId the senderSubId of messages received by the gateway on this session
     *                    or <code>null</code> if not used in session identification.
     * @param localLocationId the senderLocationId of messages sent by the gateway on this session
     *                        or <code>null</code> if not used in session identification.
     * @param remoteLocationId the senderLocationId of messages received by the gateway on this session
     *                         or <code>null</code> if not used in session identification.
     *
     * @return the reply object asynchronously wrapping the session id
     */
    public Reply<Long> lookupSessionId(
        final String localCompId,
        final String remoteCompId,
        final String localSubId,
        final String remoteSubId,
        final String localLocationId,
        final String remoteLocationId)
    {
        return framerContext.lookupSessionId(
            localCompId, remoteCompId, localSubId, remoteSubId, localLocationId, remoteLocationId);
    }

    private FixEngine(final EngineConfiguration configuration)
    {
        try
        {
            this.configuration = configuration;

            timers = new EngineTimers(configuration.clock());
            scheduler = configuration.scheduler();
            scheduler.configure(configuration.aeronContext());
            init(configuration);
            final AeronArchive.Context archiveContext = configuration.aeronArchiveContext();
            final AeronArchive aeronArchive =
                configuration.logAnyMessages() ? AeronArchive.connect(archiveContext.aeron(aeron)) : null;
            recordingCoordinator = new RecordingCoordinator(
                aeronArchive,
                configuration,
                aeron.conductorAgentInvoker(),
                configuration.archiverIdleStrategy());

            final ExclusivePublication replayPublication = replayPublication();
            engineContext = new EngineContext(
                configuration,
                errorHandler,
                replayPublication,
                fixCounters,
                aeron,
                aeronArchive,
                recordingCoordinator);
            initFramer(configuration, fixCounters, replayPublication.sessionId());
            initMonitoringAgent(timers.all(), configuration);
            recordingCoordinator.awaitReady();
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
            IPC_CHANNEL, configuration.outboundReplayStream());
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
            timers,
            aeron.conductorAgentInvoker(),
            recordingCoordinator);
    }

    private Image replayImage(final String name, final int replaySessionId)
    {
        final Subscription subscription = aeron.addSubscription(
            IPC_CHANNEL, configuration.outboundReplayStream());
        StreamInformation.print(name, subscription, configuration);

        // Await replay publication
        while (true)
        {
            final Image image = subscription.imageBySessionId(replaySessionId);
            if (image != null)
            {
                return image;
            }

            invokeAeronConductor();

            Thread.yield();
        }
    }

    // To be invoked by called called before a scheduler has launched
    private void invokeAeronConductor()
    {
        final AgentInvoker invoker = aeron.conductorAgentInvoker();
        if (invoker != null)
        {
            invoker.invoke();
        }
    }

    private FixEngine launch()
    {
        scheduler.launch(
            configuration,
            errorHandler,
            framerContext.framer(),
            engineContext.archivingAgent(),
            monitoringAgent,
            conductorAgent(),
            recordingCoordinator);

        return this;
    }

    /**
     * Close the engine down, including stopping other running threads.
     *
     * This does not remove files associated with the engine, that are persistent
     * over multiple runs of the engine.
     */
    public void close()
    {
        synchronized (CLOSE_MUTEX)
        {
            closeAll(scheduler, engineContext, configuration, super::close);
        }
    }

    public EngineConfiguration configuration()
    {
        return configuration;
    }
}
