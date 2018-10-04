/*
 * Copyright 2015-2017 Real Logic Ltd.
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
package uk.co.real_logic.artio.engine.framer;

import io.aeron.Image;
import org.agrona.ErrorHandler;
import org.agrona.LangUtil;
import org.agrona.collections.LongHashSet;
import org.agrona.concurrent.*;
import uk.co.real_logic.artio.FixCounters;
import uk.co.real_logic.artio.Reply;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.EngineContext;
import uk.co.real_logic.artio.engine.logger.SequenceNumberIndexReader;
import uk.co.real_logic.artio.protocol.GatewayPublication;
import uk.co.real_logic.artio.protocol.Streams;
import uk.co.real_logic.artio.replication.ClusterableStreams;
import uk.co.real_logic.artio.session.SessionIdStrategy;
import uk.co.real_logic.artio.timing.EngineTimers;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Context that injects all the necessary information into different Framer classes.
 *
 * This enables many classes in the framer package to be package scoped as they don't
 * need the Fix Engine itself to touch them.
 */
public class FramerContext
{
    private static final int ADMIN_COMMAND_CAPACITY = 16;

    private final QueuedPipe<AdminCommand> adminCommands = new ManyToOneConcurrentArrayQueue<>(ADMIN_COMMAND_CAPACITY);

    private final Framer framer;

    private final GatewaySessions gatewaySessions;
    private final SequenceNumberIndexReader sentSequenceNumberIndex;
    private final SequenceNumberIndexReader receivedSequenceNumberIndex;
    private final GatewayPublication outboundPublication;
    private final GatewayPublication inboundLibraryPublication;
    private final SessionContexts sessionContexts;
    private final AgentInvoker conductorAgentInvoker;

    public FramerContext(
        final EngineConfiguration configuration,
        final FixCounters fixCounters,
        final EngineContext engineContext,
        final ErrorHandler errorHandler,
        final Image replayImage,
        final Image slowReplayImage,
        final EngineTimers timers,
        final AgentInvoker conductorAgentInvoker)
    {
        this.conductorAgentInvoker = conductorAgentInvoker;
        final ClusterableStreams streams = engineContext.streams();
        final SessionIdStrategy sessionIdStrategy = configuration.sessionIdStrategy();
        this.sessionContexts = new SessionContexts(configuration.sessionIdBuffer(), sessionIdStrategy, errorHandler);
        final IdleStrategy idleStrategy = configuration.framerIdleStrategy();
        final Streams outboundLibraryStreams = engineContext.outboundLibraryStreams();
        final Streams inboundLibraryStreams = engineContext.inboundLibraryStreams();

        final SystemEpochClock clock = new SystemEpochClock();
        final LongHashSet replicatedConnectionIds = new LongHashSet();
        final GatewayPublication inboundClusterablePublication =
            inboundLibraryStreams.gatewayPublication(idleStrategy, "inboundPublication");
        this.inboundLibraryPublication = engineContext.inboundLibraryPublication();
        this.outboundPublication = outboundLibraryStreams.gatewayPublication(idleStrategy, "outboundPublication");

        gatewaySessions = new GatewaySessions(
            clock,
            outboundPublication,
            sessionIdStrategy,
            configuration.sessionCustomisationStrategy(),
            fixCounters,
            configuration.authenticationStrategy(),
            configuration.messageValidationStrategy(),
            configuration.sessionBufferSize(),
            configuration.sendingTimeWindowInMs(),
            configuration.reasonableTransmissionTimeInMs(),
            errorHandler,
            sessionContexts,
            configuration.sessionPersistenceStrategy());

        final EndPointFactory endPointFactory = new EndPointFactory(
            configuration,
            sessionContexts,
            inboundLibraryPublication,
            inboundClusterablePublication,
            fixCounters,
            errorHandler,
            replicatedConnectionIds,
            gatewaySessions,
            engineContext.senderSequenceNumbers());

        sentSequenceNumberIndex = new SequenceNumberIndexReader(
            configuration.sentSequenceNumberBuffer(), errorHandler);
        receivedSequenceNumberIndex = new SequenceNumberIndexReader(
            configuration.receivedSequenceNumberBuffer(), errorHandler);

        final FinalImagePositions finalImagePositions = new FinalImagePositions();

        framer = new Framer(
            clock,
            timers.outboundTimer(),
            timers.sendTimer(),
            configuration,
            endPointFactory,
            streams,
            engineContext.outboundLibrarySubscription(
                "outboundLibrarySubscription", finalImagePositions),
            engineContext.outboundLibrarySubscription(
                "outboundSlowSubscription", null),
            replayImage,
            slowReplayImage,
            engineContext.inboundReplayQuery(),
            outboundPublication,
            inboundLibraryPublication,
            adminCommands,
            sessionIdStrategy,
            sessionContexts,
            sentSequenceNumberIndex,
            receivedSequenceNumberIndex,
            gatewaySessions,
            errorHandler,
            configuration.agentNamePrefix(),
            engineContext.inboundCompletionPosition(),
            engineContext.outboundLibraryCompletionPosition(),
            engineContext.outboundClusterCompletionPosition(),
            finalImagePositions,
            conductorAgentInvoker);
    }

    public Agent framer()
    {
        return framer;
    }

    public Reply<List<LibraryInfo>> libraries()
    {
        final QueryLibrariesCommand reply = new QueryLibrariesCommand();

        if (adminCommands.offer(reply))
        {
            return reply;
        }

        return null;
    }

    public Reply<?> resetSequenceNumber(final long sessionId)
    {
        final ResetSequenceNumberCommand reply = new ResetSequenceNumberCommand(
            sessionId,
            gatewaySessions,
            sessionContexts,
            receivedSequenceNumberIndex,
            sentSequenceNumberIndex,
            inboundLibraryPublication,
            outboundPublication);

        if (adminCommands.offer(reply))
        {
            return reply;
        }

        return null;
    }

    public Reply<?> resetSessionIds(final File backupLocation, final IdleStrategy idleStrategy)
    {
        if (backupLocation != null && !backupLocation.exists())
        {
            try
            {
                if (!backupLocation.createNewFile())
                {
                    throw new IllegalStateException("Could not create: " + backupLocation);
                }
            }
            catch (final IOException ex)
            {
                LangUtil.rethrowUnchecked(ex);
            }
        }

        final ResetSessionIdsCommand command = new ResetSessionIdsCommand(backupLocation);
        if (adminCommands.offer(command))
        {
            return command;
        }

        return null;
    }

    public Reply<Long> lookupSessionId(
        final String localCompId,
        final String remoteCompId,
        final String localSubId,
        final String remoteSubId,
        final String localLocationId,
        final String remoteLocationId)
    {
        final LookupSessionIdCommand command = new LookupSessionIdCommand(
            localCompId,
            remoteCompId,
            localSubId,
            remoteSubId,
            localLocationId,
            remoteLocationId);

        if (adminCommands.offer(command))
        {
            return command;
        }

        return null;
    }
}
