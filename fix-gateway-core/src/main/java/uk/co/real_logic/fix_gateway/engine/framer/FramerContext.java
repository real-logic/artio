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
package uk.co.real_logic.fix_gateway.engine.framer;

import io.aeron.Subscription;
import org.agrona.ErrorHandler;
import org.agrona.LangUtil;
import org.agrona.collections.LongHashSet;
import org.agrona.concurrent.*;
import uk.co.real_logic.fix_gateway.FixCounters;
import uk.co.real_logic.fix_gateway.Reply;
import uk.co.real_logic.fix_gateway.engine.EngineConfiguration;
import uk.co.real_logic.fix_gateway.engine.EngineContext;
import uk.co.real_logic.fix_gateway.engine.EngineDescriptorStore;
import uk.co.real_logic.fix_gateway.engine.SessionInfo;
import uk.co.real_logic.fix_gateway.engine.logger.SequenceNumberIndexReader;
import uk.co.real_logic.fix_gateway.protocol.GatewayPublication;
import uk.co.real_logic.fix_gateway.protocol.Streams;
import uk.co.real_logic.fix_gateway.replication.ClusterableStreams;
import uk.co.real_logic.fix_gateway.session.SessionIdStrategy;
import uk.co.real_logic.fix_gateway.timing.EngineTimers;

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

    public FramerContext(
        final EngineConfiguration configuration,
        final FixCounters fixCounters,
        final EngineContext engineContext,
        final ErrorHandler errorHandler,
        final Subscription replaySubscription,
        final EngineDescriptorStore engineDescriptorStore,
        final EngineTimers timers)
    {
        final ClusterableStreams streams = engineContext.streams();
        final SessionIdStrategy sessionIdStrategy = configuration.sessionIdStrategy();
        final SessionIds sessionIds = new SessionIds(configuration.sessionIdBuffer(), sessionIdStrategy, errorHandler);
        final IdleStrategy idleStrategy = configuration.framerIdleStrategy();
        final Streams outboundLibraryStreams = engineContext.outboundLibraryStreams();
        final Streams inboundLibraryStreams = engineContext.inboundLibraryStreams();

        final SystemEpochClock clock = new SystemEpochClock();
        final LongHashSet replicatedConnectionIds = new LongHashSet(SessionIds.MISSING);

        final EndPointFactory endPointFactory = new EndPointFactory(
            configuration,
            sessionIdStrategy,
            sessionIds,
            inboundLibraryStreams,
            engineContext.inboundLibraryPublication(),
            idleStrategy,
            fixCounters,
            errorHandler,
            replicatedConnectionIds);

        gatewaySessions = new GatewaySessions(
            clock,
            outboundLibraryStreams.gatewayPublication(idleStrategy),
            sessionIdStrategy,
            configuration.sessionCustomisationStrategy(),
            fixCounters,
            configuration.authenticationStrategy(),
            configuration.messageValidationStrategy(),
            configuration.sessionBufferSize(),
            configuration.sendingTimeWindowInMs());

        sentSequenceNumberIndex = new SequenceNumberIndexReader(configuration.sentSequenceNumberBuffer());
        receivedSequenceNumberIndex = new SequenceNumberIndexReader(configuration.receivedSequenceNumberBuffer());
        outboundPublication = outboundLibraryStreams.gatewayPublication(idleStrategy);
        inboundLibraryPublication = engineContext.inboundLibraryPublication();

        framer = new Framer(
            clock,
            timers.outboundTimer(),
            timers.sendTimer(),
            configuration,
            endPointFactory,
            engineContext.outboundClusterSubscription(),
            engineContext.outboundLibrarySubscription(),
            engineContext.outboundLibrarySubscription(),
            replaySubscription,
            adminCommands,
            sessionIdStrategy,
            sessionIds,
            sentSequenceNumberIndex,
            receivedSequenceNumberIndex,
            gatewaySessions,
            engineContext.inboundReplayQuery(),
            errorHandler,
            outboundPublication,
            inboundLibraryPublication,
            streams,
            engineDescriptorStore,
            replicatedConnectionIds,
            endPointFactory.inboundPublication());
    }

    public Agent framer()
    {
        return framer;
    }

    public List<LibraryInfo> libraries(final IdleStrategy idleStrategy)
    {
        final QueryLibrariesCommand command = new QueryLibrariesCommand();
        sendAdminCommand(idleStrategy, command);

        return command.awaitResponse(idleStrategy);
    }

    private void sendAdminCommand(final IdleStrategy idleStrategy, final AdminCommand query)
    {
        while (!adminCommands.offer(query))
        {
            idleStrategy.idle();
        }
        idleStrategy.reset();
    }

    public Reply<?> resetSequenceNumber(final long sessionId)
    {
        final ResetSequenceNumberReply reply = new ResetSequenceNumberReply(
            sessionId,
            gatewaySessions,
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

    public List<SessionInfo> gatewaySessions(final IdleStrategy idleStrategy)
    {
        final GatewaySessionsCommand command = new GatewaySessionsCommand();
        sendAdminCommand(idleStrategy, command);

        return command.awaitResponse(idleStrategy);
    }

    public void resetSessionIds(final File backupLocation, final IdleStrategy idleStrategy)
    {
        if (backupLocation != null && !backupLocation.exists())
        {
            try
            {
                backupLocation.createNewFile();
            }
            catch (IOException e)
            {
                LangUtil.rethrowUnchecked(e);
            }
        }

        final ResetSessionIdsCommand command = new ResetSessionIdsCommand(backupLocation);
        sendAdminCommand(idleStrategy, command);
        command.awaitResponse(idleStrategy);
    }
}
