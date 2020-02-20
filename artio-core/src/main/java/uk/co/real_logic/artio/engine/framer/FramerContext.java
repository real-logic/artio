/*
 * Copyright 2015-2020 Real Logic Limited, Adaptive Financial Consulting Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
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
import org.agrona.concurrent.*;
import uk.co.real_logic.artio.CommonConfiguration;
import uk.co.real_logic.artio.FixCounters;
import uk.co.real_logic.artio.Reply;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.EngineContext;
import uk.co.real_logic.artio.engine.RecordingCoordinator;
import uk.co.real_logic.artio.engine.SessionInfo;
import uk.co.real_logic.artio.engine.logger.SequenceNumberIndexReader;
import uk.co.real_logic.artio.protocol.GatewayPublication;
import uk.co.real_logic.artio.protocol.Streams;
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
    private static final int ADMIN_COMMAND_CAPACITY = 64;

    private final QueuedPipe<AdminCommand> adminCommands = new ManyToOneConcurrentArrayQueue<>(ADMIN_COMMAND_CAPACITY);
    private final SystemEpochClock epochClock = new SystemEpochClock();

    private final Framer framer;

    private final EngineConfiguration configuration;
    private final GatewaySessions gatewaySessions;
    private final SequenceNumberIndexReader sentSequenceNumberIndex;
    private final SequenceNumberIndexReader receivedSequenceNumberIndex;
    private final GatewayPublication outboundPublication;
    private final GatewayPublication inboundPublication;
    private final SessionContexts sessionContexts;

    public FramerContext(
        final EngineConfiguration configuration,
        final FixCounters fixCounters,
        final EngineContext engineContext,
        final ErrorHandler errorHandler,
        final Image replayImage,
        final Image slowReplayImage,
        final EngineTimers timers,
        final AgentInvoker conductorAgentInvoker,
        final RecordingCoordinator recordingCoordinator)
    {
        this.configuration = configuration;

        final SessionIdStrategy sessionIdStrategy = configuration.sessionIdStrategy();

        final IdleStrategy idleStrategy = configuration.framerIdleStrategy();
        final Streams outboundLibraryStreams = engineContext.outboundLibraryStreams();

        this.sessionContexts = new SessionContexts(
            configuration.sessionIdBuffer(), sessionIdStrategy, errorHandler);

        this.inboundPublication = engineContext.inboundPublication();
        this.outboundPublication = outboundLibraryStreams.gatewayPublication(idleStrategy, "outboundPublication");

        sentSequenceNumberIndex = new SequenceNumberIndexReader(
            configuration.sentSequenceNumberBuffer(), errorHandler, configuration.logFileDir());
        receivedSequenceNumberIndex = new SequenceNumberIndexReader(
            configuration.receivedSequenceNumberBuffer(), errorHandler, null);

        gatewaySessions = new GatewaySessions(
            epochClock,
            inboundPublication,
            outboundPublication,
            sessionIdStrategy,
            configuration.sessionCustomisationStrategy(),
            fixCounters,
            configuration,
            errorHandler,
            sessionContexts,
            configuration.sessionPersistenceStrategy(),
            sentSequenceNumberIndex,
            receivedSequenceNumberIndex);

        final EndPointFactory endPointFactory = new EndPointFactory(
            configuration,
            sessionContexts,
            inboundPublication,
            fixCounters,
            errorHandler,
            gatewaySessions,
            engineContext.senderSequenceNumbers());

        final FinalImagePositions finalImagePositions = new FinalImagePositions();

        framer = new Framer(
            epochClock,
            timers.outboundTimer(),
            timers.sendTimer(),
            configuration,
            endPointFactory,
            engineContext.outboundLibrarySubscription(
                "outboundLibrarySubscription", finalImagePositions),
            engineContext.outboundLibrarySubscription(
                "outboundSlowSubscription", null),
            replayImage,
            slowReplayImage,
            engineContext.inboundReplayQuery(),
            outboundPublication,
            inboundPublication,
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
            finalImagePositions,
            conductorAgentInvoker,
            recordingCoordinator);
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
            inboundPublication,
            outboundPublication,
            epochClock.time());

        if (adminCommands.offer(reply))
        {
            return reply;
        }

        return null;
    }

    public Reply<?> resetSessionIds(final File backupLocation)
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

    public void startClose()
    {
        final IdleStrategy idleStrategy = CommonConfiguration.backoffIdleStrategy();
        final StartCloseCommand command = new StartCloseCommand();
        while (!adminCommands.offer(command))
        {
            idleStrategy.idle();
        }
        idleStrategy.reset();

        while (!command.hasCompleted())
        {
            idleStrategy.idle();
        }
        idleStrategy.reset();

        if (command.hasErrored())
        {
            LangUtil.rethrowUnchecked(command.error());
        }
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

    public Reply<?> bind(final boolean bind)
    {
        final BindCommand command = new BindCommand(bind);

        if (bind && !configuration.hasBindAddress())
        {
            command.onError(new IllegalStateException("Missing address: EngineConfiguration.bindTo()"));
            return command;
        }

        if (adminCommands.offer(command))
        {
            return command;
        }

        return null;
    }

    public boolean offer(final WriteMetaDataResponse response)
    {
        return adminCommands.offer(response);
    }

    public List<SessionInfo> allSessions()
    {
        return sessionContexts.allSessions();
    }
}
