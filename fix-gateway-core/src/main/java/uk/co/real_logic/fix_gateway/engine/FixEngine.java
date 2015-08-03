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
import uk.co.real_logic.aeron.logbuffer.BufferClaim;
import uk.co.real_logic.agrona.concurrent.*;
import uk.co.real_logic.fix_gateway.FixCounters;
import uk.co.real_logic.fix_gateway.GatewayProcess;
import uk.co.real_logic.fix_gateway.StaticConfiguration;
import uk.co.real_logic.fix_gateway.engine.framer.Framer;
import uk.co.real_logic.fix_gateway.engine.framer.Multiplexer;
import uk.co.real_logic.fix_gateway.engine.framer.SessionIds;
import uk.co.real_logic.fix_gateway.engine.logger.*;
import uk.co.real_logic.fix_gateway.session.SessionIdStrategy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FixEngine extends GatewayProcess
{

    private AgentRunner framerRunner;
    private AgentRunner loggingRunner;

    FixEngine(final StaticConfiguration configuration)
    {
        super(configuration);

        initFramer(configuration, fixCounters);
        initLogger(configuration);
    }

    private void initLogger(final StaticConfiguration configuration)
    {
        if (isLoggingMessages())
        {
            final int loggerCacheCapacity = configuration.loggerCacheCapacity();
            final String logFileDir = configuration.logFileDir();

            final List<Subscription> subscriptions = new ArrayList<>();
            if (configuration.logInboundMessages())
            {
                subscriptions.add(inboundStreams.dataSubscription());
            }
            if (configuration.logOutboundMessages())
            {
                subscriptions.add(outboundStreams.dataSubscription());
            }
            final Archiver archiver = new Archiver(
                LoggerUtil.newArchiveMetaData(configuration), logFileDir, loggerCacheCapacity, subscriptions);
            final ArchiveReader archiveReader = new ArchiveReader(
                LoggerUtil::mapExistingFile, LoggerUtil.newArchiveMetaData(configuration), logFileDir, loggerCacheCapacity);

            final List<Index> indices = Arrays.asList(
                new ReplayIndex(logFileDir, configuration.indexFileSize(), loggerCacheCapacity, LoggerUtil::map));
            final Indexer indexer = new Indexer(indices, outboundStreams);

            final ReplayQuery replayQuery = new ReplayQuery(
                logFileDir, loggerCacheCapacity, LoggerUtil::mapExistingFile, archiveReader);
            final Replayer replayer = new Replayer(
                inboundStreams.dataSubscription(),
                replayQuery,
                outboundStreams.dataPublication(),
                new BufferClaim(),
                backoffIdleStrategy());

            final Agent loggingAgent = new CompositeAgent(archiver, new CompositeAgent(indexer, replayer));

            loggingRunner =
                new AgentRunner(backoffIdleStrategy(), Throwable::printStackTrace, fixCounters.exceptions(), loggingAgent);
        }
    }

    private void initFramer(final StaticConfiguration configuration, final FixCounters fixCounters)
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
            fixCounters);

        final Framer framer = new Framer(configuration, handler, multiplexer, dataSubscription,
            inboundStreams.gatewayPublication(), sessionIdStrategy, sessionIds);
        multiplexer.framer(framer);
        framerRunner = new AgentRunner(idleStrategy, Throwable::printStackTrace, fixCounters.exceptions(), framer);
    }

    private BackoffIdleStrategy backoffIdleStrategy()
    {
        return new BackoffIdleStrategy(1, 1, 1, 1 << 20);
    }

    public static FixEngine launch(final StaticConfiguration configuration)
    {
        return new FixEngine(configuration.conclude()).start();
    }

    private FixEngine start()
    {
        start(framerRunner);
        if (isLoggingMessages())
        {
            start(loggingRunner);
        }
        return this;
    }

    private boolean isLoggingMessages()
    {
        return configuration.logInboundMessages() || configuration.logOutboundMessages();
    }

    private void start(final AgentRunner runner)
    {
        final Thread thread = new Thread(runner);
        thread.setName(runner.agent().roleName());
        thread.start();
    }

    public synchronized void close()
    {
        framerRunner.close();
        loggingRunner.close();

        super.close();
    }

}
