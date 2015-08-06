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
package uk.co.real_logic.fix_gateway.engine.logger;

import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.logbuffer.BufferClaim;
import uk.co.real_logic.agrona.ErrorHandler;
import uk.co.real_logic.agrona.concurrent.Agent;
import uk.co.real_logic.agrona.concurrent.AgentRunner;
import uk.co.real_logic.agrona.concurrent.BackoffIdleStrategy;
import uk.co.real_logic.agrona.concurrent.CompositeAgent;
import uk.co.real_logic.fix_gateway.StaticConfiguration;
import uk.co.real_logic.fix_gateway.replication.ReplicatedStream;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Top level entry point for the whole logging module.
 */
public class LoggerModule implements AutoCloseable
{

    private final StaticConfiguration configuration;
    private AgentRunner loggingRunner;

    public LoggerModule(final StaticConfiguration configuration,
                        final ReplicatedStream inboundStreams,
                        final ReplicatedStream outboundStreams,
                        final ErrorHandler errorHandler)
    {
        this.configuration = configuration;
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
                new AgentRunner(backoffIdleStrategy(), errorHandler, null, loggingAgent);
        }
    }

    public void start()
    {
        if (isLoggingMessages())
        {
            AgentRunner.startOnThread(loggingRunner);
        }
    }

    private boolean isLoggingMessages()
    {
        return configuration.logInboundMessages() || configuration.logOutboundMessages();
    }

    private BackoffIdleStrategy backoffIdleStrategy()
    {
        return new BackoffIdleStrategy(1, 1, 1, 1 << 20);
    }

    public void close()
    {
        loggingRunner.close();
    }
}
