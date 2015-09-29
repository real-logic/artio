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

import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.logbuffer.BufferClaim;
import uk.co.real_logic.agrona.ErrorHandler;
import uk.co.real_logic.agrona.concurrent.Agent;
import uk.co.real_logic.agrona.concurrent.AgentRunner;
import uk.co.real_logic.agrona.concurrent.CompositeAgent;
import uk.co.real_logic.fix_gateway.engine.EngineConfiguration;
import uk.co.real_logic.fix_gateway.streams.Streams;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static uk.co.real_logic.agrona.concurrent.AgentRunner.startOnThread;

/**
 * Top level entry point for the whole logging module.
 */
public class Logger implements AutoCloseable
{
    private final EngineConfiguration configuration;
    private final Streams inboundLibraryStreams;
    private final Streams outboundLibraryStreams;
    private final Publication replayPublication;
    private final ErrorHandler errorHandler;
    private final SequenceNumbers sequenceNumbers;

    private Archiver archiver;
    private ArchiveReader archiveReader;
    private AgentRunner loggingRunner;

    public Logger(
        final EngineConfiguration configuration,
        final Streams inboundLibraryStreams,
        final Streams outboundLibraryStreams,
        final ErrorHandler errorHandler,
        final Publication replayPublication,
        final SequenceNumbers sequenceNumbers)
    {
        this.configuration = configuration;
        this.inboundLibraryStreams = inboundLibraryStreams;
        this.outboundLibraryStreams = outboundLibraryStreams;
        this.replayPublication = replayPublication;
        this.errorHandler = errorHandler;
        this.sequenceNumbers = sequenceNumbers;
    }

    public void init()
    {
        if (isLoggingMessages())
        {
            initArchival();
            initReplay();
        }
    }

    public void initReplay()
    {
        if (configuration.logOutboundMessages())
        {
            final int loggerCacheCapacity = configuration.loggerCacheCapacity();
            final String logFileDir = configuration.logFileDir();
            final List<Index> indices = Arrays.asList(
                new ReplayIndex(logFileDir, configuration.indexFileSize(), loggerCacheCapacity, LoggerUtil::map),
                sequenceNumbers);
            final Indexer indexer = new Indexer(indices, outboundLibraryStreams.subscription());

            final ReplayQuery replayQuery = new ReplayQuery(
                logFileDir, loggerCacheCapacity, LoggerUtil::mapExistingFile, archiveReader);
            final Replayer replayer = new Replayer(
                inboundLibraryStreams.subscription(),
                replayQuery,
                replayPublication,
                new BufferClaim(),
                configuration.loggerIdleStrategy());

            final Agent loggingAgent = new CompositeAgent(archiver, new CompositeAgent(indexer, replayer));

            loggingRunner = newRunner(loggingAgent);
        }
    }

    private AgentRunner newRunner(final Agent loggingAgent)
    {
        return new AgentRunner(configuration.loggerIdleStrategy(), errorHandler, null, loggingAgent);
    }

    public void initArchival()
    {
        final int loggerCacheCapacity = configuration.loggerCacheCapacity();
        final String logFileDir = configuration.logFileDir();

        final List<Subscription> subscriptions = new ArrayList<>();
        if (configuration.logInboundMessages())
        {
            subscriptions.add(inboundLibraryStreams.subscription());
        }
        if (configuration.logOutboundMessages())
        {
            subscriptions.add(outboundLibraryStreams.subscription());
        }
        archiver = new Archiver(
            LoggerUtil.newArchiveMetaData(configuration.logFileDir()), logFileDir, loggerCacheCapacity, subscriptions);

        archiveReader = new ArchiveReader(
            LoggerUtil::mapExistingFile,
            LoggerUtil.newArchiveMetaData(configuration.logFileDir()),
            logFileDir,
            loggerCacheCapacity);
    }

    public Archiver archiver()
    {
        return archiver;
    }

    public ArchiveReader archiveReader()
    {
        return archiveReader;
    }

    public void start()
    {
        if (isLoggingMessages())
        {
            if (loggingRunner == null)
            {
                loggingRunner = newRunner(archiver);
            }

            startOnThread(loggingRunner);
        }
    }

    private boolean isLoggingMessages()
    {
        return configuration.logInboundMessages() || configuration.logOutboundMessages();
    }

    public void close()
    {
        if (loggingRunner != null)
        {
            loggingRunner.close();
        }
        else
        {
            archiver.onClose();
        }

        archiveReader.close();
    }
}
