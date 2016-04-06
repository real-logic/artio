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
package uk.co.real_logic.fix_gateway.engine.logger;

import io.aeron.Publication;
import io.aeron.Subscription;
import io.aeron.logbuffer.BufferClaim;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.CompositeAgent;
import uk.co.real_logic.fix_gateway.engine.EngineConfiguration;
import uk.co.real_logic.fix_gateway.replication.StreamIdentifier;
import uk.co.real_logic.fix_gateway.protocol.Streams;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.agrona.concurrent.AgentRunner.startOnThread;

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
    private final SequenceNumberIndexWriter sentSequenceNumberIndex;
    private final SequenceNumberIndexWriter receivedSequenceNumberIndex;
    private final List<Archiver> archivers = new ArrayList<>();

    private LogDirectoryDescriptor directoryDescriptor;
    private AgentRunner loggingRunner;
    private ArchiveReader outboundArchiveReader;
    private ArchiveReader inboundArchiveReader;

    public Logger(
        final EngineConfiguration configuration,
        final Streams inboundLibraryStreams,
        final Streams outboundLibraryStreams,
        final ErrorHandler errorHandler,
        final Publication replayPublication,
        final SequenceNumberIndexWriter sentSequenceNumberIndexWriter,
        final SequenceNumberIndexWriter receivedSequenceNumberIndex)
    {
        this.configuration = configuration;
        this.inboundLibraryStreams = inboundLibraryStreams;
        this.outboundLibraryStreams = outboundLibraryStreams;
        this.replayPublication = replayPublication;
        this.errorHandler = errorHandler;
        this.sentSequenceNumberIndex = sentSequenceNumberIndexWriter;
        this.receivedSequenceNumberIndex = receivedSequenceNumberIndex;
    }

    public void init()
    {
        initArchival();
        initIndexers();
    }

    public void initIndexers()
    {
        if (configuration.logOutboundMessages())
        {
            final int cacheSetSize = configuration.loggerCacheSetSize();
            final int cacheNumSets = configuration.loggerCacheNumSets();
            final String logFileDir = configuration.logFileDir();

            final ReplayIndex replayIndex =
                new ReplayIndex(logFileDir, configuration.indexFileSize(), cacheNumSets, cacheSetSize, LoggerUtil::map);

            final Indexer outboundIndexer = new Indexer(
                Arrays.asList(replayIndex, sentSequenceNumberIndex),
                outboundLibraryStreams.subscription(),
                outboundArchiveReader);

            final Indexer inboundIndexer = new Indexer(
                Arrays.asList(replayIndex, receivedSequenceNumberIndex),
                inboundLibraryStreams.subscription(),
                inboundArchiveReader);

            final ReplayQuery replayQuery =
                newReplayQuery(logFileDir, outboundArchiveReader);
            final Replayer replayer = new Replayer(
                inboundLibraryStreams.subscription(),
                replayQuery,
                replayPublication,
                new BufferClaim(),
                configuration.loggerIdleStrategy(),
                errorHandler,
                configuration.outboundMaxClaimAttempts());

            final List<Agent> agents = new ArrayList<>(archivers);
            agents.add(outboundIndexer);
            agents.add(inboundIndexer);
            agents.add(replayer);

            final Agent loggingAgent = new CompositeAgent(agents);

            loggingRunner = newRunner(loggingAgent);
        }
        else
        {
            final GapFiller gapFiller = new GapFiller(
                inboundLibraryStreams.subscription(),
                outboundLibraryStreams.gatewayPublication(configuration.loggerIdleStrategy()));
            loggingRunner = newRunner(gapFiller);
        }
    }

    private ReplayQuery newReplayQuery(final String logFileDir,
                                       final ArchiveReader archiveReader)
    {
        final int cacheSetSize = configuration.loggerCacheSetSize();
        final int cacheNumSets = configuration.loggerCacheNumSets();
        final int streamId = archiveReader.fullStreamId().streamId();
        return new ReplayQuery(
            logFileDir,
            cacheNumSets,
            cacheSetSize,
            LoggerUtil::mapExistingFile,
            archiveReader,
            streamId);
    }

    public void initArchival()
    {
        final int cacheNumSets = configuration.loggerCacheNumSets();
        final int cacheSetSize = configuration.loggerCacheSetSize();
        final String logFileDir = configuration.logFileDir();

        directoryDescriptor = new LogDirectoryDescriptor(logFileDir);

        if (configuration.logInboundMessages())
        {
            final Subscription inboundSubscription = inboundLibraryStreams.subscription();
            addArchiver(cacheNumSets, cacheSetSize, inboundSubscription);
            inboundArchiveReader = archiveReader(logFileDir, inboundSubscription);
        }

        if (configuration.logOutboundMessages())
        {
            final Subscription outboundSubscription = outboundLibraryStreams.subscription();
            addArchiver(cacheNumSets, cacheSetSize, outboundSubscription);
            outboundArchiveReader = archiveReader(logFileDir, outboundSubscription);
        }
    }

    private ArchiveReader archiveReader(final String logFileDir, final Subscription subscription)
    {
        return new ArchiveReader(
            LoggerUtil.newArchiveMetaData(logFileDir),
            configuration.loggerCacheNumSets(),
            configuration.loggerCacheSetSize(),
            new StreamIdentifier(subscription));
    }

    private AgentRunner newRunner(final Agent loggingAgent)
    {
        return new AgentRunner(configuration.loggerIdleStrategy(), errorHandler, null, loggingAgent);
    }

    private void addArchiver(final int cacheNumSets,
                             final int cacheSetSize,
                             final Subscription subscription)
    {
        final Archiver archiver = new Archiver(
            LoggerUtil.newArchiveMetaData(configuration.logFileDir()),
            cacheNumSets,
            cacheSetSize,
            new StreamIdentifier(subscription))
            .subscription(subscription);
        archivers.add(archiver);
    }

    public List<Archiver> archivers()
    {
        return archivers;
    }

    public ArchiveReader outboundArchiveReader()
    {
        return outboundArchiveReader;
    }

    public LogDirectoryDescriptor directoryDescriptor()
    {
        return directoryDescriptor;
    }

    public void start()
    {
        if (loggingRunner == null)
        {
            loggingRunner = newRunner(new CompositeAgent(archivers));
        }

        startOnThread(loggingRunner);
    }

    public void close()
    {
        if (loggingRunner != null)
        {
            loggingRunner.close();
        }
        else
        {
            archivers.forEach(Archiver::onClose);
        }

        outboundArchiveReader.close();
        sentSequenceNumberIndex.close();
        receivedSequenceNumberIndex.close();
    }

    public ReplayQuery inboundMessageQuery()
    {
        final String logFileDir = configuration.logFileDir();
        final ArchiveReader archiveReader = archiveReader(logFileDir, inboundLibraryStreams.subscription());
        return newReplayQuery(logFileDir, archiveReader);
    }
}
