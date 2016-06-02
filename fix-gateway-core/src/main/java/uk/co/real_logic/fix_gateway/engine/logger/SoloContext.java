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

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.logbuffer.BufferClaim;
import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.*;
import uk.co.real_logic.fix_gateway.FixCounters;
import uk.co.real_logic.fix_gateway.engine.EngineConfiguration;
import uk.co.real_logic.fix_gateway.protocol.GatewayPublication;
import uk.co.real_logic.fix_gateway.protocol.Streams;
import uk.co.real_logic.fix_gateway.replication.ClusterableNode;
import uk.co.real_logic.fix_gateway.replication.SoloNode;
import uk.co.real_logic.fix_gateway.replication.SoloPublication;
import uk.co.real_logic.fix_gateway.replication.StreamIdentifier;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static org.agrona.concurrent.AgentRunner.startOnThread;
import static uk.co.real_logic.fix_gateway.GatewayProcess.INBOUND_LIBRARY_STREAM;
import static uk.co.real_logic.fix_gateway.GatewayProcess.OUTBOUND_LIBRARY_STREAM;
import static uk.co.real_logic.fix_gateway.ReliefValve.NO_RELIEF_VALVE;

public class SoloContext extends Context
{
    private final EngineConfiguration configuration;
    private final Publication replayPublication;
    private final ErrorHandler errorHandler;
    private final SequenceNumberIndexWriter sentSequenceNumberIndex;
    private final SequenceNumberIndexWriter receivedSequenceNumberIndex;
    private final FixCounters fixCounters;
    private final List<Archiver> archivers = new ArrayList<>();
    private final StreamIdentifier inboundStreamId;
    private final StreamIdentifier outboundStreamId;

    private LogDirectoryDescriptor directoryDescriptor;
    private AgentRunner loggingRunner;
    private ArchiveReader outboundArchiveReader;
    private ArchiveReader inboundArchiveReader;
    private Archiver inboundArchiver;
    private Archiver outboundArchiver;
    private Aeron aeron;
    private SoloNode node;
    private Streams inboundLibraryStreams;
    private Streams outboundLibraryStreams;

    SoloContext(
        final EngineConfiguration configuration,
        final ErrorHandler errorHandler,
        final Publication replayPublication,
        final SequenceNumberIndexWriter sentSequenceNumberIndexWriter,
        final SequenceNumberIndexWriter receivedSequenceNumberIndex,
        final FixCounters fixCounters,
        final Aeron aeron)
    {
        this.configuration = configuration;
        this.replayPublication = replayPublication;
        this.errorHandler = errorHandler;
        this.sentSequenceNumberIndex = sentSequenceNumberIndexWriter;
        this.receivedSequenceNumberIndex = receivedSequenceNumberIndex;
        this.fixCounters = fixCounters;
        this.aeron = aeron;

        final String channel = configuration.libraryAeronChannel();
        this.inboundStreamId = new StreamIdentifier(channel, INBOUND_LIBRARY_STREAM);
        this.outboundStreamId = new StreamIdentifier(channel, OUTBOUND_LIBRARY_STREAM);
    }

    public SoloContext init()
    {
        initNode();
        initStreams();
        initArchival();
        initIndexers();

        return this;
    }

    private void initNode()
    {
        node = new SoloNode(aeron, configuration.libraryAeronChannel());
    }

    public void initIndexers()
    {
        if (configuration.logOutboundMessages())
        {
            final int cacheSetSize = configuration.loggerCacheSetSize();
            final int cacheNumSets = configuration.loggerCacheNumSets();
            final String logFileDir = configuration.logFileDir();
            final Indexer outboundIndexer = new Indexer(
                asList(
                    newReplayIndex(cacheSetSize, cacheNumSets, logFileDir, OUTBOUND_LIBRARY_STREAM),
                    sentSequenceNumberIndex),
                outboundArchiveReader);

            final Indexer inboundIndexer = new Indexer(
                asList(
                    newReplayIndex(cacheSetSize, cacheNumSets, logFileDir, INBOUND_LIBRARY_STREAM),
                    receivedSequenceNumberIndex),
                inboundArchiveReader);

            final ReplayQuery replayQuery =
                newReplayQuery(logFileDir, outboundArchiveReader);
            final Replayer replayer = new Replayer(
                replayQuery,
                replayPublication,
                new BufferClaim(),
                configuration.loggerIdleStrategy(),
                errorHandler,
                configuration.outboundMaxClaimAttempts());

            inboundArchiver.subscription(
                aeron.addSubscription(inboundStreamId.channel(), inboundStreamId.streamId()));
            outboundArchiver.subscription(
                aeron.addSubscription(outboundStreamId.channel(), outboundStreamId.streamId()));
            inboundIndexer.subscription(inboundLibraryStreams.subscription());
            outboundIndexer.subscription(outboundLibraryStreams.subscription());
            replayer.subscription(inboundLibraryStreams.subscription());

            final List<Agent> agents = new ArrayList<>(archivers);
            agents.add(outboundIndexer);
            agents.add(inboundIndexer);
            agents.add(replayer);

            final Agent loggingAgent = new CompositeAgent(agents);

            loggingRunner = newRunner(loggingAgent);
        }
        else
        {
            final GatewayPublication replayGatewayPublication =
                new GatewayPublication(
                    new SoloPublication(replayPublication),
                    fixCounters.failedReplayPublications(),
                    configuration.loggerIdleStrategy(),
                    new SystemNanoClock(),
                    configuration.outboundMaxClaimAttempts(),
                    NO_RELIEF_VALVE);
            final GapFiller gapFiller = new GapFiller(
                inboundLibraryStreams.subscription(),
                replayGatewayPublication);
            loggingRunner = newRunner(gapFiller);
        }
    }

    private ReplayIndex newReplayIndex(
        final int cacheSetSize,
        final int cacheNumSets,
        final String logFileDir,
        final int streamId)
    {
        return new ReplayIndex(
            logFileDir,
            streamId,
            configuration.indexFileSize(),
            cacheNumSets,
            cacheSetSize,
            LoggerUtil::map,
            ReplayIndex.replayBuffer(logFileDir, streamId),
            errorHandler);
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
            inboundArchiver = addArchiver(cacheNumSets, cacheSetSize, inboundStreamId);
            inboundArchiveReader = archiveReader(logFileDir, inboundStreamId);
        }

        if (configuration.logOutboundMessages())
        {
            outboundArchiver = addArchiver(cacheNumSets, cacheSetSize, outboundStreamId);
            outboundArchiveReader = archiveReader(logFileDir, outboundStreamId);
        }
    }

    private void initStreams()
    {
        final NanoClock nanoClock = new SystemNanoClock();
        inboundLibraryStreams = new Streams(
            node, fixCounters.failedInboundPublications(), INBOUND_LIBRARY_STREAM, nanoClock,
            configuration.inboundMaxClaimAttempts());
        outboundLibraryStreams = new Streams(
            node, fixCounters.failedOutboundPublications(), OUTBOUND_LIBRARY_STREAM, nanoClock,
            configuration.outboundMaxClaimAttempts());
    }

    private ArchiveReader archiveReader(final String logFileDir, final StreamIdentifier streamId)
    {
        return new ArchiveReader(
            LoggerUtil.newArchiveMetaData(logFileDir),
            configuration.loggerCacheNumSets(),
            configuration.loggerCacheSetSize(),
            streamId);
    }

    private AgentRunner newRunner(final Agent loggingAgent)
    {
        return new AgentRunner(configuration.loggerIdleStrategy(), errorHandler, null, loggingAgent);
    }

    private Archiver addArchiver(
        final int cacheNumSets,
        final int cacheSetSize,
        final StreamIdentifier streamId)
    {
        final Archiver archiver = new Archiver(
            LoggerUtil.newArchiveMetaData(configuration.logFileDir()),
            cacheNumSets,
            cacheSetSize,
            streamId);
        archivers.add(archiver);
        return archiver;
    }

    public Streams outboundLibraryStreams()
    {
        return outboundLibraryStreams;
    }

    public Streams inboundLibraryStreams()
    {
        return inboundLibraryStreams;
    }

    public ClusterableNode node()
    {
        return node;
    }

    public ReplayQuery inboundReplayQuery()
    {
        if (!configuration.logInboundMessages())
        {
            return null;
        }

        final String logFileDir = configuration.logFileDir();
        final ArchiveReader archiveReader =
            archiveReader(logFileDir, inboundStreamId);
        return newReplayQuery(logFileDir, archiveReader);
    }

    public List<Archiver> archivers()
    {
        return archivers;
    }

    public ArchiveReader outboundArchiveReader()
    {
        return outboundArchiveReader;
    }

    public ArchiveReader inboundArchiveReader()
    {
        return inboundArchiveReader;
    }

    public Archiver outboundArchiver()
    {
        return outboundArchiver;
    }

    public Archiver inboundArchiver()
    {
        return inboundArchiver;
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

        CloseHelper.close(inboundArchiveReader);
        CloseHelper.close(outboundArchiveReader);
        sentSequenceNumberIndex.close();
        receivedSequenceNumberIndex.close();
    }
}
