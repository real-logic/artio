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
import org.agrona.ErrorHandler;
import org.agrona.concurrent.CompositeAgent;
import uk.co.real_logic.fix_gateway.FixCounters;
import uk.co.real_logic.fix_gateway.engine.EngineConfiguration;
import uk.co.real_logic.fix_gateway.protocol.Streams;
import uk.co.real_logic.fix_gateway.replication.ClusterNode;
import uk.co.real_logic.fix_gateway.replication.ClusterNodeConfiguration;
import uk.co.real_logic.fix_gateway.replication.ClusterableNode;
import uk.co.real_logic.fix_gateway.replication.StreamIdentifier;

import static java.util.Arrays.asList;
import static org.agrona.concurrent.AgentRunner.startOnThread;
import static uk.co.real_logic.fix_gateway.GatewayProcess.INBOUND_LIBRARY_STREAM;
import static uk.co.real_logic.fix_gateway.replication.ClusterNodeConfiguration.DEFAULT_DATA_STREAM_ID;

public class ClusterContext extends EngineContext
{
    private final StreamIdentifier dataStream;
    private final ClusterNode node;

    public ClusterContext(
        final EngineConfiguration configuration,
        final ErrorHandler errorHandler,
        final Publication replayPublication,
        final FixCounters fixCounters,
        final Aeron aeron)
    {
        super(configuration, errorHandler, fixCounters, aeron);

        final String channel = configuration.clusterAeronChannel();
        dataStream = new StreamIdentifier(channel, DEFAULT_DATA_STREAM_ID);

        final int cacheNumSets = configuration.loggerCacheNumSets();
        final int cacheSetSize = configuration.loggerCacheSetSize();
        final String logFileDir = configuration.logFileDir();

        final ArchiveReader archiveReader = archiveReader(dataStream);
        final Archiver archiver = new Archiver(
            LoggerUtil.newArchiveMetaData(logFileDir), cacheNumSets, cacheSetSize, dataStream);

        final ClusterNodeConfiguration clusterNodeConfiguration = new ClusterNodeConfiguration()
            .nodeId(configuration.nodeId())
            .otherNodes(configuration.otherNodes())
            .timeoutIntervalInMs(configuration.clusterTimeoutIntervalInMs())
            .idleStrategy(configuration.framerIdleStrategy())
            .archiver(archiver)
            .archiveReader(archiveReader)
            .failCounter(fixCounters.failedRaftPublications())
            .maxClaimAttempts(configuration.inboundMaxClaimAttempts())
            .aeronChannel(channel)
            .aeron(aeron);

        node = new ClusterNode(clusterNodeConfiguration, System.currentTimeMillis());

        initStreams(node);

        final Indexer indexer = new Indexer(
            asList(
                newReplayIndex(cacheSetSize, cacheNumSets, logFileDir, INBOUND_LIBRARY_STREAM),
                // TODO: distinguish between indices
                sentSequenceNumberIndex,
                receivedSequenceNumberIndex),
            archiveReader);

        final ReplayQuery replayQuery =
            newReplayQuery(archiveReader /*outboundArchiveReader*/);
        final Replayer replayer = new Replayer(
            replayQuery,
            replayPublication,
            new BufferClaim(),
            configuration.loggerIdleStrategy(),
            errorHandler,
            configuration.outboundMaxClaimAttempts());

        indexer.subscription(inboundLibraryStreams.subscription());
        replayer.subscription(inboundLibraryStreams.subscription());

        loggingRunner = newRunner(new CompositeAgent(indexer, replayer));
    }

    public ReplayQuery inboundReplayQuery()
    {
        final ArchiveReader archiveReader = archiveReader(dataStream); // TODO: inbound
        return newReplayQuery(archiveReader);
    }

    public ClusterableNode node()
    {
        return node;
    }

    public void start()
    {
        startOnThread(loggingRunner);
    }

    public Streams outboundLibraryStreams()
    {
        return outboundLibraryStreams;
    }

    public Streams inboundLibraryStreams()
    {
        return inboundLibraryStreams;
    }

    public void close()
    {
        loggingRunner.close();
        super.close();
    }
}
