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
package uk.co.real_logic.fix_gateway.engine;

import io.aeron.Aeron;
import io.aeron.Publication;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.CompositeAgent;
import uk.co.real_logic.fix_gateway.FixCounters;
import uk.co.real_logic.fix_gateway.ReliefValve;
import uk.co.real_logic.fix_gateway.engine.logger.*;
import uk.co.real_logic.fix_gateway.protocol.GatewayPublication;
import uk.co.real_logic.fix_gateway.protocol.Streams;
import uk.co.real_logic.fix_gateway.replication.*;

import static org.agrona.concurrent.AgentRunner.startOnThread;
import static uk.co.real_logic.fix_gateway.GatewayProcess.INBOUND_LIBRARY_STREAM;
import static uk.co.real_logic.fix_gateway.GatewayProcess.OUTBOUND_LIBRARY_STREAM;
import static uk.co.real_logic.fix_gateway.replication.ClusterNodeConfiguration.DEFAULT_DATA_STREAM_ID;
import static uk.co.real_logic.fix_gateway.replication.ReservedValue.NO_FILTER;

class ClusterContext extends EngineContext
{
    private final SoloSubscription outboundLibrarySubscription;
    private final Publication inboundPublication;
    private final StreamIdentifier dataStream;
    private final ClusterAgent node;

    ClusterContext(
        final EngineConfiguration configuration,
        final ErrorHandler errorHandler,
        final Publication replayPublication,
        final FixCounters fixCounters,
        final Aeron aeron,
        final EngineDescriptorStore engineDescriptorStore)
    {
        super(configuration, errorHandler, fixCounters, aeron);

        final String channel = configuration.clusterAeronChannel();
        dataStream = new StreamIdentifier(channel, DEFAULT_DATA_STREAM_ID);
        final String libraryAeronChannel = configuration.libraryAeronChannel();
        inboundPublication = aeron.addPublication(libraryAeronChannel, INBOUND_LIBRARY_STREAM);
        outboundLibrarySubscription = new SoloSubscription(
            aeron.addSubscription(libraryAeronChannel, OUTBOUND_LIBRARY_STREAM));
        node = node(configuration, fixCounters, aeron, channel, engineDescriptorStore);
        newStreams(node.clusterStreams());
        newIndexers(inboundArchiveReader(), outboundArchiveReader());
        final Replayer replayer = newReplayer(replayPublication, outboundArchiveReader());

        loggingRunner = newRunner(new CompositeAgent(inboundIndexer, outboundIndexer, node, replayer));
    }

    private ClusterAgent node(
        final EngineConfiguration configuration,
        final FixCounters fixCounters,
        final Aeron aeron,
        final String channel,
        final EngineDescriptorStore engineDescriptorStore)
    {
        final int cacheNumSets = configuration.loggerCacheNumSets();
        final int cacheSetSize = configuration.loggerCacheSetSize();
        final String logFileDir = configuration.logFileDir();

        final ArchiveReader dataArchiveReader = archiveReader(dataStream, NO_FILTER);
        final Archiver archiver = new Archiver(
            LoggerUtil.newArchiveMetaData(logFileDir), cacheNumSets, cacheSetSize, dataStream);

        final ClusterNodeConfiguration clusterNodeConfiguration = new ClusterNodeConfiguration()
            .nodeId(configuration.nodeId())
            .otherNodes(configuration.otherNodes())
            .timeoutIntervalInMs(configuration.clusterTimeoutIntervalInMs())
            .idleStrategy(configuration.framerIdleStrategy())
            .archiver(archiver)
            .archiveReader(dataArchiveReader)
            .failCounter(fixCounters.failedRaftPublications())
            .maxClaimAttempts(configuration.inboundMaxClaimAttempts())
            .copyTo(inboundPublication)
            .aeronChannel(channel)
            .aeron(aeron)
            .nodeState(EngineDescriptorFactory.make(configuration.libraryAeronChannel()))
            .nodeStateHandler(engineDescriptorStore);

        return new ClusterAgent(clusterNodeConfiguration, System.currentTimeMillis());
    }

    private ArchiveReader outboundArchiveReader()
    {
        return archiveReader(dataStream, OUTBOUND_LIBRARY_STREAM);
    }

    private ArchiveReader inboundArchiveReader()
    {
        return archiveReader(dataStream, INBOUND_LIBRARY_STREAM);
    }

    public ReplayQuery inboundReplayQuery()
    {
        return newReplayQuery(inboundArchiveReader());
    }

    public ClusterableStreams streams()
    {
        return node.clusterStreams();
    }

    public void start()
    {
        startOnThread(loggingRunner);
    }

    public GatewayPublication inboundLibraryPublication()
    {
        return new GatewayPublication(
            new SoloPublication(inboundPublication),
            fixCounters.failedInboundPublications(),
            configuration.framerIdleStrategy(),
            nanoClock,
            configuration.inboundMaxClaimAttempts(),
            ReliefValve.NO_RELIEF_VALVE);
    }

    public Streams outboundLibraryStreams()
    {
        return outboundLibraryStreams;
    }

    public Streams inboundLibraryStreams()
    {
        return inboundLibraryStreams;
    }

    public ClusterableSubscription outboundLibrarySubscription()
    {
        return outboundLibrarySubscription;
    }

    public ClusterableSubscription outboundClusterSubscription()
    {
        return outboundLibraryStreams().subscription();
    }

    public void close()
    {
        loggingRunner.close();
        super.close();
    }
}
