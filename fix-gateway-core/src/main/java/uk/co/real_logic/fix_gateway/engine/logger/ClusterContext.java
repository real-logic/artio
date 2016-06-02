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
import org.agrona.ErrorHandler;
import uk.co.real_logic.fix_gateway.FixCounters;
import uk.co.real_logic.fix_gateway.engine.EngineConfiguration;
import uk.co.real_logic.fix_gateway.protocol.Streams;
import uk.co.real_logic.fix_gateway.replication.ClusterNode;
import uk.co.real_logic.fix_gateway.replication.ClusterNodeConfiguration;
import uk.co.real_logic.fix_gateway.replication.ClusterableNode;
import uk.co.real_logic.fix_gateway.replication.StreamIdentifier;

import static uk.co.real_logic.fix_gateway.replication.ClusterNodeConfiguration.DEFAULT_DATA_STREAM_ID;

// TODO: finish cluster context
public class ClusterContext extends EngineContext
{
    private ClusterNode node;

    public ClusterContext(
        final EngineConfiguration configuration,
        final ErrorHandler errorHandler,
        final Publication replayPublication, // TODO: use
        final FixCounters fixCounters,
        final Aeron aeron)
    {
        super(configuration, errorHandler, fixCounters, aeron);

        final String channel = configuration.clusterAeronChannel();
        final StreamIdentifier dataStream = new StreamIdentifier(channel, DEFAULT_DATA_STREAM_ID);
        final int cacheNumSets = configuration.loggerCacheNumSets();
        final int cacheSetSize = configuration.loggerCacheSetSize();
        final ArchiveReader archiveReader = new ArchiveReader(
            LoggerUtil.newArchiveMetaData(configuration.logFileDir()), cacheNumSets, cacheSetSize, dataStream);
        final Archiver archiver = new Archiver(
            LoggerUtil.newArchiveMetaData(configuration.logFileDir()), cacheNumSets, cacheSetSize, dataStream);

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
    }

    public ReplayQuery inboundReplayQuery()
    {
        return null;
    }

    public ClusterableNode node()
    {
        return node;
    }

    public void start()
    {

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

    }
}
