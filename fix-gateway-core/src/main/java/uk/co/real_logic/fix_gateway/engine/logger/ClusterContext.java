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
import uk.co.real_logic.fix_gateway.engine.EngineConfiguration;
import uk.co.real_logic.fix_gateway.protocol.Streams;
import uk.co.real_logic.fix_gateway.replication.ClusterNode;
import uk.co.real_logic.fix_gateway.replication.ClusterNodeConfiguration;
import uk.co.real_logic.fix_gateway.replication.ClusterableNode;

// TODO: finish cluster context
public class ClusterContext extends Context
{
    private final EngineConfiguration configuration;
    private final ErrorHandler errorHandler;
    private final Publication replayPublication;
    private final SequenceNumberIndexWriter sentSequenceNumberIndexWriter;
    private final SequenceNumberIndexWriter receivedSequenceNumberIndex;
    private final Aeron aeron;

    private ClusterNode node;

    public ClusterContext(
        final EngineConfiguration configuration,
        final ErrorHandler errorHandler,
        final Publication replayPublication,
        final SequenceNumberIndexWriter sentSequenceNumberIndexWriter,
        final SequenceNumberIndexWriter receivedSequenceNumberIndex,
        final Aeron aeron)
    {
        this.configuration = configuration;
        this.errorHandler = errorHandler;
        this.replayPublication = replayPublication;
        this.sentSequenceNumberIndexWriter = sentSequenceNumberIndexWriter;
        this.receivedSequenceNumberIndex = receivedSequenceNumberIndex;
        this.aeron = aeron;
    }

    public void init()
    {
        final ClusterNodeConfiguration clusterNodeConfiguration = new ClusterNodeConfiguration()
            .nodeId(configuration.nodeId())
            .otherNodes(configuration.otherNodes())
            .timeoutIntervalInMs(configuration.clusterTimeoutIntervalInMs())
            .idleStrategy(configuration.framerIdleStrategy())
            .archiver(null) // TODO
            .archiveReader(null)
            .failCounter(null)
            .maxClaimAttempts(configuration.inboundMaxClaimAttempts())
            .aeronChannel(configuration.clusterAeronChannel())
            .aeron(aeron);

        node = new ClusterNode(clusterNodeConfiguration, System.currentTimeMillis());
    }

    public ReplayQuery inboundReplayQuery()
    {
        return null;
    }

    public ClusterableNode node()
    {
        return null;
    }

    public void start()
    {

    }

    public void close()
    {

    }

    public Streams outboundLibraryStreams()
    {
        return null;
    }

    public Streams inboundLibraryStreams()
    {
        return null;
    }
}
