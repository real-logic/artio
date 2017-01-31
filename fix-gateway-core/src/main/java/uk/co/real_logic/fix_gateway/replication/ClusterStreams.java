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
package uk.co.real_logic.fix_gateway.replication;

import io.aeron.Publication;
import uk.co.real_logic.fix_gateway.engine.logger.ArchiveReader;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Thread-safe constructor of clusterable streams
 */
class ClusterStreams extends ClusterableStreams
{
    private final RaftTransport transport;
    private final int ourSessionId;
    private final AtomicInteger leaderSessionId;
    private final Publication dataPublication;
    private final ArchiveReader archiveReader;

    ClusterStreams(
        final RaftTransport transport,
        final int ourSessionId,
        final AtomicInteger leaderSessionId,
        final Publication dataPublication,
        final ArchiveReader archiveReader)
    {
        this.transport = transport;
        this.ourSessionId = ourSessionId;
        this.leaderSessionId = leaderSessionId;
        this.dataPublication = dataPublication;
        this.archiveReader = archiveReader;
    }

    public boolean isLeader()
    {
        return isLeader(ourSessionId, leaderSessionId);
    }

    static boolean isLeader(final int ourSessionId, final AtomicInteger leaderSessionId)
    {
        return leaderSessionId.get() == ourSessionId;
    }

    public ClusterPublication publication(final int clusterStreamId)
    {
        return new ClusterPublication(dataPublication, leaderSessionId, ourSessionId, clusterStreamId);
    }

    public ClusterSubscription subscription(final int clusterStreamId)
    {
        return new ClusterSubscription(
            transport.dataSubscription(), clusterStreamId, transport.controlSubscription(), archiveReader);
    }
}
