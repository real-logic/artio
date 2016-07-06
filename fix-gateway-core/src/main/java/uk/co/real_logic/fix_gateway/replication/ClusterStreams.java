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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Thread-safe constructor of clusterable streams
 */
public class ClusterStreams extends ClusterableStreams
{
    private final RaftTransport transport;
    private final int ourSessionId;
    private final AtomicInteger leaderSessionId;
    private final AtomicLong consensusPosition;
    private final Publication dataPublication;

    public ClusterStreams(
        final RaftTransport transport,
        final int ourSessionId,
        final AtomicInteger leaderSessionId,
        final AtomicLong consensusPosition,
        final Publication dataPublication)
    {
        this.transport = transport;
        this.ourSessionId = ourSessionId;
        this.leaderSessionId = leaderSessionId;
        this.consensusPosition = consensusPosition;
        this.dataPublication = dataPublication;
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

    public ClusterableSubscription subscription(final int clusterStreamId)
    {
        return new ClusterSubscription(
            transport.dataSubscription(), clusterStreamId, transport.controlSubscription());
    }
}
