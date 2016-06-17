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

/**
 * Thread-safe constructor of clusterable streams
 */
public class ClusterStreams extends ClusterableStreams
{
    private final RaftTransport transport;
    private final int ourSessionId;
    private final Publication dataPublication;
    private final TermState termState;

    public ClusterStreams(
        final RaftTransport transport,
        final int ourSessionId,
        final Publication dataPublication,
        final TermState termState)
    {
        this.transport = transport;
        this.ourSessionId = ourSessionId;
        this.dataPublication = dataPublication;
        this.termState = termState;
    }

    public boolean isLeader()
    {
        return isLeader(ourSessionId, termState);
    }

    static boolean isLeader(final int ourSessionId, final TermState termState)
    {
        return termState.leaderPosition().sessionId() == ourSessionId;
    }

    public ClusterPublication publication(final int clusterStreamId)
    {
        return new ClusterPublication(dataPublication, termState, ourSessionId, clusterStreamId);
    }

    public ClusterableSubscription subscription(final int clusterStreamId)
    {
        return new ClusterSubscription(
            transport.dataSubscription(), clusterStreamId, termState);
    }
}
