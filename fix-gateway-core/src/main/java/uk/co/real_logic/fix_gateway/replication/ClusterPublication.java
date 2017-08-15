/*
 * Copyright 2015-2017 Real Logic Ltd.
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

import io.aeron.ExclusivePublication;
import io.aeron.logbuffer.ExclusiveBufferClaim;

import java.util.concurrent.atomic.AtomicInteger;

import static uk.co.real_logic.fix_gateway.replication.PositionTranslations.transportToReplicated;

/**
 * A Clustered Publication is a publication that support the raft protocol and allows
 * for publication of messages if you're the leader of the cluster.
 */
class ClusterPublication extends ClusterablePublication
{
    private final ExclusivePublication dataPublication;
    private final TermState termState;
    private final AtomicInteger leaderSessionId;
    private final int ourSessionId;
    private final long reservedValue;
    private final int streamId;

    ClusterPublication(
        final ExclusivePublication dataPublication,
        final TermState termState,
        final AtomicInteger leaderSessionId,
        final int ourSessionId,
        final int streamId)
    {
        this.dataPublication = dataPublication;
        this.termState = termState;
        this.leaderSessionId = leaderSessionId;
        this.ourSessionId = ourSessionId;
        this.reservedValue = ReservedValue.ofClusterStreamId(streamId);
        this.streamId = streamId;
    }

    public long tryClaim(final int length, final ExclusiveBufferClaim bufferClaim)
    {
        if (!ClusterStreams.isLeader(ourSessionId, leaderSessionId))
        {
            return CANT_PUBLISH;
        }

        final long transportPosition = dataPublication.tryClaim(length, bufferClaim);
        if (transportPosition > 0)
        {
            bufferClaim.reservedValue(reservedValue);

            return transportToReplicated(transportPosition, termState.transportPositionDelta());
        }
        return transportPosition;
    }

    public int id()
    {
        return streamId;
    }

    /**
     *
     * @return the replicated position
     */
    public long position()
    {
        return transportToReplicated(dataPublication.position(), termState.transportPositionDelta());
    }

    public int maxPayloadLength()
    {
        return dataPublication.maxPayloadLength();
    }

    public void close()
    {
    }
}
