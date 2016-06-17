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
import io.aeron.logbuffer.BufferClaim;

/**
 * A Clustered Publication is a publication that support the raft protocol and allows
 * for publication of messages if you're the leader of the cluster.
 */
public class ClusterPublication extends ClusterablePublication
{
    private final Publication dataPublication;
    private final TermState termState;
    private final int ourSessionId;
    private final long reservedValue;
    private final int streamId;

    ClusterPublication(
        final Publication dataPublication,
        final TermState termState,
        final int ourSessionId,
        final int streamId)
    {
        this.dataPublication = dataPublication;
        this.termState = termState;
        this.ourSessionId = ourSessionId;
        this.reservedValue = ReservedValue.ofClusterStreamId(streamId);
        this.streamId = streamId;
    }

    public long tryClaim(final int length, final BufferClaim bufferClaim)
    {
        if (!ClusterStreams.isLeader(ourSessionId, termState))
        {
            return CANT_PUBLISH;
        }

        final long position = dataPublication.tryClaim(length, bufferClaim);
        if (position > 0)
        {
            bufferClaim.reservedValue(reservedValue);
        }
        return position;
    }

    public int id()
    {
        return streamId;
    }

    public void close()
    {
    }
}
