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
package uk.co.real_logic.artio.protocol;

import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.status.AtomicCounter;
import uk.co.real_logic.artio.Clock;
import uk.co.real_logic.artio.replication.ClusterablePublication;
import uk.co.real_logic.artio.replication.ClusterableStreams;
import uk.co.real_logic.artio.replication.ClusterableSubscription;

public final class Streams
{
    private final int streamId;
    private final Clock clock;
    private final ClusterableStreams node;
    private final AtomicCounter failedPublications;
    private final int maxClaimAttempts;

    public Streams(
        final ClusterableStreams node,
        final AtomicCounter failedPublications,
        final int streamId,
        final Clock clock,
        final int maxClaimAttempts)
    {
        this.node = node;
        this.failedPublications = failedPublications;
        this.streamId = streamId;
        this.clock = clock;
        this.maxClaimAttempts = maxClaimAttempts;
    }

    public GatewayPublication gatewayPublication(final IdleStrategy idleStrategy, final String name)
    {
        return new GatewayPublication(
            dataPublication(name),
            failedPublications,
            idleStrategy,
            clock,
            maxClaimAttempts
        );
    }

    private ClusterablePublication dataPublication(final String name)
    {
        return node.publication(streamId, name);
    }

    public ClusterableSubscription subscription(final String name)
    {
        return node.subscription(streamId, name);
    }
}
