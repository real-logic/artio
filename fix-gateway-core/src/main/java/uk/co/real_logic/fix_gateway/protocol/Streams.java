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
package uk.co.real_logic.fix_gateway.protocol;

import io.aeron.Aeron;
import io.aeron.Publication;
import io.aeron.Subscription;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.status.AtomicCounter;
import uk.co.real_logic.fix_gateway.ReliefValve;

import static uk.co.real_logic.fix_gateway.ReliefValve.NO_RELIEF_VALVE;

public class Streams
{
    public static final int UNKNOWN_TEMPLATE = -1;

    private final int streamId;
    private final NanoClock nanoClock;
    private final String channel;
    private final Aeron aeron;
    private final AtomicCounter failedPublications;
    private final int maxClaimAttempts;

    public Streams(
        final String channel,
        final Aeron aeron,
        final AtomicCounter failedPublications,
        final int streamId,
        final NanoClock nanoClock,
        final int maxClaimAttempts)
    {
        this.channel = channel;
        this.aeron = aeron;
        this.failedPublications = failedPublications;
        this.streamId = streamId;
        this.nanoClock = nanoClock;
        this.maxClaimAttempts = maxClaimAttempts;
    }

    public GatewayPublication gatewayPublication(final IdleStrategy idleStrategy)
    {
        return gatewayPublication(idleStrategy, NO_RELIEF_VALVE);
    }

    public GatewayPublication gatewayPublication(final IdleStrategy idleStrategy, final ReliefValve reliefValve)
    {
        return new GatewayPublication(
            dataPublication(),
            failedPublications,
            idleStrategy,
            nanoClock,
            maxClaimAttempts,
            reliefValve);
    }

    public Publication dataPublication()
    {
        return aeron.addPublication(channel, streamId);
    }

    public Subscription subscription()
    {
        return aeron.addSubscription(channel, streamId);
    }
}
