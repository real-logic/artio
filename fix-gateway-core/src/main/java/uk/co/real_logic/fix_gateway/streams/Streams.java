/*
 * Copyright 2015 Real Logic Ltd.
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
package uk.co.real_logic.fix_gateway.streams;

import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.agrona.concurrent.AtomicCounter;
import uk.co.real_logic.agrona.concurrent.BackoffIdleStrategy;

public class Streams
{
    private final int streamId;

    private final String channel;
    private final Aeron aeron;
    private final AtomicCounter failedPublications;

    public Streams(
        final String channel,
        final Aeron aeron,
        final AtomicCounter failedPublications,
        final int streamId)
    {
        this.channel = channel;
        this.aeron = aeron;
        this.failedPublications = failedPublications;
        this.streamId = streamId;
    }

    public GatewayPublication gatewayPublication()
    {
        return new GatewayPublication(dataPublication(), failedPublications, new BackoffIdleStrategy(1, 1, 1, 1 << 20));
    }

    public Publication dataPublication()
    {
        return aeron.addPublication(channel, streamId);
    }

    public Subscription dataSubscription()
    {
        return aeron.addSubscription(channel, streamId);
    }

}
