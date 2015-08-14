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

import java.util.ArrayList;
import java.util.List;

public class ReplicatedStream implements AutoCloseable
{
    private final List<Subscription> subscriptions = new ArrayList<>();

    private final int dataStream;

    private final String channel;
    private final Aeron aeron;
    private final AtomicCounter failedDataPublications;
    private final Publication dataPublication;

    public ReplicatedStream(
        final String channel,
        final Aeron aeron,
        final AtomicCounter failedDataPublications,
        final int dataStream)
    {
        this.channel = channel;
        this.aeron = aeron;
        this.failedDataPublications = failedDataPublications;
        this.dataStream = dataStream;
        dataPublication = aeron.addPublication(channel, dataStream);
    }

    public GatewayPublication gatewayPublication()
    {
        return new GatewayPublication(dataPublication, failedDataPublications, new BackoffIdleStrategy(1, 1, 1, 1 << 20));
    }

    public Publication dataPublication()
    {
        return dataPublication;
    }

    public Subscription dataSubscription()
    {
        return addSubscription(dataStream);
    }

    private Subscription addSubscription(final int stream)
    {
        final Subscription subscription = aeron.addSubscription(channel, stream);
        subscriptions.add(subscription);
        return subscription;
    }

    public void close()
    {
        dataPublication.close();
        subscriptions.forEach(Subscription::close);
    }
}
