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
package uk.co.real_logic.fix_gateway.replication;

import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.DataHandler;
import uk.co.real_logic.aeron.exceptions.RegistrationException;
import uk.co.real_logic.agrona.concurrent.AtomicCounter;
import uk.co.real_logic.agrona.concurrent.BackoffIdleStrategy;

import java.util.ArrayList;
import java.util.List;

public class ReplicationStreams implements AutoCloseable
{
    private final List<Subscription> subscriptions = new ArrayList<>();

    private final int dataStream;
    private final int controlStream;

    private final String channel;
    private final Aeron aeron;
    private final AtomicCounter failedDataPublications;
    private final Publication dataPublication;

    public ReplicationStreams(
        final String channel,
        final Aeron aeron,
        final AtomicCounter failedDataPublications,
        final int dataStream,
        final int controlStream)
    {
        this.channel = channel;
        this.aeron = aeron;
        this.failedDataPublications = failedDataPublications;
        this.dataStream = dataStream;
        this.controlStream = controlStream;
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

    public Publication controlPublication()
    {
        return aeron.addPublication(channel, controlStream);
    }

    public Subscription dataSubscription(final DataHandler handler)
    {
        return addSubscription(dataStream, handler);
    }

    public Subscription controlSubscription(final DataHandler handler)
    {
        return addSubscription(controlStream, handler);
    }

    private Subscription addSubscription(final int stream, final DataHandler handler)
    {
        final Subscription subscription = aeron.addSubscription(channel, stream, handler);
        subscriptions.add(subscription);
        return subscription;
    }

    public void close()
    {
        try
        {
            dataPublication.close();
        }
        catch (RegistrationException e)
        {
            // Deliberately blank, TODO: remove once closing publications is idempotent.
        }
        subscriptions.forEach(Subscription::close);
    }
}
