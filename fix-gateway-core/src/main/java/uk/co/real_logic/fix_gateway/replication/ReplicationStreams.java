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
import uk.co.real_logic.agrona.concurrent.AtomicCounter;
import uk.co.real_logic.fix_gateway.FixPublication;

public final class ReplicationStreams
{
    private static final int DATA_STREAM = 0;
    private static final int CONTROL_STREAM = 1;

    private final String channel;
    private final Aeron aeron;
    private final AtomicCounter failedDataPublications;

    public ReplicationStreams(final String channel, final Aeron aeron, final AtomicCounter failedDataPublications)
    {
        this.channel = channel;
        this.aeron = aeron;
        this.failedDataPublications = failedDataPublications;
    }

    public Publication dataPublication()
    {
        return aeron.addPublication(channel, DATA_STREAM);
    }

    public FixPublication fixPublication()
    {
        return new FixPublication(dataPublication(), failedDataPublications);
    }

    public Publication controlPublication()
    {
        return aeron.addPublication(channel, CONTROL_STREAM);
    }

    public Subscription dataSubscription(final DataHandler handler)
    {
        return aeron.addSubscription(channel, DATA_STREAM, handler);
    }

    public Subscription controlSubscription(final DataHandler handler)
    {
        return aeron.addSubscription(channel, CONTROL_STREAM, handler);
    }
}
