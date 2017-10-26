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
package uk.co.real_logic.artio.replication;

import io.aeron.Aeron;
import io.aeron.ExclusivePublication;
import io.aeron.Subscription;
import uk.co.real_logic.artio.StreamInformation;

class SoloStreams extends ClusterableStreams
{
    private final Aeron aeron;
    private final String aeronChannel;
    private final boolean printAeronStreamIdentifiers;

    SoloStreams(final Aeron aeron, final String aeronChannel, final boolean printAeronStreamIdentifiers)
    {
        this.aeron = aeron;
        this.aeronChannel = aeronChannel;
        this.printAeronStreamIdentifiers = printAeronStreamIdentifiers;
    }

    public boolean isLeader()
    {
        return true;
    }

    public SoloPublication publication(final int clusterStreamId, final String name)
    {
        final ExclusivePublication publication = aeron.addExclusivePublication(aeronChannel, clusterStreamId);
        StreamInformation.print(name, publication, printAeronStreamIdentifiers);
        return ClusterablePublication.solo(publication);
    }

    public SoloSubscription subscription(final int clusterStreamId, final String name)
    {
        final Subscription subscription = aeron.addSubscription(aeronChannel, clusterStreamId);
        StreamInformation.print(name, subscription, printAeronStreamIdentifiers);
        return new SoloSubscription(subscription);
    }
}
