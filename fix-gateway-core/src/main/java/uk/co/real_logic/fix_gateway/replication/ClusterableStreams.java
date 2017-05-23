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

import io.aeron.Aeron;

public abstract class ClusterableStreams
{

    public static SoloStreams solo(
        final Aeron aeron,
        final String aeronChannel,
        final boolean printAeronStreamIdentifiers)
    {
        return new SoloStreams(aeron, aeronChannel, printAeronStreamIdentifiers);
    }

    public abstract boolean isLeader();

    /**
     * Get the publication for this stream id, new object every time.
     *
     * @param clusterStreamId a unique identifier for the stream
     * @param name
     * @return the publication for this stream id.
     */
    public abstract ClusterablePublication publication(int clusterStreamId, String name);

    /**
     * Get the subscription for this stream id, new object every time.
     *
     * @param clusterStreamId a unique identifier for the stream
     * @param name
     * @return the subscription for this stream id.
     */
    public abstract ClusterableSubscription subscription(int clusterStreamId, String name);
}
