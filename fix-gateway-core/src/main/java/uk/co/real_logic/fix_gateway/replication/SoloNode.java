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

import io.aeron.Aeron;

public class SoloNode extends ClusterableNode
{
    private final Aeron aeron;
    private final String aeronChannel;

    public SoloNode(final Aeron aeron, final String aeronChannel)
    {
        this.aeron = aeron;
        this.aeronChannel = aeronChannel;
    }

    public boolean isLeader()
    {
        return true;
    }

    public boolean isPublishable()
    {
        return true;
    }

    public SoloPublication publication(final int streamId)
    {
        return new SoloPublication(aeron.addPublication(aeronChannel, streamId));
    }

    public SoloSubscription subscription(final int streamId)
    {
        return new SoloSubscription(aeron.addSubscription(aeronChannel, streamId));
    }

    public int poll(final int fragmentLimit, final long timeInMs)
    {
        return 0;
    }
}
