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

import io.aeron.ExclusivePublication;
import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import org.agrona.DirectBuffer;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.ABORT;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;

class OutboundPipe implements ClusterFragmentHandler
{
    private final ExclusivePublication publication;
    private final ClusterStreams streams;
    private final ClusterableSubscription subscription;

    OutboundPipe(final ExclusivePublication publication, final ClusterStreams streams)
    {
        this.publication = publication;
        this.streams = streams;
        this.subscription = publication != null ?
            streams.subscription(publication.streamId(), "outboundPipe") : null;
    }

    public int poll(final int fragmentLimit)
    {
        if (publication != null && streams.isLeader())
        {
            return subscription.poll(this, fragmentLimit);
        }

        return 0;
    }

    public Action onFragment(final DirectBuffer buffer, final int offset, final int length, final ClusterHeader header)
    {
        final long position = publication.offer(buffer, offset, length);
        if (position < 0)
        {
            return ABORT;
        }

        return CONTINUE;
    }
}
