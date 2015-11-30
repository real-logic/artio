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

import uk.co.real_logic.aeron.CommonContext;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.aeron.driver.media.UdpChannel;

/**
 * .
 */
public final class StreamIdentifier
{
    private final int streamId;
    private final String channel;
    private final String canonicalForm;

    public StreamIdentifier(final Subscription subscription)
    {
        this(subscription.channel(), subscription.streamId());
    }

    public StreamIdentifier(final String channel, final int streamId)
    {
        this.streamId = streamId;
        this.channel = channel;
        if (CommonContext.IPC_CHANNEL.equals(channel))
        {
            canonicalForm = channel;
        }
        else
        {
            canonicalForm = UdpChannel.parse(channel).canonicalForm();
        }
    }

    public int streamId()
    {
        return streamId;
    }

    public String channel()
    {
        return channel;
    }

    public String canonicalForm()
    {
        return canonicalForm;
    }

    public boolean equals(final Object o)
    {
        if (this == o)
        {
            return true;
        }
        if (o == null || getClass() != o.getClass())
        {
            return false;
        }

        final StreamIdentifier that = (StreamIdentifier) o;

        return streamId == that.streamId && channel.equals(that.channel);
    }

    public int hashCode()
    {
        return 31 * streamId + channel.hashCode();
    }
}
