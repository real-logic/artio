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

import io.aeron.Subscription;
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.DirectBuffer;

/**
 * NB: left exposed as a public class because it the additional functionality over
 * {@link ClusterableSubscription} of being able to lookup the position of a session.
 */
class SoloSubscription extends ClusterableSubscription implements ControlledFragmentHandler
{
    private final Subscription subscription;
    private final ClusterHeader clusterHeader;

    private ClusterFragmentHandler fragmentHandler;

    SoloSubscription(final Subscription subscription)
    {
        this.subscription = subscription;
        clusterHeader = new ClusterHeader(subscription.streamId());
    }

    public int poll(final ClusterFragmentHandler fragmentHandler, final int fragmentLimit)
    {
        this.fragmentHandler = fragmentHandler;
        return subscription.controlledPoll(this, fragmentLimit);
    }

    public void close()
    {
        subscription.close();
    }

    public Action onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
    {
        clusterHeader.update(header.position(), header.sessionId(), header.flags());
        return fragmentHandler.onFragment(buffer, offset, length, clusterHeader);
    }
}
