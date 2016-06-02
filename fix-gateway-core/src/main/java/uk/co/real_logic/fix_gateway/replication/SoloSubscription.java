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

import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.logbuffer.ControlledFragmentHandler;

import java.util.function.Consumer;

public class SoloSubscription extends ClusterableSubscription
{
    private PositionHandler handler;
    private final Consumer<Image> positionUpdater =
        image -> handler.onNewPosition(image.sessionId(), image.position());
    private final Subscription subscription;

    public SoloSubscription(final Subscription subscription)
    {
        this.subscription = subscription;
    }

    public int controlledPoll(final ControlledFragmentHandler fragmentHandler, final int fragmentLimit)
    {
        return subscription.controlledPoll(fragmentHandler, fragmentLimit);
    }

    public void close()
    {
        subscription.close();
    }

    public void forEachPosition(final PositionHandler handler)
    {
        this.handler = handler;
        subscription.forEachImage(positionUpdater);
    }
}
