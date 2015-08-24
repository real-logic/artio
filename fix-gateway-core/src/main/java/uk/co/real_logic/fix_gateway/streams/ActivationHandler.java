/*
 * Copyright 2014 Real Logic Ltd.
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

import uk.co.real_logic.aeron.Image;
import uk.co.real_logic.aeron.InactiveImageHandler;
import uk.co.real_logic.aeron.NewImageHandler;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.agrona.collections.Int2IntHashMap;
import uk.co.real_logic.agrona.concurrent.QueuedPipe;

import java.util.Objects;

public class ActivationHandler implements NewImageHandler, InactiveImageHandler
{
    private final Int2IntHashMap libraryIds = new Int2IntHashMap(0);
    private final QueuedPipe<Object> commands;
    private final String channel;
    private final int streamId;

    @SuppressWarnings("unchecked")
    public ActivationHandler(
        final QueuedPipe<?> commands,
        final String channel,
        final int streamId)
    {
        this.streamId = streamId;
        Objects.requireNonNull(commands, "commands");
        Objects.requireNonNull(channel, "channel");

        this.commands = (QueuedPipe<Object>) commands;
        this.channel = channel;
    }

    public void onInactiveImage(final Image image, final Subscription subscription, final long position)
    {
        if (isExpectedStream(subscription))
        {
            //System.out.println("Inactive: " + System.currentTimeMillis());
            final int libraryId = image.sessionId();
            final int count = libraryIds.get(libraryId) - 1;
            libraryIds.put(libraryId, count);
            if (count == 0)
            {
                //put(new InactiveProcess(libraryId));
            }
        }
    }

    public void onNewImage(final Image image,
                           final Subscription subscription,
                           final long joiningPosition,
                           final String sourceIdentity)
    {
        if (isExpectedStream(subscription))
        {
            final int libraryId = image.sessionId();
            final int count = libraryIds.get(libraryId) + 1;
            libraryIds.put(libraryId, count);
            //System.out.println("Active: " + System.currentTimeMillis() + " : " + libraryId + " @ " + count);
            if (count == 1)
            {
                //put(new NewProcess(libraryId));
            }
        }
    }

    private void put(final Object command)
    {
        while (!commands.offer(command))
        {
            Thread.yield();
            // TODO: consider monitoring this
            // TODO: consider backoff
        }
    }

    private boolean isExpectedStream(final Subscription subscription)
    {
        return subscription.streamId() == streamId
            && channel.equals(subscription.channel());
    }

}
