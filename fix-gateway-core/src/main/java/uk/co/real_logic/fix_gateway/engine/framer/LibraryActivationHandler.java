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
package uk.co.real_logic.fix_gateway.engine.framer;

import uk.co.real_logic.aeron.Image;
import uk.co.real_logic.aeron.InactiveImageHandler;
import uk.co.real_logic.aeron.NewImageHandler;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.agrona.collections.Int2IntHashMap;
import uk.co.real_logic.agrona.concurrent.QueuedPipe;

import java.util.Objects;
import java.util.concurrent.locks.LockSupport;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static uk.co.real_logic.fix_gateway.GatewayProcess.OUTBOUND_LIBRARY_STREAM;

public class LibraryActivationHandler implements NewImageHandler, InactiveImageHandler
{
    private static final long PAUSE = MICROSECONDS.toNanos(10);

    private final Int2IntHashMap libraryIds = new Int2IntHashMap(0);
    private final QueuedPipe<AdminCommand> commands;
    private final String channel;

    public LibraryActivationHandler(final QueuedPipe<AdminCommand> commands, final String channel)
    {
        Objects.requireNonNull(commands, "commands");
        Objects.requireNonNull(channel, "channel");
        this.commands = commands;
        this.channel = channel;
    }

    public void onInactiveImage(final Image image, final Subscription subscription, final long position)
    {
        if (isOutbound(subscription))
        {
            final int libraryId = image.sessionId();
            final int count = libraryIds.get(libraryId) - 1;
            libraryIds.put(libraryId, count);
            if (count == 0)
            {
                put(new InactiveLibrary(libraryId));
            }
        }
    }

    public void onNewImage(final Image image,
                           final Subscription subscription,
                           final long joiningPosition,
                           final String sourceIdentity)
    {
        if (isOutbound(subscription))
        {
            final int libraryId = image.sessionId();
            final int count = libraryIds.get(libraryId);
            libraryIds.put(libraryId, count + 1);
            if (count == 0)
            {
                put(new NewLibrary(libraryId));
            }
        }
    }

    private void put(final AdminCommand command)
    {
        while (!commands.offer(command))
        {
            LockSupport.parkNanos(PAUSE);
            // TODO: consider monitoring this
        }
    }

    private boolean isOutbound(final Subscription subscription)
    {
        return subscription.streamId() == OUTBOUND_LIBRARY_STREAM
            && channel.equals(subscription.channel());
    }

}
