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
import uk.co.real_logic.agrona.concurrent.QueuedPipe;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.LockSupport;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static uk.co.real_logic.fix_gateway.GatewayProcess.INBOUND_LIBRARY_STREAM;

public class LibraryActivationHandler implements NewImageHandler, InactiveImageHandler
{
    private static final long PAUSE = MICROSECONDS.toNanos(10);

    private final Set<Image> images = new HashSet<>();
    private final QueuedPipe<AdminCommand> commands;
    private final int firstLibrary;

    private int imageCount = 0;

    public LibraryActivationHandler(
        final boolean logInboundMessages, final QueuedPipe<AdminCommand> commands)
    {
        firstLibrary = logInboundMessages ? 1 : 0;
        this.commands = commands;
    }

    public void onInactiveImage(final Image image,
                                final String channel,
                                final int streamId,
                                final int sessionId,
                                final long position)
    {
        if (streamId == INBOUND_LIBRARY_STREAM && !images.contains(image))
        {
            put(new InactiveLibrary(sessionId));
        }
    }

    public void onNewImage(final Image image,
                           final String channel,
                           final int streamId,
                           final int sessionId,
                           final long joiningPosition,
                           final String sourceIdentity)
    {
        if (streamId == INBOUND_LIBRARY_STREAM)
        {
            if (imageCount >= firstLibrary)
            {
                put(new NewLibrary(sessionId));
            }
            else
            {
                images.add(image);
            }
            imageCount++;
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
}
