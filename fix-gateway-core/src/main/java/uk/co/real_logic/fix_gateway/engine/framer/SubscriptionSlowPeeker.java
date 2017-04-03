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
package uk.co.real_logic.fix_gateway.engine.framer;

import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.logbuffer.ControlledFragmentHandler;
import org.agrona.collections.Int2ObjectHashMap;

import java.util.Iterator;
import java.util.function.IntFunction;

class SubscriptionSlowPeeker
{
    private final Subscription subscription;
    private final Int2ObjectHashMap<LibrarySlowPeeker> sessionIdToImagePeeker = new Int2ObjectHashMap<>();
    private final IntFunction<LibrarySlowPeeker> newLibraryPeeker = this::newLibraryPeeker;

    SubscriptionSlowPeeker(final Subscription subscription)
    {
        this.subscription = subscription;
    }

    int peek(final ControlledFragmentHandler handler, final int fragmentLimit)
    {
        final Iterator<LibrarySlowPeeker> it = sessionIdToImagePeeker.values().iterator();
        int fragmentsRead = 0;
        while (it.hasNext() && fragmentsRead < fragmentLimit)
        {
            final SlowPeeker imagePeeker = it.next();
            fragmentsRead += imagePeeker.peek(handler, fragmentLimit - fragmentsRead);
        }
        return fragmentsRead;
    }

    LibrarySlowPeeker addLibrary(final int aeronSessionId)
    {
        final LibrarySlowPeeker imagePeeker = sessionIdToImagePeeker.computeIfAbsent(aeronSessionId, newLibraryPeeker);
        imagePeeker.addLibrary();
        return imagePeeker;
    }

    private LibrarySlowPeeker newLibraryPeeker(final int aeronSessionId)
    {
        return new LibrarySlowPeeker(subscription.imageBySessionId(aeronSessionId));
    }

    class LibrarySlowPeeker extends SlowPeeker
    {
        private int libraries;

        LibrarySlowPeeker(final Image image)
        {
            super(image);
        }

        void addLibrary()
        {
            libraries++;
        }

        void removeLibrary()
        {
            libraries--;
            if (libraries == 0)
            {
                sessionIdToImagePeeker.remove(image.sessionId());
            }
        }
    }

}
