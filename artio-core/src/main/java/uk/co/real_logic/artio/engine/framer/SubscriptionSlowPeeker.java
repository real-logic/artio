/*
 * Copyright 2015-2022 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.co.real_logic.artio.engine.framer;

import io.aeron.Image;
import io.aeron.Subscription;
import io.aeron.logbuffer.ControlledFragmentHandler;
import org.agrona.ErrorHandler;
import org.agrona.collections.Int2ObjectHashMap;

import java.util.function.IntFunction;

class SubscriptionSlowPeeker
{
    private final Subscription peekSubscription;
    private final Subscription normalSubscription;
    private final Int2ObjectHashMap<LibrarySlowPeeker> sessionIdToImagePeeker = new Int2ObjectHashMap<>();
    private final IntFunction<LibrarySlowPeeker> newLibraryPeeker = this::newLibraryPeeker;
    private final ErrorHandler errorHandler;
    private int senderMaxBytesInBuffer;

    SubscriptionSlowPeeker(
        final Subscription peekSubscription,
        final Subscription normalSubscription,
        final ErrorHandler errorHandler,
        final int senderMaxBytesInBuffer)
    {
        this.peekSubscription = peekSubscription;
        this.normalSubscription = normalSubscription;
        this.errorHandler = errorHandler;
        this.senderMaxBytesInBuffer = senderMaxBytesInBuffer;
    }

    int peek(final ControlledFragmentHandler handler)
    {
        int bytesRead = 0;
        for (final LibrarySlowPeeker slowPeeker : sessionIdToImagePeeker.values())
        {
            bytesRead += slowPeeker.peek(handler);
        }
        return bytesRead;
    }

    LibrarySlowPeeker addLibrary(final int aeronSessionId)
    {
        final LibrarySlowPeeker imagePeeker = sessionIdToImagePeeker.computeIfAbsent(aeronSessionId, newLibraryPeeker);
        if (imagePeeker == null)
        {
            return null;
        }

        imagePeeker.addLibrary();
        return imagePeeker;
    }

    private LibrarySlowPeeker newLibraryPeeker(final int aeronSessionId)
    {
        final Image peekImage = peekSubscription.imageBySessionId(aeronSessionId);
        final Image normalImage = normalSubscription.imageBySessionId(aeronSessionId);

        if (peekImage == null || normalImage == null)
        {
            return null;
        }

        FramerContext.validateMaxBytesInBuffer(normalImage.termBufferLength(), errorHandler, senderMaxBytesInBuffer);

        return new LibrarySlowPeeker(peekImage, normalImage, errorHandler);
    }

    class LibrarySlowPeeker extends SlowPeeker
    {
        private int libraries;

        LibrarySlowPeeker(final Image peekImage, final Image normalImage, final ErrorHandler errorHandler)
        {
            super(peekImage, normalImage, errorHandler);
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
                sessionIdToImagePeeker.remove(peekImage.sessionId());
            }
        }

        int peek(final ControlledFragmentHandler handler)
        {
            try
            {
                return super.peek(handler);
            }
            catch (final IllegalStateException e)
            {
                if (peekImage.isClosed() || normalImage.isClosed())
                {
                    removeLibrary();
                    return 1;
                }
                else
                {
                    throw e;
                }
            }
        }
    }

}
