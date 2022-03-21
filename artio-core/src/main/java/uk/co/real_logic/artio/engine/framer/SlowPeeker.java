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
import io.aeron.logbuffer.ControlledFragmentHandler;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.ErrorHandler;

class SlowPeeker extends BlockablePosition
{
    final Image normalImage;
    final Image peekImage;

    SlowPeeker(final Image peekImage, final Image normalImage, final ErrorHandler errorHandler)
    {
        super(peekImage.mtuLength() - DataHeaderFlyweight.HEADER_LENGTH, errorHandler);
        this.peekImage = peekImage;
        this.normalImage = normalImage;
    }

    int peek(final ControlledFragmentHandler handler)
    {
        final long initialPosition = peekImage.position();
        final long normalImagePosition = normalImage.position();

        startPeek(initialPosition, normalImagePosition);

        final long resultingPosition = peekImage.controlledPeek(
            initialPosition, handler, normalImagePosition);

        final long delta = resultingPosition - initialPosition;
        if (!peekImage.isClosed())
        {
            final long blockPosition = blockPosition();

            if (blockPosition != DID_NOT_BLOCK) // lgtm [java/constant-comparison]
            {
                peekImage.position(blockPosition);
            }
            else
            {
                peekImage.position(resultingPosition);
            }

            return (int)delta;
        }
        else
        {
            return 0;
        }
    }
}
