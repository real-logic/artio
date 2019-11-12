/*
 * Copyright 2015-2017 Real Logic Ltd.
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
import uk.co.real_logic.artio.DebugLogger;
import uk.co.real_logic.artio.LogTag;

class SlowPeeker extends BlockablePosition
{
    final Image normalImage;
    final Image peekImage;

    SlowPeeker(final Image peekImage, final Image normalImage)
    {
        this.peekImage = peekImage;
        this.normalImage = normalImage;
    }

    private int peekCounter = 0;

    int peek(final ControlledFragmentHandler handler)
    {
        blockPosition = DID_NOT_BLOCK;
        final long initialPosition = peekImage.position();
        final long normalImagePosition = normalImage.position();

        final long resultingPosition = peekImage.controlledPeek(
            initialPosition, handler, normalImagePosition);

        final long delta = resultingPosition - initialPosition;
        final long blockPosition = this.blockPosition;

        if ((peekCounter++ % 1000) == 0)
        {
            DebugLogger.log(LogTag.POSITION, "init=%d, norm=%d, res=%d, block=%d%n",
                initialPosition,
                normalImagePosition,
                resultingPosition,
                blockPosition);
        }

        if (!peekImage.isClosed())
        {
            if (blockPosition != DID_NOT_BLOCK)
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
