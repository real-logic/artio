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
import io.aeron.logbuffer.ControlledFragmentHandler;

class SlowPeeker
{
    private static final int DID_NOT_BLOCK = 0;

    final Image imageToPoll;

    private long blockPosition;

    SlowPeeker(final Image imageToPoll)
    {
        this.imageToPoll = imageToPoll;
    }

    int peek(
        final ControlledFragmentHandler handler,
        final int fragmentLimit)
    {
        blockPosition = DID_NOT_BLOCK;
        final long oldPosition = imageToPoll.position();
        final long endPosition = imageToPoll.controlledPeek(oldPosition, handler, fragmentLimit);
        final long delta = endPosition - oldPosition;
        imageToPoll.position(blockPosition != DID_NOT_BLOCK ? blockPosition : endPosition);

        return (int) delta;
    }

    void blockPosition(final long blockPosition)
    {
        // Pick the lowest (ie first) position to block at
        if (this.blockPosition == DID_NOT_BLOCK)
        {
            this.blockPosition = blockPosition;
        }
    }
}
