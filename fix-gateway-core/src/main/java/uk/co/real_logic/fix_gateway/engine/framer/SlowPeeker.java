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

class SlowPeeker extends BlockablePosition
{
    final Image normalImage;
    final Image peekImage;

    SlowPeeker(final Image peekImage, final Image normalImage)
    {
        this.peekImage = peekImage;
        this.normalImage = normalImage;
    }

    int peek(final ControlledFragmentHandler handler)
    {
        blockPosition = DID_NOT_BLOCK;
        final long limitPosition = normalImage.position();
        final long initialPosition = peekImage.position();
        final long resultingPosition = peekImage.controlledPeek(
                initialPosition, handler, limitPosition);
        final long delta = resultingPosition - initialPosition;
        peekImage.position(blockPosition != DID_NOT_BLOCK ? blockPosition : resultingPosition);

        return (int) delta;
    }
}
