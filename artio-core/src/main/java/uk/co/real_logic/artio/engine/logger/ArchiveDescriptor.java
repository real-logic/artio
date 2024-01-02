/*
 * Copyright 2015-2024 Real Logic Limited, Adaptive Financial Consulting Ltd.
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
package uk.co.real_logic.artio.engine.logger;

import org.agrona.BitUtil;

import static io.aeron.logbuffer.FrameDescriptor.FRAME_ALIGNMENT;

public final class ArchiveDescriptor
{
    public static int alignTerm(final int frameLength)
    {
        return BitUtil.align(frameLength, FRAME_ALIGNMENT);
    }

    public static long alignTerm(final long position)
    {
        return align(position, FRAME_ALIGNMENT);
    }

    private static long align(final long value, final long alignment)
    {
        return (value + (alignment - 1)) & -alignment;
    }
}
