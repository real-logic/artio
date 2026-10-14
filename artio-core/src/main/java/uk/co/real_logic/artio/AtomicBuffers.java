/*
 * Copyright 2015-2026 Real Logic Limited.
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
package uk.co.real_logic.artio;

import org.agrona.concurrent.AtomicBuffer;

/**
 * Utility methods for AtomicBuffers.
 */
public final class AtomicBuffers
{
    private AtomicBuffers()
    {
    }

    /**
     * Check an {@link AtomicBuffer} is aligned and return it for a cleaner API. This will either log or throw based
     * on the configration of {@link AtomicBuffer#STRICT_ALIGNMENT_CHECKS_PROP_NAME}.
     *
     * @param atomicBuffer to be checked.
     * @return the checked buffer.
     */
    public static AtomicBuffer checkAligned(final AtomicBuffer atomicBuffer)
    {
        atomicBuffer.verifyAlignment();
        return atomicBuffer;
    }
}
