/*
 * Copyright 2015-2023 Real Logic Limited.
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
package uk.co.real_logic.artio.protocol;

import io.aeron.logbuffer.Header;

/**
 * 8 byte reserved word is used with the
 */
public final class ReservedValue
{
    public static final int NO_FILTER = 0;

    private static final int BITS_IN_INT = 32;

    public static long ofClusterStreamId(final int clusterStreamId)
    {
        return clusterStreamId & 0xFFFFFFFFL;
    }

    public static long ofChecksum(final int checksum)
    {
        return ((long)checksum) << BITS_IN_INT;
    }

    public static long of(final int clusterStreamId, final int checksum)
    {
        return ofChecksum(checksum) | ofClusterStreamId(clusterStreamId);
    }

    public static int clusterStreamId(final long reservedValue)
    {
        return (int)reservedValue;
    }

    public static int clusterStreamId(final Header header)
    {
        final long reservedValue = header.reservedValue();
        return clusterStreamId(reservedValue);
    }

    public static int streamId(final Header header)
    {
        final int clusterStreamId = clusterStreamId(header);
        if (clusterStreamId == NO_FILTER)
        {
            return header.streamId();
        }

        return clusterStreamId;
    }

    public static int checksum(final long reservedValue)
    {
        return (int)(reservedValue >> BITS_IN_INT);
    }
}
