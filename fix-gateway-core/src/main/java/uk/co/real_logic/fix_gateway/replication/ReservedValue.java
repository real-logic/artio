/*
 * Copyright 2015-2016 Real Logic Ltd.
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
package uk.co.real_logic.fix_gateway.replication;

/**
 * 8 byte reserved word is used with the
 */
public final class ReservedValue
{
    private static final int BITS_IN_INT = 32;

    public static long ofStreamId(final int streamId)
    {
        return streamId & 0xFFFFFFFFL;
    }

    public static long ofChecksum(final int checksum)
    {
        return ((long)checksum) << BITS_IN_INT;
    }

    public static long of(final int streamId, final int checksum)
    {
        return ofChecksum(checksum) | ofStreamId(streamId);
    }

    public static int streamId(final long reservedValue)
    {
        return (int) reservedValue;
    }

    public static int checksum(final long reservedValue)
    {
        return (int) (reservedValue >> BITS_IN_INT);
    }
}
