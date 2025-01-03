/*
 * Copyright 2015-2025 Real Logic Limited.
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
package uk.co.real_logic.artio.fields;

import uk.co.real_logic.artio.util.MutableAsciiBuffer;

/**
 * An encoder for FIX's Local Market Date data types. See {@link LocalMktDateDecoder} for details of the format.
 *
 * See {@link UtcTimestampEncoder} for UTC Timestamps.
 */
public final class LocalMktDateEncoder
{
    public static final int LENGTH = 8;
    public static final int MIN_EPOCH_DAYS = LocalMktDateDecoder.MIN_EPOCH_DAYS;
    public static final int MAX_EPOCH_DAYS = LocalMktDateDecoder.MAX_EPOCH_DAYS;

    private final MutableAsciiBuffer flyweight = new MutableAsciiBuffer();

    /**
     * Encode a FIX Local Market Date value to a <code>byte[]</code>.
     *
     * @param localEpochDays the number of days since 1970-01-01 in the local timezone.
     * @param bytes a byte[] to encode the data into.
     * @return the length of the data encoded (always 8)
     */
    public int encode(final int localEpochDays, final byte[] bytes)
    {
        final MutableAsciiBuffer flyweight = this.flyweight;
        flyweight.wrap(bytes);
        return encode(localEpochDays, flyweight, 0);
    }

    /**
     * Encode a FIX Local Market Date value to a {@link MutableAsciiBuffer}.
     *
     * @param localEpochDays the number of days since 1970-01-01 in the local timezone.
     * @param buffer a {@link MutableAsciiBuffer} to encode the data into.
     * @param offset the offset to start within buffer
     * @return the length of the data encoded (always 8)
     */
    public static int encode(final int localEpochDays, final MutableAsciiBuffer buffer, final int offset)
    {
        if (localEpochDays < MIN_EPOCH_DAYS || localEpochDays > MAX_EPOCH_DAYS)
        {
            throw new IllegalArgumentException(localEpochDays + " is outside of the valid range for this encoder");
        }

        CalendricalUtil.encodeDate(localEpochDays, buffer, offset);

        return LENGTH;
    }
}
