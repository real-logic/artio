/*
 * Copyright 2015-2024 Real Logic Limited.
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

import uk.co.real_logic.artio.util.AsciiBuffer;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

import static uk.co.real_logic.artio.fields.CalendricalUtil.getValidInt;
import static uk.co.real_logic.artio.fields.CalendricalUtil.toEpochDay;

/**
 * An encoder for FIX's Local Market Date data types. This an allocation free class for encoding specific data types,
 * with better performance characteristics than using a Java {@link java.time.LocalDate}. Local here means defined in
 * terms of the timezone of the market.
 * <p>
 * The encoded message format is equivalent to encoding a Java format string of "yyyyMMdd". The decoder and encoder
 * convert into "epoch days" - the epoch here where days are 0 is 1970-01-01.
 * <p>
 * Valid values: YYYY = 0000-9999, MM = 01-12, DD = 01-31.
 * <p>
 * See {@link LocalMktDateEncoder} for how to encode this data type, see {@link UtcTimestampDecoder} for UTC
 * timestamps.
 */
public final class LocalMktDateDecoder
{
    public static final int MIN_EPOCH_DAYS = -719162;
    public static final int MAX_EPOCH_DAYS = 2932896;
    public static final int LENGTH = 8;

    private final AsciiBuffer buffer = new MutableAsciiBuffer();

    /**
     * Decode a FIX local mkt date value from a <code>byte[]</code> into an epoch days int value.
     *
     * @param bytes a byte array of length 8 or more containing the data to decode.
     * @return the number of days since 1970-01-01 in the local timezone.
     */
    public int decode(final byte[] bytes)
    {
        if (bytes.length < LENGTH)
        {
            throw new IllegalArgumentException(
                "note enough data, needs " + LENGTH + " bytes, but has " + bytes.length);
        }

        final AsciiBuffer buffer = this.buffer;
        buffer.wrap(bytes);
        return decode(buffer, 0, LENGTH);
    }

    /**
     * Decode a FIX local mkt date value from a buffer into an epoch days int value.
     *
     * @param timestamp the buffer containing the FIX local mkt date value
     * @param offset the position in the buffer where the value starts
     * @param length the length of the value in bytes
     * @return the number of days since 1970-01-01 in the local timezone.
     */
    public static int decode(final AsciiBuffer timestamp, final int offset, final int length)
    {
        final int endYear = offset + 4;
        final int endMonth = endYear + 2;
        final int endDay = endMonth + 2;

        final int year = timestamp.parseNaturalIntAscii(offset, 4);
        final int month = getValidInt(timestamp, endYear, endMonth, 1, 12);
        final int day = getValidInt(timestamp, endMonth, endDay, 1, 31);

        return toEpochDay(year, month, day);
    }
}
