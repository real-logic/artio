/*
 * Copyright 2015-2020 Real Logic Limited.
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
 * Equivalent to parsing a Java format string of "yyyyMMdd", allocation free.
 */
public final class LocalMktDateDecoder
{
    public static final int MIN_EPOCH_DAYS = -719162;
    public static final int MAX_EPOCH_DAYS = 2932896;
    public static final int LENGTH = 8;

    private final AsciiBuffer buffer = new MutableAsciiBuffer();

    public int decode(final byte[] bytes)
    {
        buffer.wrap(bytes);
        return decode(buffer, 0, 1);
    }

    /**
     * Decode a FIX local mkt date value from a buffer into an epoch days int value.
     *
     * @param timestamp the buffer containing the FIX local mkt date value
     * @param offset the position in the buffer where the value starts
     * @param length the length of the value in bytes
     * @return an int representing the number of days since the beginning of the epoch in the local timezone.
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
