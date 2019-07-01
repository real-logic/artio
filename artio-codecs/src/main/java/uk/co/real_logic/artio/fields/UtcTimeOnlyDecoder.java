/*
 * Copyright 2015-2018 Real Logic Ltd.
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
package uk.co.real_logic.artio.fields;

import uk.co.real_logic.artio.util.AsciiBuffer;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

import static uk.co.real_logic.artio.fields.CalendricalUtil.*;

/**
 * "HH:mm:ss[.SSS]"         - Millisecond Format
 * "HH:mm:ss[.SSSSSS]"      - Microsecond Format
 * "HH:mm:ss[.SSSSSSSSS]"   - Nanosecond  Format
 */
public final class UtcTimeOnlyDecoder
{
    public static final int SHORT_LENGTH = 8;
    public static final int LONG_LENGTH = 12;
    public static final int LONG_LENGTH_MICROS = 15;
    public static final int LONG_LENGTH_NANOS = 18;

    static final int MILLIS_FIELD_LENGTH = 3;
    static final int MICROS_FIELD_LENGTH = 6;
    static final int NANOS_FIELD_LENGTH = 9;

    private final AsciiBuffer buffer = new MutableAsciiBuffer();

    public long decode(final byte[] bytes, final int length)
    {
        buffer.wrap(bytes);
        return decode(buffer, 0, length);
    }

    public long decodeMicros(final byte[] bytes, final int length)
    {
        buffer.wrap(bytes);
        return decodeMicros(buffer, 0, length);
    }

    public long decodeNanos(final byte[] bytes, final int length)
    {
        buffer.wrap(bytes);
        return decodeNanos(buffer, 0, length);
    }

    public long decode(final byte[] bytes)
    {
        return decode(bytes, bytes.length);
    }

    public long decodeMicros(final byte[] bytes)
    {
        return decodeMicros(bytes, bytes.length);
    }

    public long decodeNanos(final byte[] bytes)
    {
        return decodeNanos(bytes, bytes.length);
    }

    public static long decode(final AsciiBuffer time, final int offset, final int length)
    {
        return decodeFraction(time, offset, length, MILLIS_FIELD_LENGTH, MILLIS_IN_SECOND);
    }

    public static long decodeMicros(final AsciiBuffer time, final int offset, final int length)
    {
        return decodeFraction(time, offset, length, MICROS_FIELD_LENGTH, MICROS_IN_SECOND);
    }

    public static long decodeNanos(final AsciiBuffer time, final int offset, final int length)
    {
        return decodeFraction(time, offset, length, NANOS_FIELD_LENGTH, NANOS_IN_SECOND);
    }

    // A fraction could be a millisecond or a microsecond
    private static long decodeFraction(
        final AsciiBuffer time,
        final int offset,
        final int length,
        final int fractionsLength,
        final long fractionsInSecond)
    {
        final int startHour = offset;
        final int endHour = startHour + 2;

        final int startMinute = endHour + 1;
        final int endMinute = startMinute + 2;

        final int startSecond = endMinute + 1;
        final int endSecond = startSecond + 2;

        final int startFraction = endSecond + 1;
        final int endFraction = startFraction + fractionsLength;

        final int hour = getValidInt(time, startHour, endHour, 0, 23);
        final int minute = getValidInt(time, startMinute, endMinute, 0, 59);
        final int second = getValidInt(time, startSecond, endSecond, 0, 60);
        final int fraction;
        if (offset + length > endSecond && time.isDigit(startFraction))
        {
            fraction = time.getNatural(startFraction, endFraction);
        }
        else
        {
            fraction = 0;
        }

        final int secondOfDay = hour * SECONDS_IN_HOUR + minute * SECONDS_IN_MINUTE + second;

        return secondOfDay * fractionsInSecond + fraction;
    }
}
