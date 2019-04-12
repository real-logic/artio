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
package uk.co.real_logic.artio.fields;

import org.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

import static uk.co.real_logic.artio.fields.CalendricalUtil.*;
import static uk.co.real_logic.artio.fields.UtcTimeOnlyDecoder.MICROS_FIELD_LENGTH;
import static uk.co.real_logic.artio.fields.UtcTimeOnlyDecoder.MILLIS_FIELD_LENGTH;

/**
 * .
 */
public final class UtcTimeOnlyEncoder
{
    public static final int LENGTH_WITHOUT_MILLISECONDS = 8;
    public static final int LENGTH_WITH_MILLISECONDS = 12;
    public static final int LENGTH_WITH_MICROSECONDS = 15;

    private final UnsafeBuffer buffer = new UnsafeBuffer(0, 0);
    private final MutableAsciiBuffer flyweight = new MutableAsciiBuffer(buffer);

    public int encode(final long millisecondOfDay, final byte[] bytes)
    {
        buffer.wrap(bytes);
        return encode(millisecondOfDay, flyweight, 0);
    }

    public static int encode(
        final long millisecondOfDay,
        final MutableAsciiBuffer string,
        final int offset)
    {
        final long localSecond = Math.floorDiv(millisecondOfDay, MILLIS_IN_SECOND);
        final int fractionOfSecond = (int)(Math.floorMod(millisecondOfDay, MILLIS_IN_SECOND));

        encodeFraction(localSecond, fractionOfSecond, string, offset, MILLIS_FIELD_LENGTH);

        return fractionOfSecond > 0 ? LENGTH_WITH_MILLISECONDS : LENGTH_WITHOUT_MILLISECONDS;
    }

    public static int encodeMicros(
        final long millisecondOfDay,
        final MutableAsciiBuffer string,
        final int offset)
    {
        final long localSecond = Math.floorDiv(millisecondOfDay, MICROS_IN_SECOND);
        final int fractionOfSecond = (int)(Math.floorMod(millisecondOfDay, MICROS_IN_SECOND));

        encodeFraction(localSecond, fractionOfSecond, string, offset, MICROS_FIELD_LENGTH);

        return fractionOfSecond > 0 ? LENGTH_WITH_MICROSECONDS : LENGTH_WITHOUT_MILLISECONDS;
    }

    /*static void encode(
        final long epochSecond,
        final int millisOfSecond,
        final MutableAsciiBuffer string,
        final int offset)
    {
        encodeFraction(epochSecond, millisOfSecond, string, offset, MILLIS_FIELD_LENGTH);
    }

    static void encodeMicros(
        final long epochSecond,
        final int microsOfSecond,
        final MutableAsciiBuffer string,
        final int offset)
    {
        encodeFraction(epochSecond, microsOfSecond, string, offset, MICROS_FIELD_LENGTH);
    }*/

    static void encodeFraction(
        final long epochSecond,
        final int fractionOfSecond,
        final MutableAsciiBuffer string,
        final int offset,
        final int fractionFieldLength)
    {
        int secondOfDay = (int)Math.floorMod(epochSecond, SECONDS_IN_DAY);
        final int hours = secondOfDay / SECONDS_IN_HOUR;
        secondOfDay -= hours * SECONDS_IN_HOUR;
        final int minutes = secondOfDay / SECONDS_IN_MINUTE;
        secondOfDay -= minutes * SECONDS_IN_MINUTE;

        string.putNaturalPaddedIntAscii(offset, 2, hours);
        string.putChar(offset + 2, ':');
        string.putNaturalPaddedIntAscii(offset + 3, 2, minutes);
        string.putChar(offset + 5, ':');
        string.putNaturalPaddedIntAscii(offset + 6, 2, secondOfDay);

        if (fractionOfSecond > 0)
        {
            string.putChar(offset + 8, '.');
            string.putNaturalPaddedIntAscii(offset + 9, fractionFieldLength, fractionOfSecond);
        }
    }
}
