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

import uk.co.real_logic.artio.util.MutableAsciiBuffer;

import static uk.co.real_logic.artio.fields.CalendricalUtil.*;
import static uk.co.real_logic.artio.fields.UtcTimeOnlyDecoder.MICROS_FIELD_LENGTH;
import static uk.co.real_logic.artio.fields.UtcTimeOnlyDecoder.MILLIS_FIELD_LENGTH;

public final class UtcTimestampEncoder
{
    public static final long MIN_EPOCH_MILLIS = UtcTimestampDecoder.MIN_EPOCH_MILLIS;
    public static final long MAX_EPOCH_MILLIS = UtcTimestampDecoder.MAX_EPOCH_MILLIS;
    public static final long MIN_EPOCH_MICROS = UtcTimestampDecoder.MIN_EPOCH_MICROS;
    public static final long MAX_EPOCH_MICROS = UtcTimestampDecoder.MAX_EPOCH_MICROS;

    public static final int LENGTH_WITHOUT_MILLISECONDS = UtcTimestampDecoder.LENGTH_WITHOUT_MILLISECONDS;
    public static final int LENGTH_WITH_MILLISECONDS = UtcTimestampDecoder.LENGTH_WITH_MILLISECONDS;
    public static final int LENGTH_WITH_MICROSECONDS = UtcTimestampDecoder.LENGTH_WITH_MICROSECONDS;

    private static final int LENGTH_OF_DATE = 8;
    private static final int LENGTH_OF_DATE_AND_DASH = LENGTH_OF_DATE + 1;

    private final boolean usesMilliseconds;
    private final byte[] bytes = new byte[LENGTH_WITH_MICROSECONDS];
    private final MutableAsciiBuffer flyweight = new MutableAsciiBuffer(bytes);

    private long startOfNextDayInFraction;
    private long beginningOfDayInFraction;

    public UtcTimestampEncoder()
    {
        this(true);
    }

    public UtcTimestampEncoder(final boolean usesMilliseconds)
    {
        this.usesMilliseconds = usesMilliseconds;
        flyweight.wrap(bytes);
    }

    /**
     * Encode the current time into the buffer as an ascii UTC String
     *
     * @param epochFraction the current time as the number of milliseconds since the start of the UNIX Epoch.
     * @return the length of the encoded data in the flyweight.
     */
    public int encode(final long epochFraction)
    {
        if (usesMilliseconds)
        {
            return encode(epochFraction, flyweight, 0);
        }
        else
        {
            return encodeMicros(epochFraction, flyweight, 0);
        }
    }

    public int initialise(final long epochFraction)
    {
        final long minEpochFraction;
        final long maxEpochFraction;
        final long fractionInSecond;
        final long fractionInDay;
        final int fractionFieldLength;
        final int lengthWithFraction;

        if (usesMilliseconds)
        {
            minEpochFraction = MIN_EPOCH_MILLIS;
            maxEpochFraction = MAX_EPOCH_MILLIS;
            fractionInSecond = MILLIS_IN_SECOND;
            fractionInDay = MILLIS_IN_DAY;
            fractionFieldLength = MILLIS_FIELD_LENGTH;
            lengthWithFraction = LENGTH_WITH_MILLISECONDS;
        }
        else
        {
            minEpochFraction = MIN_EPOCH_MICROS;
            maxEpochFraction = MAX_EPOCH_MICROS;
            fractionInSecond = MICROS_IN_SECOND;
            fractionInDay = MICROS_IN_DAY;
            fractionFieldLength = MICROS_FIELD_LENGTH;
            lengthWithFraction = LENGTH_WITH_MICROSECONDS;
        }

        validate(epochFraction, minEpochFraction, maxEpochFraction);

        final long localSecond = localSecond(epochFraction, fractionInSecond);
        final long epochDay = epochDay(localSecond);
        final int fractionOfSecond = fractionOfSecond(epochFraction, fractionInSecond);

        startOfNextDayInFraction = (epochDay + 1) * fractionInDay;
        beginningOfDayInFraction = startOfNextDayInFraction - fractionInDay;

        encodeDate(epochDay, flyweight, 0);
        flyweight.putChar(LENGTH_OF_DATE, '-');
        UtcTimeOnlyEncoder.encodeFraction(
            localSecond, fractionOfSecond, flyweight, LENGTH_OF_DATE_AND_DASH, fractionFieldLength);

        return fractionOfSecond > 0 ? lengthWithFraction : LENGTH_WITHOUT_MILLISECONDS;
    }

    public int update(final long epochFraction)
    {
        if (epochFraction > startOfNextDayInFraction || epochFraction < beginningOfDayInFraction)
        {
            return initialise(epochFraction);
        }

        final long fractionInSecond;
        final int fractionFieldLength;
        final int lengthWithFraction;

        if (usesMilliseconds)
        {
            fractionInSecond = MILLIS_IN_SECOND;
            fractionFieldLength = MILLIS_FIELD_LENGTH;
            lengthWithFraction = LENGTH_WITH_MILLISECONDS;
        }
        else
        {
            fractionInSecond = MICROS_IN_SECOND;
            fractionFieldLength = MICROS_FIELD_LENGTH;
            lengthWithFraction = LENGTH_WITH_MICROSECONDS;
        }

        final long localSecond = localSecond(epochFraction, fractionInSecond);
        final int fractionOfSecond = fractionOfSecond(epochFraction, fractionInSecond);

        UtcTimeOnlyEncoder.encodeFraction(
            localSecond, fractionOfSecond, flyweight, LENGTH_OF_DATE_AND_DASH, fractionFieldLength);

        return fractionOfSecond > 0 ? lengthWithFraction : LENGTH_WITHOUT_MILLISECONDS;
    }

    public byte[] buffer()
    {
        return bytes;
    }

    public static int encode(
        final long epochMillis,
        final MutableAsciiBuffer string,
        final int offset)
    {
        return encodeFraction(
            epochMillis,
            string,
            offset,
            MIN_EPOCH_MILLIS,
            MAX_EPOCH_MILLIS,
            MILLIS_IN_SECOND,
            LENGTH_WITH_MILLISECONDS,
            MILLIS_FIELD_LENGTH);
    }

    public static int encodeMicros(
        final long epochMicros,
        final MutableAsciiBuffer string,
        final int offset)
    {
        return encodeFraction(
            epochMicros,
            string,
            offset,
            MIN_EPOCH_MICROS,
            MAX_EPOCH_MICROS,
            MICROS_IN_SECOND,
            LENGTH_WITH_MICROSECONDS,
            MICROS_FIELD_LENGTH);
    }

    private static int encodeFraction(
        final long epochFraction,
        final MutableAsciiBuffer string,
        final int offset,
        final long minEpochFraction,
        final long maxEpochFraction,
        final long fractionInSecond,
        final int lengthWithFraction,
        final int fractionFieldLength)
    {
        validate(epochFraction, minEpochFraction, maxEpochFraction);

        final long localSecond = localSecond(epochFraction, fractionInSecond);
        final long epochDay = epochDay(localSecond);
        final int fractionOfSecond = fractionOfSecond(epochFraction, fractionInSecond);

        encodeDate(epochDay, string, offset);
        string.putChar(offset + LENGTH_OF_DATE, '-');
        UtcTimeOnlyEncoder.encodeFraction(
            localSecond,
            fractionOfSecond,
            string,
            offset + LENGTH_OF_DATE_AND_DASH,
            fractionFieldLength);

        return fractionOfSecond > 0 ? lengthWithFraction : LENGTH_WITHOUT_MILLISECONDS;
    }

    private static long epochDay(final long localSecond)
    {
        return Math.floorDiv(localSecond, SECONDS_IN_DAY);
    }

    private static int fractionOfSecond(final long epochFraction, final long fractionInSecond)
    {
        return (int)(Math.floorMod(epochFraction, fractionInSecond));
    }

    private static long localSecond(final long epochFraction, final long fractionInSecond)
    {
        return Math.floorDiv(epochFraction, fractionInSecond);
    }

    private static void validate(final long epochFraction, final long minEpochFraction, final long maxEpochFraction)
    {
        if (epochFraction < minEpochFraction || epochFraction > maxEpochFraction)
        {
            throw new IllegalArgumentException(epochFraction + " is outside of the valid range for this encoder");
        }
    }
}
