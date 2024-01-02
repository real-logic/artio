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

import uk.co.real_logic.artio.util.MutableAsciiBuffer;

import java.util.concurrent.TimeUnit;

import static uk.co.real_logic.artio.fields.CalendricalUtil.*;
import static uk.co.real_logic.artio.fields.UtcTimeOnlyDecoder.*;

/**
 * An encoder for FIX's Utc timestamp data types. See {@link UtcTimestampDecoder} for details of the format.
 */
public final class UtcTimestampEncoder
{

    public static final long MIN_EPOCH_MILLIS = UtcTimestampDecoder.MIN_EPOCH_MILLIS;
    public static final long MAX_EPOCH_MILLIS = UtcTimestampDecoder.MAX_EPOCH_MILLIS;
    public static final long MIN_EPOCH_MICROS = UtcTimestampDecoder.MIN_EPOCH_MICROS;
    public static final long MAX_EPOCH_MICROS = UtcTimestampDecoder.MAX_EPOCH_MICROS;
    public static final long MIN_EPOCH_NANOS = UtcTimestampDecoder.MIN_EPOCH_NANOS;
    public static final long MAX_EPOCH_NANOS = UtcTimestampDecoder.MAX_EPOCH_NANOS;

    public static final int LENGTH_WITHOUT_MILLISECONDS = UtcTimestampDecoder.LENGTH_WITHOUT_MILLISECONDS;
    public static final int LENGTH_WITH_MILLISECONDS = UtcTimestampDecoder.LENGTH_WITH_MILLISECONDS;
    public static final int LENGTH_WITH_MICROSECONDS = UtcTimestampDecoder.LENGTH_WITH_MICROSECONDS;
    public static final int LENGTH_WITH_NANOSECONDS = UtcTimestampDecoder.LENGTH_WITH_NANOSECONDS;

    private static final int LENGTH_OF_DATE = 8;
    private static final int LENGTH_OF_DATE_AND_DASH = LENGTH_OF_DATE + 1;

    private static final int MILLISECONDS_EPOCH_FRACTION = EpochFractionFormat.MILLISECONDS.ordinal();
    private static final int MICROSECONDS_EPOCH_FRACTION = EpochFractionFormat.MICROSECONDS.ordinal();

    private final int epochFractionPrecision;
    private final byte[] bytes;
    private final MutableAsciiBuffer flyweight;

    private long startOfNextDayInFraction;
    private long beginningOfDayInFraction;

    public UtcTimestampEncoder()
    {
        this(EpochFractionFormat.MILLISECONDS);
    }

    /**
     * Create the encoder.
     *
     * @param epochFractionPrecision true if you want to use milliseconds as the precision of the
     *                                        timeunit for the <code>epochFraction</code> passed to encode().
     *                                        False if you wish to use microseconds.
     */
    public UtcTimestampEncoder(final EpochFractionFormat epochFractionPrecision)
    {
        this.epochFractionPrecision = epochFractionPrecision.ordinal();
        switch (epochFractionPrecision)
        {
            case NANOSECONDS:
                bytes = new byte[LENGTH_WITH_NANOSECONDS];
                break;

            case MICROSECONDS:
                bytes = new byte[LENGTH_WITH_MICROSECONDS];
                break;

            case MILLISECONDS:
                bytes = new byte[LENGTH_WITH_MILLISECONDS];
                break;

            default:
                throw new RuntimeException("Unknown precision: " + epochFractionPrecision);
        }
        flyweight = new MutableAsciiBuffer(bytes);
    }

    /**
     * Encode the current time into the buffer as an ascii UTC String
     *
     * @param duration the current time as the number of specified time unit since the start of the UNIX Epoch.
     * @param timeUnit the {@link TimeUnit} of the duration.
     * @return the length of the encoded data in the flyweight.
     */
    public int encodeFrom(final long duration, final TimeUnit timeUnit)
    {
        return encode(convertToThisTimeUnit(duration, timeUnit));
    }

    /**
     * Encode the current time into the buffer as an ascii UTC String
     *
     * @param epochFraction the current time as the number of milliseconds, microseconds or nanoseconds since the
     *                      start of the UNIX Epoch. The unit of this parameter should align with the constructor
     *                      parameter EpochFractionFormat.
     * @return the length of the encoded data in the flyweight.
     */
    public int encode(final long epochFraction)
    {
        final int epochFractionPrecision = this.epochFractionPrecision;
        if (epochFractionPrecision == MILLISECONDS_EPOCH_FRACTION)
        {
            return encode(epochFraction, flyweight, 0);
        }
        else if (epochFractionPrecision == MICROSECONDS_EPOCH_FRACTION)
        {
            return encodeMicros(epochFraction, flyweight, 0);
        }
        else /*(epochFractionPrecision == NANOSECONDS_EPOCH_FRACTION)*/
        {
            return encodeNanos(epochFraction, flyweight, 0);
        }
    }

    public int initialise(final long duration, final TimeUnit timeUnit)
    {
        return initialise(convertToThisTimeUnit(duration, timeUnit));
    }

    public int initialise(final long epochFraction)
    {
        final long minEpochFraction;
        final long maxEpochFraction;
        final long fractionInSecond;
        final long fractionInDay;
        final int fractionFieldLength;
        final int lengthWithFraction;

        final int epochFractionPrecision = this.epochFractionPrecision;
        if (epochFractionPrecision == MILLISECONDS_EPOCH_FRACTION)
        {
            minEpochFraction = MIN_EPOCH_MILLIS;
            maxEpochFraction = MAX_EPOCH_MILLIS;
            fractionInSecond = MILLIS_IN_SECOND;
            fractionInDay = MILLIS_IN_DAY;
            fractionFieldLength = MILLIS_FIELD_LENGTH;
            lengthWithFraction = LENGTH_WITH_MILLISECONDS;
        }
        else if (epochFractionPrecision == MICROSECONDS_EPOCH_FRACTION)
        {
            minEpochFraction = MIN_EPOCH_MICROS;
            maxEpochFraction = MAX_EPOCH_MICROS;
            fractionInSecond = MICROS_IN_SECOND;
            fractionInDay = MICROS_IN_DAY;
            fractionFieldLength = MICROS_FIELD_LENGTH;
            lengthWithFraction = LENGTH_WITH_MICROSECONDS;
        }
        else /*(epochFractionPrecision == NANOSECONDS_EPOCH_FRACTION)*/
        {
            minEpochFraction = MIN_EPOCH_NANOS;
            maxEpochFraction = MAX_EPOCH_NANOS;
            fractionInSecond = NANOS_IN_SECOND;
            fractionInDay = NANOS_IN_DAY;
            fractionFieldLength = NANOS_FIELD_LENGTH;
            lengthWithFraction = LENGTH_WITH_NANOSECONDS;
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

        return lengthWithFraction;
    }

    /**
     * Update the current time into the buffer as an ascii UTC String
     *
     * @param duration the current time as the number of specified time unit since the start of the UNIX Epoch.
     * @param timeUnit the {@link TimeUnit} of the duration.
     * @return the length of the encoded data in the flyweight.
     */
    public int updateFrom(final long duration, final TimeUnit timeUnit)
    {
        return update(convertToThisTimeUnit(duration, timeUnit));
    }

    /**
     * Update the current time into the buffer as an ascii UTC String
     *
     * @param epochFraction the current time as the number of milliseconds, microseconds or nanoseconds since the
     *                      start of the UNIX Epoch. The unit of this parameter should align with the constructor
     *                      parameter EpochFractionFormat.
     * @return the length of the encoded data in the flyweight.
     */
    public int update(final long epochFraction)
    {
        if (epochFraction >= startOfNextDayInFraction || epochFraction < beginningOfDayInFraction)
        {
            return initialise(epochFraction);
        }

        final long fractionInSecond;
        final int fractionFieldLength;
        final int lengthWithFraction;

        final int epochFractionPrecision = this.epochFractionPrecision;
        if (epochFractionPrecision == MILLISECONDS_EPOCH_FRACTION)
        {
            fractionInSecond = MILLIS_IN_SECOND;
            fractionFieldLength = MILLIS_FIELD_LENGTH;
            lengthWithFraction = LENGTH_WITH_MILLISECONDS;
        }
        else if (epochFractionPrecision == MICROSECONDS_EPOCH_FRACTION)
        {
            fractionInSecond = MICROS_IN_SECOND;
            fractionFieldLength = MICROS_FIELD_LENGTH;
            lengthWithFraction = LENGTH_WITH_MICROSECONDS;
        }
        else /*(epochFractionPrecision == NANOSECONDS_EPOCH_FRACTION)*/
        {
            fractionInSecond = NANOS_IN_SECOND;
            fractionFieldLength = NANOS_FIELD_LENGTH;
            lengthWithFraction = LENGTH_WITH_NANOSECONDS;
        }

        final long localSecond = localSecond(epochFraction, fractionInSecond);
        final int fractionOfSecond = fractionOfSecond(epochFraction, fractionInSecond);

        UtcTimeOnlyEncoder.encodeFraction(
            localSecond, fractionOfSecond, flyweight, LENGTH_OF_DATE_AND_DASH, fractionFieldLength);

        return lengthWithFraction;
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

    public static int encodeNanos(
        final long epochNanos,
        final MutableAsciiBuffer string,
        final int offset)
    {
        return encodeFraction(
            epochNanos,
            string,
            offset,
            MIN_EPOCH_NANOS,
            MAX_EPOCH_NANOS,
            NANOS_IN_SECOND,
            LENGTH_WITH_NANOSECONDS,
            NANOS_FIELD_LENGTH);
    }

    private long convertToThisTimeUnit(final long duration, final TimeUnit timeUnit)
    {
        if (epochFractionPrecision == MILLISECONDS_EPOCH_FRACTION)
        {
            return timeUnit.toMillis(duration);
        }
        else if (epochFractionPrecision == MICROSECONDS_EPOCH_FRACTION)
        {
            return timeUnit.toMicros(duration);
        }
        else /*(epochFractionPrecision == NANOSECONDS_EPOCH_FRACTION)*/
        {
            return timeUnit.toNanos(duration);
        }
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

        return lengthWithFraction;
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
