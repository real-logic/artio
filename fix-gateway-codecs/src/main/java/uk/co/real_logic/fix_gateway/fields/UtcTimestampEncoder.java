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
package uk.co.real_logic.fix_gateway.fields;

import uk.co.real_logic.fix_gateway.util.MutableAsciiBuffer;

import java.util.concurrent.TimeUnit;

import static uk.co.real_logic.fix_gateway.fields.CalendricalUtil.*;

public final class UtcTimestampEncoder
{
    public static final long MIN_EPOCH_MILLIS = UtcTimestampDecoder.MIN_EPOCH_MILLIS;
    public static final long MAX_EPOCH_MILLIS = UtcTimestampDecoder.MAX_EPOCH_MILLIS;

    public static final long DAY_IN_MILLIS = TimeUnit.DAYS.toMillis(1);
    public static final int LENGTH_WITH_MILLISECONDS = 21;
    public static final int LENGTH_WITHOUT_MILLISECONDS = 17;
    public static final int LENGTH_OF_DATE = 8;
    public static final int LENGTH_OF_DATE_AND_DASH = LENGTH_OF_DATE + 1;

    private final byte[] bytes = new byte[LENGTH_WITH_MILLISECONDS];
    private final MutableAsciiBuffer flyweight = new MutableAsciiBuffer(bytes);

    private long startOfNextDayInMs;
    private long beginningOfDayInMs;

    public UtcTimestampEncoder()
    {
        flyweight.wrap(bytes);
    }

    /**
     * Encode the current time into the buffer as an ascii UTC String
     *
     * @param epochMillis the current time as the number of milliseconds since the start of the UNIX Epoch.
     * @return the length of the encoded data in the flyweight.
     */
    public int encode(final long epochMillis)
    {
        return encode(epochMillis, flyweight, 0);
    }

    public int initialise(final long epochMillis)
    {
        validate(epochMillis);

        final long localSecond = localSecond(epochMillis);
        final long epochDay = epochDay(localSecond);
        final int fractionOfSecond = fractionOfSecond(epochMillis);

        startOfNextDayInMs = (epochDay + 1) * DAY_IN_MILLIS;
        beginningOfDayInMs = startOfNextDayInMs - DAY_IN_MILLIS;

        encodeDate(epochDay, flyweight, 0);
        flyweight.putChar(LENGTH_OF_DATE, '-');
        UtcTimeOnlyEncoder.encode(localSecond, fractionOfSecond, flyweight, LENGTH_OF_DATE_AND_DASH);

        return fractionOfSecond > 0 ? LENGTH_WITH_MILLISECONDS : LENGTH_WITHOUT_MILLISECONDS;
    }

    public int update(final long epochMillis)
    {
        if (epochMillis > startOfNextDayInMs || epochMillis < beginningOfDayInMs)
        {
            return initialise(epochMillis);
        }

        final long localSecond = localSecond(epochMillis);
        final int fractionOfSecond = fractionOfSecond(epochMillis);

        UtcTimeOnlyEncoder.encode(localSecond, fractionOfSecond, flyweight, LENGTH_OF_DATE_AND_DASH);

        return fractionOfSecond > 0 ? LENGTH_WITH_MILLISECONDS : LENGTH_WITHOUT_MILLISECONDS;
    }

    public byte[] buffer()
    {
        return bytes;
    }

    public static int encode(final long epochMillis, final MutableAsciiBuffer string, final int offset)
    {
        validate(epochMillis);

        final long localSecond = localSecond(epochMillis);
        final long epochDay = epochDay(localSecond);
        final int fractionOfSecond = fractionOfSecond(epochMillis);

        encodeDate(epochDay, string, offset);
        string.putChar(offset + LENGTH_OF_DATE, '-');
        UtcTimeOnlyEncoder.encode(localSecond, fractionOfSecond, string, offset + LENGTH_OF_DATE_AND_DASH);

        return fractionOfSecond > 0 ? LENGTH_WITH_MILLISECONDS : LENGTH_WITHOUT_MILLISECONDS;
    }

    private static long epochDay(final long localSecond)
    {
        return Math.floorDiv(localSecond, SECONDS_IN_DAY);
    }

    private static int fractionOfSecond(final long epochMillis)
    {
        return (int)(Math.floorMod(epochMillis, MILLIS_IN_SECOND));
    }

    private static long localSecond(final long epochMillis)
    {
        return Math.floorDiv(epochMillis, MILLIS_IN_SECOND);
    }

    private static void validate(final long epochMillis)
    {
        if (epochMillis < MIN_EPOCH_MILLIS || epochMillis > MAX_EPOCH_MILLIS)
        {
            throw new IllegalArgumentException(epochMillis + " is outside of the valid range for this encoder");
        }
    }
}
