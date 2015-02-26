/*
 * Copyright 2015 Real Logic Ltd.
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

import uk.co.real_logic.fix_gateway.util.AsciiFlyweight;

import static java.lang.String.format;
import static java.time.Year.isLeap;

/**
 * Parser for Fix's UTC timestamps - see http://fixwiki.org/fixwiki/UTCTimestampDataType for details
 * <p>
 * Equivalent to a Java format string of "yyyyMMdd-HH:mm:ss[.SSS]". The builtin parsers could cope with
 * this situation, but allocate and perform poorly.
 * <p>
 */
public class UtcTimestampParser
{

    // ------------ Time Constants ------------
    private static final int SECONDS_IN_MINUTE = 60;
    private static final int SECONDS_IN_HOUR = SECONDS_IN_MINUTE * 60;
    private static final int SECONDS_IN_DAY = SECONDS_IN_HOUR * 24;
    private static final long MILLIS_IN_SECOND = 1000;

    // ------------ Date Constants ------------

    private static final int MAX_DAYS_IN_YEAR = 365;
    private static final int MONTHS_IN_YEAR = 12;
    private static final int DAYS_UNTIL_START_OF_UNIX_EPOCH = 719528;

    /**
     * @param timestamp
     * @param offset
     * @param length
     * @return the number of milliseconds since the Unix Epoch that represents this timestamp
     */
    public static long parse(final AsciiFlyweight timestamp, final int offset, final int length)
    {
        final int endYear = offset + 4;
        final int endMonth = endYear + 2;
        final int endDay = endMonth + 2;

        final int startHour = endDay + 1;
        final int endHour = startHour + 2;

        final int startMinute = endHour + 1;
        final int endMinute = startMinute + 2;

        final int startSecond = endMinute + 1;
        final int endSecond = startSecond + 2;

        final int startMillisecond = endSecond + 1;
        final int endMillisecond = startMillisecond + 3;

        final int year = timestamp.getInt(offset, endYear);
        final int month = getValidInt(timestamp, endYear, endMonth, 1, 12);
        final int day = getValidInt(timestamp, endMonth, endDay, 1, 31);

        final int hour = getValidInt(timestamp, startHour, endHour, 0, 23);
        final int minute = getValidInt(timestamp, startMinute, endMinute, 0, 59);
        final int second = getValidInt(timestamp, startSecond, endSecond, 0, 60);
        final int millisecond = length > endSecond ? timestamp.getInt(startMillisecond, endMillisecond) : 0;

        final int secondOfDay = hour * SECONDS_IN_HOUR + minute * SECONDS_IN_MINUTE + second;

        final long epochDay = toEpochDay(year, month, day);
        final long secs = epochDay * SECONDS_IN_DAY + secondOfDay;
        return secs * MILLIS_IN_SECOND + millisecond;
    }

    private static int getValidInt(
        final AsciiFlyweight timestamp,
        final int startInclusive,
        final int endExclusive,
        final int min,
        final int max)
    {
        final int value = timestamp.getInt(startInclusive, endExclusive);
        if (value < min || value > max)
        {
            throw new IllegalArgumentException(format("Invalid value: %s outside of range %d-%d", value, min, max));
        }
        return value;
    }

    /**
     * Converts a year/month/day representation of a UTC date to the number of days since the epoch.
     *
     * @param year
     * @param month
     * @param day
     * @return
     */
    private static long toEpochDay(int year, int month, int day)
    {
        return yearsToDays(year) + monthsToDays(month, year) + (day - 1) - DAYS_UNTIL_START_OF_UNIX_EPOCH;
    }

    private static long monthsToDays(final int month, final int year)
    {
        long days = (367 * month - 362) / MONTHS_IN_YEAR;
        if (month > 2)
        {
            days--;
            if (!isLeap(year))
            {
                days--;
            }
        }
        return days;
    }

    /**
     * @param years a positive number of years
     * @return the number of days in years
     */
    private static long yearsToDays(final int years)
    {
        // All divisions by a statically known constant, so are really multiplications
        return MAX_DAYS_IN_YEAR * years + (years + 3) / 4 - (years + 99) / 100 + (years + 399) / 400;
    }

}
