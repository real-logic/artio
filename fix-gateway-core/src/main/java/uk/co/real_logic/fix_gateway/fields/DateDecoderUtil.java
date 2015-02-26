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

final class DateDecoderUtil
{
    private DateDecoderUtil()
    {
    }

    // ------------ Date Constants ------------

    private static final int MAX_DAYS_IN_YEAR = 365;
    private static final int MONTHS_IN_YEAR = 12;
    private static final int DAYS_UNTIL_START_OF_UNIX_EPOCH = 719528;

    static int getValidInt(
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
    static int toEpochDay(int year, int month, int day)
    {
        return yearsToDays(year) + monthsToDays(month, year) + (day - 1) - DAYS_UNTIL_START_OF_UNIX_EPOCH;
    }

    private static int monthsToDays(final int month, final int year)
    {
        int days = (367 * month - 362) / MONTHS_IN_YEAR;
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
    private static int yearsToDays(final int years)
    {
        // All divisions by a statically known constant, so are really multiplications
        return MAX_DAYS_IN_YEAR * years + (years + 3) / 4 - (years + 99) / 100 + (years + 399) / 400;
    }
}
