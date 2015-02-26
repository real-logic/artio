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

import uk.co.real_logic.fix_gateway.util.MutableAsciiFlyweight;

import static uk.co.real_logic.fix_gateway.fields.CalendricalUtil.*;

public final class UtcTimestampEncoder
{
    private UtcTimestampEncoder()
    {
    }

    public static int encode(final long epochMillis, final MutableAsciiFlyweight string, final int offset)
    {
        final long localSecond = Math.floorDiv(epochMillis, MILLIS_IN_SECOND);
        final long epochDay = Math.floorDiv(localSecond, SECONDS_IN_DAY);
        final int fractionOfSecond = (int) (Math.floorMod(epochMillis, MILLIS_IN_SECOND));

        encodeDate(epochDay, string, offset);
        string.putChar(8, '-');
        encodeTime(localSecond, fractionOfSecond, string, offset + 9);

        return fractionOfSecond > 0 ? 21 : 17;
    }

    // Based on:
    // https://github.com/ThreeTen/threetenbp/blob/master/src/main/java/org/threeten/bp/LocalDate.java#L281
    private static void encodeDate(final long epochDay, final MutableAsciiFlyweight string, final int offset)
    {
        long zeroDay = epochDay + DAYS_UNTIL_START_OF_UNIX_EPOCH;
        // find the march-based year
        zeroDay -= 60;  // adjust to 0000-03-01 so leap day is at end of four year cycle
        long adjust = 0;
        if (zeroDay < 0)
        {
            // adjust negative years to positive for calculation
            long adjustCycles = (zeroDay + 1) / DAYS_IN_400_YEAR_CYCLE - 1;
            adjust = adjustCycles * 400;
            zeroDay += -adjustCycles * DAYS_IN_400_YEAR_CYCLE;
        }
        long yearEst = (400 * zeroDay + 591) / DAYS_IN_400_YEAR_CYCLE;
        long doyEst = estimateDayOfYear(zeroDay, yearEst);
        if (doyEst < 0)
        {
            // fix estimate
            yearEst--;
            doyEst = estimateDayOfYear(zeroDay, yearEst);
        }
        yearEst += adjust;  // reset any negative year
        int marchDoy0 = (int) doyEst;

        // convert march-based values back to january-based
        int marchMonth0 = (marchDoy0 * 5 + 2) / 153;
        int month = (marchMonth0 + 2) % 12 + 1;
        int day = marchDoy0 - (marchMonth0 * 306 + 5) / 10 + 1;
        int year = (int) (yearEst + marchMonth0 / 10);

        string.putNatural(offset, 4, year);
        string.putNatural(offset + 4, 2, month);
        string.putNatural(offset + 6, 2, day);
    }

    private static void encodeTime(
        final long epochSecond,
        final int epochMillis,
        final MutableAsciiFlyweight string,
        final int offset)
    {
        int secondOfDay = (int) Math.floorMod(epochSecond, SECONDS_IN_DAY);
        int hours = (int) (secondOfDay / SECONDS_IN_HOUR);
        secondOfDay -= hours * SECONDS_IN_HOUR;
        int minutes = (int) (secondOfDay / SECONDS_IN_MINUTE);
        secondOfDay -= minutes * SECONDS_IN_MINUTE;

        string.putNatural(offset, 2, hours);
        string.putChar(offset + 2, ':');
        string.putNatural(offset + 3, 2, minutes);
        string.putChar(offset + 5, ':');
        string.putNatural(offset + 6, 2, secondOfDay);

        if (epochMillis > 0)
        {
            string.putChar(offset + 8, '.');
            string.putNatural(offset + 9, 3, epochMillis);
        }
    }

    private static long estimateDayOfYear(final long zeroDay, final long yearEst)
    {
        return zeroDay - (365 * yearEst + yearEst / 4 - yearEst / 100 + yearEst / 400);
    }
}
