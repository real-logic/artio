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

import java.time.Month;

import static uk.co.real_logic.fix_gateway.fields.CalendricalUtil.isValidDayOfMonth;
import static uk.co.real_logic.fix_gateway.fields.CalendricalUtil.isValidMonth;

/**
 * Allocation free representation + codec for the FIX MonthYear data type.
 *
 * A Month Year could be:
 *
 * <ul>
 *     <li>A pair of Year & Month</li>
 *     <li>A pair of Year & Month with a day</li>
 *     <li>A pair of Year & Month with a week</li>
 * </ul>
 *
 * Since the month year field may represent calendrical values with differing
 * precisions it can't just be represented by a primitive field-of-epoch format.
 */
public final class MonthYear
{
    private static final int SIZE_OF_YEAR = 4;
    private static final int SIZE_OF_MONTH = 2;
    private static final int SIZE_OF_DAY = 2;
    private static final int SIZE_OF_WEEK = 1;

    public static final int NONE = -1;
    public static final int SHORT_LENGTH = 6;
    public static final int LONG_LENGTH = 8;

    private int year;
    private Month month = Month.JANUARY;
    private int dayOfMonth = NONE;
    private int weekOfMonth = NONE;

    public static MonthYear of(final int year, final Month month)
    {
        return new MonthYear().year(year).month(month);
    }

    public static MonthYear withDayOfMonth(final int year, final Month month, final int dayOfMonth)
    {
        return of(year, month).dayOfMonth(dayOfMonth);
    }

    public static MonthYear withWeekOfMonth(final int year, final Month month, final int weekOfMonth)
    {
        return of(year, month).weekOfMonth(weekOfMonth);
    }

    public int year()
    {
        return year;
    }

    public MonthYear year(final int year)
    {
        this.year = year;
        return this;
    }

    public Month month()
    {
        return month;
    }

    public MonthYear month(final Month month)
    {
        this.month = month;
        return this;
    }

    public int dayOfMonth()
    {
        return dayOfMonth;
    }

    public MonthYear dayOfMonth(final int dayOfMonth)
    {
        this.dayOfMonth = dayOfMonth;
        return this;
    }

    public boolean hasDayOfMonth()
    {
        return dayOfMonth() != NONE;
    }

    public int weekOfMonth()
    {
        return weekOfMonth;
    }

    public MonthYear weekOfMonth(final int weekOfMonth)
    {
        this.weekOfMonth = weekOfMonth;
        return this;
    }

    public boolean hasWeekOfMonth()
    {
        return weekOfMonth() != NONE;
    }

    public boolean decode(final AsciiFlyweight buffer, final int offset, final int length)
    {
        if (length != SHORT_LENGTH && length != LONG_LENGTH)
        {
            return false;
        }

        final int endYear = offset + SIZE_OF_YEAR;
        final int endMonth = endYear + SIZE_OF_MONTH;

        final int year = buffer.getNatural(offset, endYear);
        final int month = buffer.getNatural(endYear, endMonth);

        if (!isValidMonth(month))
        {
            return false;
        }

        if (length == LONG_LENGTH)
        {
            final int endDay = endMonth + SIZE_OF_DAY;
            final int dayOfMonth = buffer.getNatural(endMonth, endDay);

            if (!isValidDayOfMonth(dayOfMonth))
            {
                return false;
            }

            dayOfMonth(dayOfMonth);
        }

        year(year);
        month(Month.of(month));

        return true;
    }

    public boolean equals(final Object o)
    {
        if (this == o)
        {
            return true;
        }

        if (o == null || getClass() != o.getClass())
        {
            return false;
        }

        final MonthYear monthYear = (MonthYear) o;

        return year == monthYear.year
            && month == monthYear.month
            && dayOfMonth == monthYear.dayOfMonth
            && weekOfMonth == monthYear.weekOfMonth;
    }

    public int hashCode()
    {
        int result = year;
        result = 31 * result + month.hashCode();
        result = 31 * result + dayOfMonth;
        result = 31 * result + weekOfMonth;
        return result;
    }

    public String toString()
    {
        if (hasDayOfMonth())
        {
            return String.format("%04d%02d%02d", year(), month().getValue(), dayOfMonth());
        }
        else if (hasWeekOfMonth())
        {
            return String.format("%04d%02dw%d", year(), month().getValue(), weekOfMonth());
        }
        else
        {
            return String.format("%04d%02d", year(), month().getValue());
        }
    }
}
