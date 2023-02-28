/*
 * Copyright 2015-2023 Real Logic Limited.
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
 * .
 */
public final class UtcDateOnlyDecoder
{
    public static final int SIZE_OF_YEAR = 4;
    public static final int SIZE_OF_MONTH = 2;
    public static final int SIZE_OF_DAY = 2;
    public static final int LENGTH = SIZE_OF_YEAR + SIZE_OF_MONTH + SIZE_OF_DAY;

    private final AsciiBuffer flyweight = new MutableAsciiBuffer();

    public int decode(final byte[] bytes)
    {
        flyweight.wrap(bytes);
        return decode(flyweight, 0);
    }

    public static int decode(final AsciiBuffer date, final int offset)
    {
        final int endYear = offset + SIZE_OF_YEAR;
        final int endMonth = endYear + SIZE_OF_MONTH;
        final int endDay = endMonth + SIZE_OF_DAY;

        final int year = date.getNatural(offset, endYear);
        final int month = getValidInt(date, endYear, endMonth, 1, 12);
        final int day = getValidInt(date, endMonth, endDay, 1, 31);
        return toEpochDay(year, month, day);
    }
}
