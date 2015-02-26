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

import static uk.co.real_logic.fix_gateway.fields.DateDecoderUtil.getValidInt;
import static uk.co.real_logic.fix_gateway.fields.DateDecoderUtil.toEpochDay;

/**
 * Equivalent to parsing a Java format string of "yyyyMMdd", allocation free.
 */
public final class LocalMktDateDecoder
{
    private LocalMktDateDecoder()
    {
    }

    /**
     *
     * @param timestamp
     * @param offset
     * @param length
     * @return an int representing the number of days since the beginning of the epoch in the local timezone.
     */
    public static int decode(final AsciiFlyweight timestamp, final int offset, final int length)
    {
        final int endYear = offset + 4;
        final int endMonth = endYear + 2;
        final int endDay = endMonth + 2;

        final int year = timestamp.getInt(offset, endYear);
        final int month = getValidInt(timestamp, endYear, endMonth, 1, 12);
        final int day = getValidInt(timestamp, endMonth, endDay, 1, 31);

        return toEpochDay(year, month, day);
    }
}
