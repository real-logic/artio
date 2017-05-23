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
package uk.co.real_logic.fix_gateway.fields;

import uk.co.real_logic.fix_gateway.util.AsciiBuffer;
import uk.co.real_logic.fix_gateway.util.MutableAsciiBuffer;

import static uk.co.real_logic.fix_gateway.fields.CalendricalUtil.*;
import static uk.co.real_logic.fix_gateway.fields.UtcDateOnlyDecoder.LENGTH;

/**
 * "HH:mm:ss[.SSS]"
 */
public final class UtcTimeOnlyDecoder
{
    public static final int SHORT_LENGTH = 8;
    public static final int LONG_LENGTH = 12;

    private final AsciiBuffer buffer = new MutableAsciiBuffer();

    public long decode(final byte[] bytes, final int length)
    {
        buffer.wrap(bytes);
        return decode(buffer, 0, length);
    }

    public long decode(final byte[] bytes)
    {
        return decode(bytes, bytes.length);
    }

    public static long decode(final AsciiBuffer time, final int offset, final int length)
    {
        final int startHour = offset + LENGTH + 1;
        final int endHour = startHour + 2;

        final int startMinute = endHour + 1;
        final int endMinute = startMinute + 2;

        final int startSecond = endMinute + 1;
        final int endSecond = startSecond + 2;

        final int startMillisecond = endSecond + 1;
        final int endMillisecond = startMillisecond + 3;

        final int hour = getValidInt(time, startHour, endHour, 0, 23);
        final int minute = getValidInt(time, startMinute, endMinute, 0, 59);
        final int second = getValidInt(time, startSecond, endSecond, 0, 60);
        final int millisecond;
        if (offset + length > endSecond && time.isDigit(startMillisecond))
        {
            millisecond = time.getNatural(startMillisecond, endMillisecond);
        }
        else
        {
            millisecond = 0;
        }

        final int secondOfDay = hour * SECONDS_IN_HOUR + minute * SECONDS_IN_MINUTE + second;

        return secondOfDay * MILLIS_IN_SECOND + millisecond;
    }
}
