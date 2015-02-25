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

/**
 * Parser for Fix's UTC timestamps - see http://fixwiki.org/fixwiki/UTCTimestampDataType for details
 *
 * "yyyyMMdd-HH:mm:ss"
 *
 * Builtin parsers could cope with this situation, but allocate.
 */
public class UtcTimestampParser
{
    public static long parse(final AsciiFlyweight timestamp, final int offset)
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

        final int years = timestamp.getInt(offset, endYear);

        return 0;
    }
}
