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

import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.util.MutableAsciiBuffer;

import static uk.co.real_logic.fix_gateway.fields.CalendricalUtil.*;

public final class UtcTimestampEncoder
{
    public static final long MIN_EPOCH_MILLIS = UtcTimestampDecoder.MIN_EPOCH_MILLIS;
    public static final long MAX_EPOCH_MILLIS = UtcTimestampDecoder.MAX_EPOCH_MILLIS;

    public static final int LENGTH_WITH_MILLISECONDS = 21;
    public static final int LENGTH_WITHOUT_MILLISECONDS = 17;

    private final byte[] bytes = new byte[LENGTH_WITH_MILLISECONDS];
    private final UnsafeBuffer buffer = new UnsafeBuffer(bytes);
    private final MutableAsciiBuffer flyweight = new MutableAsciiBuffer(buffer);

    public int encode(final long epochMillis)
    {
        buffer.wrap(bytes);
        return encode(epochMillis, flyweight, 0);
    }

    public byte[] buffer()
    {
        return bytes;
    }

    public static int encode(final long epochMillis, final MutableAsciiBuffer string, final int offset)
    {
        if (epochMillis < MIN_EPOCH_MILLIS || epochMillis > MAX_EPOCH_MILLIS)
        {
            throw new IllegalArgumentException(epochMillis + " is outside of the valid range for this encoder");
        }

        final long localSecond = Math.floorDiv(epochMillis, MILLIS_IN_SECOND);
        final long epochDay = Math.floorDiv(localSecond, SECONDS_IN_DAY);
        final int fractionOfSecond = (int)(Math.floorMod(epochMillis, MILLIS_IN_SECOND));

        encodeDate(epochDay, string, offset);
        string.putChar(offset + 8, '-');
        UtcTimeOnlyEncoder.encode(localSecond, fractionOfSecond, string, offset + 9);

        return fractionOfSecond > 0 ? LENGTH_WITH_MILLISECONDS : LENGTH_WITHOUT_MILLISECONDS;
    }
}
