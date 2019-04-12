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
package uk.co.real_logic.artio.fields;

import uk.co.real_logic.artio.util.AsciiBuffer;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

import static uk.co.real_logic.artio.fields.CalendricalUtil.MICROS_IN_DAY;
import static uk.co.real_logic.artio.fields.CalendricalUtil.MILLIS_IN_DAY;
import static uk.co.real_logic.artio.fields.UtcDateOnlyDecoder.LENGTH;

/**
 * Parser for Fix's UTC timestamps - see http://fixwiki.org/fixwiki/UTCTimestampDataType for details
 * <p>
 * Equivalent to a Java format string of "yyyyMMdd-HH:mm:ss[.SSS]". The builtin parsers could cope with
 * this situation, but allocate and perform poorly.
 * <p>
 * If the final fraction of the second is expected to be in 3 characters and only supports up to
 * millisecond precision then you can use the normal {@link UtcTimestampDecoder#decode(AsciiBuffer, int, int)} method.
 * Support for microsecond precision, eg: "yyyyMMdd-HH:mm:ss[.SSSSSS]" is provided through the
 * {@link UtcTimestampDecoder#decodeMicros(AsciiBuffer, int, int)} method.
 */
public final class UtcTimestampDecoder
{
    public static final long MIN_EPOCH_MILLIS = -62135596800000L;
    public static final long MAX_EPOCH_MILLIS = 253402300799999L;
    public static final long MIN_EPOCH_MICROS = -62135596800000000L;
    public static final long MAX_EPOCH_MICROS = 253402300799999999L;

    public static final int LENGTH_WITHOUT_MILLISECONDS = 17;
    public static final int LENGTH_WITH_MILLISECONDS = 21;
    public static final int LENGTH_WITH_MICROSECONDS = 24;

    private static final int TIME_OFFSET = LENGTH + 1;

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

    public long decodeMicros(final byte[] bytes, final int length)
    {
        buffer.wrap(bytes);
        return decodeMicros(buffer, 0, length);
    }

    public long decodeMicros(final byte[] bytes)
    {
        return decodeMicros(bytes, bytes.length);
    }

    /**
     * @param timestamp a buffer containing the FIX encoded value of the timestamp in ASCII
     * @param offset the offset within the timestamp buffer where the value starts
     * @param length the length of the FIX encoded value in bytes / ASCII characters
     * @return the number of milliseconds since the Unix Epoch that represents this timestamp
     */
    public static long decode(final AsciiBuffer timestamp, final int offset, final int length)
    {
        final long epochDay = UtcDateOnlyDecoder.decode(timestamp, offset);
        final long millisecondOfDay = UtcTimeOnlyDecoder.decode(
            timestamp, offset + TIME_OFFSET, length - TIME_OFFSET);
        return epochDay * MILLIS_IN_DAY + millisecondOfDay;
    }

    /**
     * @param timestamp a buffer containing the FIX encoded value of the timestamp in ASCII
     * @param offset the offset within the timestamp buffer where the value starts
     * @param length the length of the FIX encoded value in bytes / ASCII characters
     * @return the number of microseconds since the Unix Epoch that represents this timestamp
     */
    public static long decodeMicros(final AsciiBuffer timestamp, final int offset, final int length)
    {
        final long epochDay = UtcDateOnlyDecoder.decode(timestamp, offset);
        final long microsOfDay = UtcTimeOnlyDecoder.decodeMicros(
            timestamp, offset + TIME_OFFSET, length - TIME_OFFSET);
        return epochDay * MICROS_IN_DAY + microsOfDay;
    }

}
