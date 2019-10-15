/*
 * Copyright 2019 Adaptive Financial Technology Ltd.
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

import org.junit.Test;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static uk.co.real_logic.artio.fields.UtcTimestampDecoder.*;
import static uk.co.real_logic.artio.fields.UtcTimestampEncoderValidCasesTest.*;

public class UtcTimestampCodecsTrailingZerosTest
{

    private static final DateTimeFormatter NANOS_FORMATTER =
        DateTimeFormatter.ofPattern("yyyyMMdd-HH:mm:ss[.nnnnnnnnn]");

    private static final String MILLIS_TIMESTAMP = "20191011-18:39:47.000";
    private static final String MICROS_TIMESTAMP = MILLIS_TIMESTAMP + "000";
    private static final String NANOS_TIMESTAMP = MICROS_TIMESTAMP + "000";

    private static final long EPOCH_NANOS = toEpochNanos(NANOS_TIMESTAMP);
    private static final long EPOCH_MICROS = NANOSECONDS.toMicros(EPOCH_NANOS);
    private static final long EPOCH_MILLIS = NANOSECONDS.toMillis(EPOCH_NANOS);

    private static long toEpochNanos(final String timestamp)
    {
        final LocalDateTime parsedDate = LocalDateTime.parse(timestamp, NANOS_FORMATTER);
        final ZonedDateTime utc = ZonedDateTime.of(parsedDate, ZoneId.of("UTC"));
        return SECONDS.toNanos(utc.toEpochSecond()) + utc.getLong(NANO_OF_SECOND);
    }

    @Test
    public void shouldDecodeTrailingZeros()
    {
        final byte[] bytes = NANOS_TIMESTAMP.getBytes(US_ASCII);
        assertEquals(LENGTH_WITH_NANOSECONDS, bytes.length);

        final MutableAsciiBuffer buffer = new MutableAsciiBuffer(new byte[LENGTH_WITH_NANOSECONDS + 2]);
        buffer.putBytes(1, bytes);

        final long epochNanos = UtcTimestampDecoder.decodeNanos(buffer, 1, LENGTH_WITH_NANOSECONDS);

        assertEquals(EPOCH_NANOS, epochNanos);
    }

    @Test
    public void shouldEncodeNanosTrailingZeros()
    {
        assertInstanceEncodesTimestampNanos(EPOCH_NANOS, NANOS_TIMESTAMP, LENGTH_WITH_NANOSECONDS);
    }

    @Test
    public void shouldEncodeNanosTrailingZerosWithOffset()
    {
        assertInstanceEncodesTimestampNanosWithOffset(EPOCH_NANOS, NANOS_TIMESTAMP, LENGTH_WITH_NANOSECONDS);
    }

    @Test
    public void shouldEncodeMicrosTrailingZeros()
    {
        assertInstanceEncodesTimestampMicros(EPOCH_MICROS, MICROS_TIMESTAMP, LENGTH_WITH_MICROSECONDS);
    }

    @Test
    public void shouldEncodeMicrosTrailingZerosWithOffset()
    {
        assertInstanceEncodesTimestampMicrosWithOffset(EPOCH_MICROS, MICROS_TIMESTAMP, LENGTH_WITH_MICROSECONDS);
    }

    @Test
    public void shouldEncodeMillisTrailingZeros()
    {
        assertInstanceEncodesTimestampMillis(EPOCH_MILLIS, MILLIS_TIMESTAMP, LENGTH_WITH_MILLISECONDS);
    }

    @Test
    public void shouldEncodeMillisTrailingZerosWithOffset()
    {
        assertInstanceEncodesTimestampMillisWithOffset(EPOCH_MILLIS, MILLIS_TIMESTAMP, LENGTH_WITH_MILLISECONDS);
    }
}
