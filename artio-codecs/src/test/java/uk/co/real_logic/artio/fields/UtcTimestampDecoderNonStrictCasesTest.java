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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Arrays;
import java.util.Collection;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.time.temporal.ChronoField.MICRO_OF_SECOND;
import static java.time.temporal.ChronoField.MILLI_OF_SECOND;
import static java.time.temporal.ChronoField.NANO_OF_SECOND;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static uk.co.real_logic.artio.fields.UtcTimestampDecoder.LENGTH_WITH_NANOSECONDS;

@RunWith(Parameterized.class)
public class UtcTimestampDecoderNonStrictCasesTest
{
    private static final DateTimeFormatter FORMATTER = new DateTimeFormatterBuilder()
        .appendPattern("yyyyMMdd-HH:mm:ss")
        .appendFraction(NANO_OF_SECOND, 0, 9, true)
        .toFormatter();

    public static long toEpochMillis(final String timestamp)
    {
        final LocalDateTime parsedDate = LocalDateTime.parse(timestamp, FORMATTER);
        final ZonedDateTime utc = ZonedDateTime.of(parsedDate, ZoneId.of("UTC"));
        return SECONDS.toMillis(utc.toEpochSecond()) + utc.getLong(MILLI_OF_SECOND);
    }

    public static long toEpochMicros(final String timestamp)
    {
        final LocalDateTime parsedDate = LocalDateTime.parse(timestamp, FORMATTER);
        final ZonedDateTime utc = ZonedDateTime.of(parsedDate, ZoneId.of("UTC"));
        return SECONDS.toMicros(utc.toEpochSecond()) + utc.getLong(MICRO_OF_SECOND);
    }

    public static long toEpochNanos(final String timestamp)
    {
        final LocalDateTime parsedDate = LocalDateTime.parse(timestamp, FORMATTER);
        final ZonedDateTime utc = ZonedDateTime.of(parsedDate, ZoneId.of("UTC"));
        return SECONDS.toNanos(utc.toEpochSecond()) + utc.getLong(NANO_OF_SECOND);
    }

    private final int length;
    private final long expectedEpochMillis;
    private final long expectedEpochMicros;
    private final long expectedEpochNanos;
    private final MutableAsciiBuffer buffer;
    private final String timestamp;

    @Parameters(name = "{0}")
    public static Collection<Object[]> data()
    {
        return Arrays.asList(
            new Object[] {"20150225-17:51:32"},
            new Object[] {"20150225-17:51:32.1"},
            new Object[] {"20600225-17:51:32.123"},
            new Object[] {"20600225-17:51:32.1234"},
            new Object[] {"20600225-17:51:32.12345"},
            new Object[] {"20600225-17:51:32.123456"},
            new Object[] {"20600225-17:51:32.1234567"},
            new Object[] {"20600225-17:51:32.12345678"},
            new Object[] {"20600225-17:51:32.123456789"}
        );
    }

    public UtcTimestampDecoderNonStrictCasesTest(final String timestamp)
    {
        this.timestamp = timestamp;
        this.length = timestamp.length();

        expectedEpochMillis = toEpochMillis(timestamp);
        expectedEpochMicros = toEpochMicros(timestamp);
        expectedEpochNanos = toEpochNanos(timestamp);

        final byte[] bytes = timestamp.getBytes(US_ASCII);
        buffer = new MutableAsciiBuffer(new byte[LENGTH_WITH_NANOSECONDS + 2]);
        buffer.putBytes(1, bytes);
    }

    @Test
    public void shouldParseTimestampMillis()
    {
        final long epochMillis = UtcTimestampDecoder.decode(buffer, 1, length, false);
        assertEquals("Failed Millis testcase for: " + timestamp, expectedEpochMillis, epochMillis);
    }

    @Test
    public void shouldParseTimestampMicros()
    {
        final long epochMicros = UtcTimestampDecoder.decodeMicros(buffer, 1, length, false);
        assertEquals("Failed Micros testcase for: " + buffer.getAscii(1, length),
            expectedEpochMicros, epochMicros);
    }

    @Test
    public void shouldParseTimestampNanos()
    {
        final long epochNanos = UtcTimestampDecoder.decodeNanos(buffer, 1, length, false);
        assertEquals("Failed Nanos testcase for: " + timestamp, expectedEpochNanos, epochNanos);
    }
}
