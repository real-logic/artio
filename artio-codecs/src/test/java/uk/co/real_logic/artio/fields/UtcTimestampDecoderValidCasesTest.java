/*
 * Copyright 2015-2024 Real Logic Limited.
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
import java.util.Arrays;
import java.util.Collection;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.time.temporal.ChronoField.MILLI_OF_SECOND;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertEquals;
import static uk.co.real_logic.artio.fields.CalendricalUtil.*;
import static uk.co.real_logic.artio.fields.UtcTimestampDecoder.*;

@RunWith(Parameterized.class)
public class UtcTimestampDecoderValidCasesTest
{
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd-HH:mm:ss[.SSS]");

    public static long toEpochMillis(final String timestamp)
    {
        final LocalDateTime parsedDate = LocalDateTime.parse(timestamp, FORMATTER);
        final ZonedDateTime utc = ZonedDateTime.of(parsedDate, ZoneId.of("UTC"));
        return SECONDS.toMillis(utc.toEpochSecond()) + utc.getLong(MILLI_OF_SECOND);
    }

    private final int length;
    private final long expectedEpochMillis;
    private long expectedEpochMicros;
    private long expectedEpochNanos;
    private final MutableAsciiBuffer buffer;
    private final String timestamp;
    private final boolean validNanoSecondTestCase;

    @Parameters(name = "{0}")
    public static Collection<Object[]> data()
    {
        return Arrays.asList(
            new Object[] {"20150225-17:51:32.000", true},
            new Object[] {"20150225-17:51:32.123", true},
            new Object[] {"20600225-17:51:32.123", true},
            new Object[] {"19700101-00:00:00.000", true},
            new Object[] {"00010101-00:00:00.000", false},
            new Object[] {"00010101-00:00:00.001", false},
            new Object[] {"99991231-23:59:59.999", false}
        );
    }

    public UtcTimestampDecoderValidCasesTest(final String timestamp, final boolean validNanoSecondTestCase)
    {
        this.timestamp = timestamp;
        this.validNanoSecondTestCase = validNanoSecondTestCase;

        expectedEpochMillis = toEpochMillis(timestamp);
        length = timestamp.length();

        expectedEpochNanos = expectedEpochMillis * NANOS_IN_MILLIS;

        final byte[] bytes = timestamp.getBytes(US_ASCII);
        buffer = new MutableAsciiBuffer(new byte[LENGTH_WITH_NANOSECONDS + 2]);
        buffer.putBytes(1, bytes);
        expectedEpochMicros = expectedEpochMillis * MICROS_IN_MILLIS;
    }

    @Test
    public void shouldParseTimestampMillis()
    {
        assertDecodeMillis(length);
    }

    @Test
    public void shouldParseTimestampMillisLong()
    {
        putMicros();

        assertDecodeMillis(LENGTH_WITH_MICROSECONDS);
    }

    private void assertDecodeMillis(final int lengthWithMicroseconds)
    {
        final long epochMillis = UtcTimestampDecoder.decode(buffer, 1, lengthWithMicroseconds, true);
        assertEquals("Failed Millis testcase for: " + timestamp, expectedEpochMillis, epochMillis);
    }

    @Test
    public void shouldParseTimestampMicros()
    {
        expectedEpochMicros++;
        putMicros();

        assertDecodesMicros(LENGTH_WITH_MICROSECONDS);
    }

    @Test
    public void shouldParseTimestampMicrosShort()
    {
        assertDecodesMicros(length);
    }

    @Test
    public void shouldParseTimestampMicrosLong()
    {
        putNanos();

        assertDecodesMicros(LENGTH_WITH_NANOSECONDS);
    }

    private void assertDecodesMicros(final int length)
    {
        final long epochMicros = UtcTimestampDecoder.decodeMicros(buffer, 1, length, true);
        assertEquals("Failed Micros testcase for: " + buffer.getAscii(1, length),
            expectedEpochMicros, epochMicros);
    }

    @Test
    public void shouldParseTimestampNanos()
    {
        if (validNanoSecondTestCase)
        {
            // If they've got the suffix field, then test microseconds, add 1 to the value
            expectedEpochNanos++;
            putNanos();

            assertDecodesNanos(LENGTH_WITH_NANOSECONDS);
        }
    }

    @Test
    public void shouldParseTimestampNanosShort()
    {
        if (validNanoSecondTestCase)
        {
            expectedEpochNanos += NANOS_IN_MICROS;

            putMicros();

            assertDecodesNanos(LENGTH_WITH_MICROSECONDS);
        }
    }

    private void assertDecodesNanos(final int length)
    {
        final long epochNanos = UtcTimestampDecoder.decodeNanos(buffer, 1, length, true);
        assertEquals("Failed Nanos testcase for: " + timestamp, expectedEpochNanos, epochNanos);
    }

    private void putNanos()
    {
        putSuffix("000001");
    }

    private void putMicros()
    {
        putSuffix("001");
    }

    private void putSuffix(final String suffix)
    {
        buffer.putAscii(length + 1, suffix);
    }

}
