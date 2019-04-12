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
import static uk.co.real_logic.artio.fields.CalendricalUtil.MICROS_IN_MILLIS;
import static uk.co.real_logic.artio.fields.UtcTimestampDecoder.LENGTH_WITH_MICROSECONDS;
import static uk.co.real_logic.artio.fields.UtcTimestampDecoder.LENGTH_WITH_MILLISECONDS;

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

    private final String timestamp;

    @Parameters(name = "{0}")
    public static Collection<String[]> data()
    {
        return Arrays.asList(
            new String[] {"00010101-00:00:00"},
            new String[] {"20150225-17:51:32"},
            new String[] {"00010101-00:00:00.001"},
            new String[] {"20150225-17:51:32.123"},
            new String[] {"99991231-23:59:59.999"}
        );
    }

    public UtcTimestampDecoderValidCasesTest(final String timestamp)
    {
        this.timestamp = timestamp;
    }

    @Test
    public void canParseTimestampWithCorrectLength()
    {
        canParseTimestamp(timestamp.length());
    }

    @Test
    public void canParseTimestampWithLongLength()
    {
        canParseTimestamp(LENGTH_WITH_MILLISECONDS);
    }

    private void canParseTimestamp(final int length)
    {
        final long expectedEpochMillis = toEpochMillis(timestamp);

        final byte[] bytes = timestamp.getBytes(US_ASCII);
        final MutableAsciiBuffer buffer = new MutableAsciiBuffer(new byte[LENGTH_WITH_MICROSECONDS + 2]);
        buffer.putBytes(1, bytes);

        final long epochMillis = UtcTimestampDecoder.decode(buffer, 1, length);
        assertEquals("Failed Millis testcase for: " + timestamp, expectedEpochMillis, epochMillis);

        long expectedEpochMicros = expectedEpochMillis * MICROS_IN_MILLIS;

        // If they've got the suffix field, then test microseconds, add 1 to the value
        if (timestamp.length() == LENGTH_WITH_MILLISECONDS)
        {
            expectedEpochMicros++;
            buffer.putAscii(timestamp.length() + 1, "001");
        }

        final long epochMicros = UtcTimestampDecoder.decodeMicros(buffer, 1, length + 3);
        assertEquals("Failed Micros testcase for: " + timestamp, expectedEpochMicros, epochMicros);
    }

    // TODO: test leap second conversion 60
}
