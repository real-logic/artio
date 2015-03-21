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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.util.AsciiFlyweight;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.time.temporal.ChronoField.MILLI_OF_SECOND;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class UtcTimestampDecoderValidCasesTest
{
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd-HH:mm:ss[.SSS]");

    public static long toEpochMillis(final String timestamp)
    {
        final LocalDateTime parsedDate = LocalDateTime.parse(timestamp, FORMATTER);
        final ZonedDateTime utc = ZonedDateTime.of(parsedDate, ZoneId.of("UTC"));
        return utc.toEpochSecond() * 1000 + utc.getLong(MILLI_OF_SECOND);
    }

    private final String timestamp;

    @Parameters
    public static Iterable<Object> data()
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
    public void canParseTimestamp()
    {
        final long expected = toEpochMillis(timestamp);

        final byte[] bytes = timestamp.getBytes(US_ASCII);
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[bytes.length + 2]);
        buffer.putBytes(1, bytes);
        final AsciiFlyweight timestampBytes = new AsciiFlyweight(buffer);

        final long epochMillis = UtcTimestampDecoder.decode(timestampBytes, 1, timestamp.length());
        assertEquals("Failed testcase for: " + timestamp, expected, epochMillis);
    }

    // TODO: test leap second conversion 60
}
