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

import org.junit.Ignore;
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
import static org.junit.Assert.assertEquals;

@Ignore
@RunWith(Parameterized.class)
public class UtcTimestampParserTest
{
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd-HH:mm:ss"); // [.sss]

    private final String timestamp;
    private final boolean valid;

    @Parameters
    public static Iterable<Object> data()
    {
        return Arrays.asList(new Object[][]
        {
            {"00010101-00:00:00", true},
            {"20150225-17:51:32", true},
            //{"00010101-00:00:00.000", true},
            //{"20150225-17:51:32.123", true},
            //{"99991231-23:59:60.999", true},
            //{"00000101-00:00:00", false},
        });
    }

    public UtcTimestampParserTest(final String timestamp, final boolean valid)
    {
        this.timestamp = timestamp;
        this.valid = valid;
    }

    @Test
    public void canParseTimestamp()
    {
        final LocalDateTime parsedDate = LocalDateTime.parse(timestamp, FORMATTER);
        final long epochSecond = ZonedDateTime.of(parsedDate, ZoneId.of("UTC")).toEpochSecond();
        final AsciiFlyweight timestampBytes = new AsciiFlyweight(new UnsafeBuffer(timestamp.getBytes(US_ASCII)));
        assertEquals("Failed testcase for: " + timestamp, epochSecond, UtcTimestampParser.parse(timestampBytes, 0));
    }
}
