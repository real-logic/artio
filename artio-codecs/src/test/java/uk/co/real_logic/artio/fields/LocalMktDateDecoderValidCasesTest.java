/*
 * Copyright 2015-2025 Real Logic Limited.
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

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.util.Arrays;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class LocalMktDateDecoderValidCasesTest
{
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyyMMdd");

    static int toLocalDay(final String timestamp)
    {
        final LocalDate parsedDate = LocalDate.parse(timestamp, FORMATTER);
        return (int)parsedDate.getLong(ChronoField.EPOCH_DAY);
    }

    private final String timestamp;

    @Parameters(name = "{0}")
    public static Iterable<Object> data()
    {
        return Arrays.asList(
            new String[]{ "00010101" },
            new String[]{ "20150225" },
            new String[]{ "00010101" },
            new String[]{ "20150225" },
            new String[]{ "99991231" }
        );
    }

    public LocalMktDateDecoderValidCasesTest(final String timestamp)
    {
        this.timestamp = timestamp;
    }

    @Test
    public void canParseTimestamp()
    {
        final int expected = toLocalDay(timestamp);

        final LocalMktDateDecoder decoder = new LocalMktDateDecoder();
        final int epochDay = decoder.decode(timestamp.getBytes(US_ASCII));
        assertEquals("Failed testcase for: " + timestamp, expected, epochDay);
    }
}
