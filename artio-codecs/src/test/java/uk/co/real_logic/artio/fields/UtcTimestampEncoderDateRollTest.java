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

import static org.junit.Assert.assertEquals;


import static java.nio.charset.StandardCharsets.US_ASCII;
import static uk.co.real_logic.artio.fields.UtcTimestampDecoderValidCasesTest.toEpochMillis;

public class UtcTimestampEncoderDateRollTest
{

    @Test
    public void shouldHandleDateRoll()
    {
        final UtcTimestampEncoder encoder = new UtcTimestampEncoder();
        encoder.initialise(toEpochMillis("20150914-12:34:56.789"));

        final String newTimestamp = "20150915-00:01:23.456";
        final int length = encoder.update(toEpochMillis(newTimestamp));

        assertEquals("encoded wrong length", newTimestamp.length(), length);
        assertEquals(newTimestamp, new String(encoder.buffer(), 0, length, US_ASCII));
    }

    @Test
    public void shouldHandleDateRollOnMidnight()
    {
        final UtcTimestampEncoder encoder = new UtcTimestampEncoder();
        encoder.initialise(toEpochMillis("20150914-12:34:56.789"));

        final String newTimestamp = "20150915-00:00:00.000";
        final int length = encoder.update(toEpochMillis(newTimestamp));

        assertEquals("encoded wrong length", newTimestamp.length(), length);
        assertEquals(newTimestamp, new String(encoder.buffer(), 0, length, US_ASCII));
    }

}
