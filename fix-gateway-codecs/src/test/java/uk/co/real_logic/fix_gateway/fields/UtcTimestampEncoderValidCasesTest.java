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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import uk.co.real_logic.fix_gateway.util.MutableAsciiBuffer;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static uk.co.real_logic.fix_gateway.fields.UtcTimestampDecoderValidCasesTest.toEpochMillis;
import static uk.co.real_logic.fix_gateway.util.CustomMatchers.sequenceEqualsAscii;

@RunWith(Parameterized.class)
public class UtcTimestampEncoderValidCasesTest
{

    private static final UtcTimestampEncoder encoder = new UtcTimestampEncoder();

    private final String expectedTimestamp;
    private final long epochMillis;
    private final int expectedLength;

    @Parameters
    public static Iterable<Object> data()
    {
        return UtcTimestampDecoderValidCasesTest.data();
    }

    public UtcTimestampEncoderValidCasesTest(final String timestamp)
    {
        this.expectedTimestamp = timestamp;
        epochMillis = toEpochMillis(expectedTimestamp);
        expectedLength = expectedTimestamp.length();
    }

    @Test
    public void canStaticEncodeTimestamp()
    {
        final MutableAsciiBuffer string = new MutableAsciiBuffer(new byte[expectedLength + 2]);

        final int length = UtcTimestampEncoder.encode(epochMillis, string, 1);

        assertEquals("encoded wrong length", expectedLength, length);
        assertThat(string, sequenceEqualsAscii(expectedTimestamp, 1, length));
    }

    @Test
    public void canInstanceEncodeTimestamp()
    {
        final int length = encoder.encode(epochMillis);

        assertEquals("encoded wrong length", expectedLength, length);
        assertEquals(new String(encoder.buffer(), 0, length, US_ASCII), expectedTimestamp);
    }

}
