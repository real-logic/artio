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

import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static uk.co.real_logic.artio.fields.CalendricalUtil.MICROS_IN_MILLIS;
import static uk.co.real_logic.artio.fields.UtcTimestampDecoderValidCasesTest.toEpochMillis;
import static uk.co.real_logic.artio.fields.UtcTimestampEncoder.EpochFractionFormat.MICROSECONDS;
import static uk.co.real_logic.artio.util.CustomMatchers.sequenceEqualsAscii;

@RunWith(Parameterized.class)
public class UtcTimestampEncoderValidCasesTest
{

    private final String expectedTimestamp;
    private final String expectedTimestampMicros;
    private final long epochMillis;
    private final long epochMicros;
    private final int expectedLength;
    private final int expectedLengthMicros;

    @Parameters(name = "{0}")
    public static Iterable<String[]> data()
    {
        return UtcTimestampDecoderValidCasesTest.data();
    }

    public UtcTimestampEncoderValidCasesTest(final String timestamp)
    {
        this.expectedTimestamp = timestamp;
        epochMillis = toEpochMillis(expectedTimestamp);
        expectedLength = expectedTimestamp.length();

        if (expectedLength == UtcTimestampEncoder.LENGTH_WITHOUT_MILLISECONDS)
        {
            expectedLengthMicros = expectedLength;
            expectedTimestampMicros = expectedTimestamp;
            epochMicros = epochMillis * MICROS_IN_MILLIS;
        }
        else
        {
            expectedLengthMicros = expectedLength + 3;
            expectedTimestampMicros = expectedTimestamp + "001";
            epochMicros = epochMillis * MICROS_IN_MILLIS + 1;
        }
    }

    @Test
    public void canStaticEncodeTimestampWithOffset()
    {
        final MutableAsciiBuffer string = new MutableAsciiBuffer(new byte[expectedLength + 2]);

        final int length = UtcTimestampEncoder.encode(epochMillis, string, 1);

        assertEquals("encoded wrong length", expectedLength, length);
        assertThat(string, sequenceEqualsAscii(expectedTimestamp, 1, length));
    }

    @Test
    public void canInstanceEncodeTimestamp()
    {
        final UtcTimestampEncoder encoder = new UtcTimestampEncoder();
        final int length = encoder.encode(epochMillis);

        assertEquals("encoded wrong length", expectedLength, length);
        assertEquals(new String(encoder.buffer(), 0, length, US_ASCII), expectedTimestamp);
    }

    @Test
    public void canStaticEncodeTimestampWithOffsetMicros()
    {
        final MutableAsciiBuffer string = new MutableAsciiBuffer(new byte[expectedLengthMicros + 2]);

        final int length = UtcTimestampEncoder.encodeMicros(epochMicros, string, 1);

        assertEquals("encoded wrong length", expectedLengthMicros, length);
        assertThat(string, sequenceEqualsAscii(expectedTimestampMicros, 1, length));
    }

    @Test
    public void canInstanceEncodeTimestampMicros()
    {
        final UtcTimestampEncoder encoder = new UtcTimestampEncoder(MICROSECONDS);
        final int length = encoder.encode(epochMicros);

        assertEquals("encoded wrong length", expectedLengthMicros, length);
        assertEquals(new String(encoder.buffer(), 0, length, US_ASCII), expectedTimestampMicros);
    }

}
