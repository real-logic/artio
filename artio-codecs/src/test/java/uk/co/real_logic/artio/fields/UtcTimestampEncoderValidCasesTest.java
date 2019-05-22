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
import static uk.co.real_logic.artio.fields.CalendricalUtil.*;
import static uk.co.real_logic.artio.fields.UtcTimestampDecoderValidCasesTest.toEpochMillis;
import static uk.co.real_logic.artio.fields.UtcTimestampEncoder.EpochFractionFormat.MICROSECONDS;
import static uk.co.real_logic.artio.fields.UtcTimestampEncoder.EpochFractionFormat.NANOSECONDS;
import static uk.co.real_logic.artio.util.CustomMatchers.sequenceEqualsAscii;

@RunWith(Parameterized.class)
public class UtcTimestampEncoderValidCasesTest
{

    private final String expectedTimestamp;
    private final boolean validNanoSecondTestCase;
    private final String expectedTimestampMicros;
    private final String expectedTimestampNanos;
    private final long epochMillis;
    private final long epochMicros;
    private final long epochNanos;
    private final int expectedLength;
    private final int expectedLengthMicros;
    private final int expectedLengthNanos;

    @Parameters(name = "{0}")
    public static Iterable<Object[]> data()
    {
        return UtcTimestampDecoderValidCasesTest.data();
    }

    public UtcTimestampEncoderValidCasesTest(final String timestamp, final boolean validNanoSecondTestCase)
    {
        this.expectedTimestamp = timestamp;
        this.validNanoSecondTestCase = validNanoSecondTestCase;
        epochMillis = toEpochMillis(expectedTimestamp);
        expectedLength = expectedTimestamp.length();

        if (expectedLength == UtcTimestampEncoder.LENGTH_WITHOUT_MILLISECONDS)
        {
            expectedLengthMicros = expectedLength;
            expectedLengthNanos = expectedLength;
            expectedTimestampMicros = expectedTimestamp;
            expectedTimestampNanos = expectedTimestamp;
            epochMicros = epochMillis * MICROS_IN_MILLIS;
            epochNanos = epochMillis * NANOS_IN_MILLIS;
        }
        else
        {
            expectedLengthMicros = expectedLength + 3;
            expectedLengthNanos = expectedLength + 6;
            expectedTimestampMicros = expectedTimestamp + "001";
            expectedTimestampNanos = expectedTimestamp + "000001";
            epochMicros = epochMillis * MICROS_IN_MILLIS + 1;
            epochNanos = epochMillis * NANOS_IN_MILLIS + 1;
        }
    }

    @Test
    public void canStaticEncodeTimestampWithOffset()
    {
        final MutableAsciiBuffer string = new MutableAsciiBuffer(new byte[expectedLength + 2]);

        final int length = UtcTimestampEncoder.encode(epochMillis, string, 1);

        assertThat(string, sequenceEqualsAscii(expectedTimestamp, 1, length));
        assertEquals("encoded wrong length", expectedLength, length);
    }

    @Test
    public void canInstanceEncodeTimestamp()
    {
        final UtcTimestampEncoder encoder = new UtcTimestampEncoder();
        final int length = encoder.encode(epochMillis);

        assertEquals(expectedTimestamp, new String(encoder.buffer(), 0, length, US_ASCII));
        assertEquals("encoded wrong length", expectedLength, length);
    }

    @Test
    public void canStaticEncodeTimestampWithOffsetMicros()
    {
        final MutableAsciiBuffer string = new MutableAsciiBuffer(new byte[expectedLengthMicros + 2]);

        final int length = UtcTimestampEncoder.encodeMicros(epochMicros, string, 1);

        assertThat(string, sequenceEqualsAscii(expectedTimestampMicros, 1, length));
        assertEquals("encoded wrong length", expectedLengthMicros, length);
    }

    @Test
    public void canInstanceEncodeTimestampMicros()
    {
        final UtcTimestampEncoder encoder = new UtcTimestampEncoder(MICROSECONDS);
        final int length = encoder.encode(epochMicros);

        assertEquals(expectedTimestampMicros, new String(encoder.buffer(), 0, length, US_ASCII));
        assertEquals("encoded wrong length", expectedLengthMicros, length);
    }

    @Test
    public void canStaticEncodeTimestampWithOffsetNanos()
    {
        if (validNanoSecondTestCase)
        {
            final MutableAsciiBuffer string = new MutableAsciiBuffer(new byte[expectedLengthNanos + 2]);

            final int length = UtcTimestampEncoder.encodeNanos(epochNanos, string, 1);

            assertThat(string, sequenceEqualsAscii(expectedTimestampNanos, 1, length));
            assertEquals("encoded wrong length", expectedLengthNanos, length);
        }
    }

    @Test
    public void canInstanceEncodeTimestampNanos()
    {
        if (validNanoSecondTestCase)
        {
            final UtcTimestampEncoder encoder = new UtcTimestampEncoder(NANOSECONDS);
            final int length = encoder.encode(epochNanos);

            assertEquals(expectedTimestampNanos, new String(encoder.buffer(), 0, length, US_ASCII));
            assertEquals("encoded wrong length", expectedLengthNanos, length);
        }
    }

}
