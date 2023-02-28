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

import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static uk.co.real_logic.artio.fields.CalendricalUtil.*;
import static uk.co.real_logic.artio.fields.UtcTimestampDecoderValidCasesTest.toEpochMillis;
import static uk.co.real_logic.artio.fields.EpochFractionFormat.*;
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
        this.validNanoSecondTestCase = validNanoSecondTestCase;
        epochMillis = toEpochMillis(timestamp);
        expectedLength = UtcTimestampEncoder.LENGTH_WITH_MILLISECONDS;
        expectedTimestamp = timestamp;
        expectedTimestampMicros = timestamp + "001";
        expectedTimestampNanos = timestamp + "000001";
        expectedLengthMicros = expectedLength + 3;
        expectedLengthNanos = expectedLength + 6;
        epochMicros = epochMillis * MICROS_IN_MILLIS + 1;
        epochNanos = epochMillis * NANOS_IN_MILLIS + 1;
    }

    @Test
    public void canStaticEncodeTimestampWithOffset()
    {
        assertInstanceEncodesTimestampMillisWithOffset(epochMillis, expectedTimestamp, expectedLength);
    }

    @Test
    public void canInstanceEncodeTimestamp()
    {
        assertInstanceEncodesTimestampMillis(epochMillis, expectedTimestamp, expectedLength);
    }

    @Test
    public void canStaticEncodeTimestampWithOffsetMicros()
    {
        assertInstanceEncodesTimestampMicrosWithOffset(epochMicros, expectedTimestampMicros, expectedLengthMicros);
    }

    @Test
    public void canInstanceEncodeTimestampMicros()
    {
        assertInstanceEncodesTimestampMicros(epochMicros, expectedTimestampMicros, expectedLengthMicros);
    }

    @Test
    public void canStaticEncodeTimestampWithOffsetNanos()
    {
        if (validNanoSecondTestCase)
        {
            assertInstanceEncodesTimestampNanosWithOffset(epochNanos, expectedTimestampNanos, expectedLengthNanos);
        }
    }

    @Test
    public void canInstanceEncodeTimestampNanos()
    {
        if (validNanoSecondTestCase)
        {
            assertInstanceEncodesTimestampNanos(epochNanos, expectedTimestampNanos, expectedLengthNanos);
        }
    }

    static void assertInstanceEncodesTimestampMillisWithOffset(
        final long epochMillis, final String expectedTimestamp, final int expectedLength)
    {
        assertInstanceEncodesTimestamp(epochMillis, expectedTimestamp, expectedLength, MILLISECONDS);
    }

    static void assertInstanceEncodesTimestampMillis(
        final long epochMillis, final String expectedTimestamp, final int expectedLength)
    {
        final UtcTimestampEncoder encoder = new UtcTimestampEncoder();
        final int length = encoder.encode(epochMillis);

        assertEquals(expectedTimestamp, new String(encoder.buffer(), 0, length, US_ASCII));
        assertEquals("encoded wrong length", expectedLength, length);
    }

    static void assertInstanceEncodesTimestampMicrosWithOffset(
        final long epochMicros, final String expectedTimestampMicros, final int expectedLength)
    {
        final MutableAsciiBuffer string = new MutableAsciiBuffer(new byte[expectedLength + 2]);

        final int length = UtcTimestampEncoder.encodeMicros(epochMicros, string, 1);

        assertThat(string, sequenceEqualsAscii(expectedTimestampMicros, 1, length));
        assertEquals("encoded wrong length", expectedLength, length);
    }

    static void assertInstanceEncodesTimestampMicros(
        final long epochMicros, final String expectedTimestampMicros, final int expectedLength)
    {
        assertInstanceEncodesTimestamp(epochMicros, expectedTimestampMicros, expectedLength, MICROSECONDS);
    }

    static void assertInstanceEncodesTimestampNanosWithOffset(
        final long epochNanos, final String expectedTimestampNanos, final int expectedLength)
    {
        final MutableAsciiBuffer string = new MutableAsciiBuffer(new byte[expectedLength + 2]);

        final int length = UtcTimestampEncoder.encodeNanos(epochNanos, string, 1);

        assertThat(string, sequenceEqualsAscii(expectedTimestampNanos, 1, length));
        assertEquals("encoded wrong length", expectedLength, length);
    }

    static void assertInstanceEncodesTimestampNanos(
        final long epochNanos, final String expectedTimestampNanos, final int expectedLength)
    {
        assertInstanceEncodesTimestamp(epochNanos, expectedTimestampNanos, expectedLength, NANOSECONDS);
    }

    private static void assertInstanceEncodesTimestamp(
        final long epochFraction,
        final String expectedTimestamp,
        final int expectedLength,
        final EpochFractionFormat epochFractionFormat)
    {
        final UtcTimestampEncoder encoder = new UtcTimestampEncoder(epochFractionFormat);
        final int length = encoder.encode(epochFraction);

        assertEquals(expectedTimestamp, new String(encoder.buffer(), 0, length, US_ASCII));
        assertEquals("encoded wrong length", expectedLength, length);
    }

}
