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

import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.junit.Assert.assertEquals;
import static uk.co.real_logic.artio.fields.CalendricalUtil.MICROS_IN_MILLIS;
import static uk.co.real_logic.artio.fields.CalendricalUtil.NANOS_IN_MILLIS;
import static uk.co.real_logic.artio.fields.UtcTimestampDecoderValidCasesTest.toEpochMillis;
import static uk.co.real_logic.artio.fields.EpochFractionFormat.MICROSECONDS;
import static uk.co.real_logic.artio.fields.EpochFractionFormat.NANOSECONDS;

@RunWith(Parameterized.class)
public class UtcTimestampEncoderUpdateValidCasesTest
{

    private final String expectedTimestamp;
    private final String expectedTimestampMicros;
    private final long epochMillis;
    private final long epochMicros;
    private final long otherEpochMillis;
    private final long otherEpochMicros;
    private final int expectedLength;
    private final int expectedLengthMicros;
    private final boolean validNanoSecondTestCase;
    private final long epochNanos;
    private final long otherEpochNanos;
    private final int expectedLengthNanos;
    private final String expectedTimestampNanos;

    @Parameters(name = "{0}, {1}")
    public static Iterable<Object[]> data()
    {
        return UtcTimestampDecoderValidCasesTest
            .data()
            .stream()
            .flatMap(x -> UtcTimestampDecoderValidCasesTest
                .data()
                .stream()
                .map(y -> new Object[]{x[0], toEpochMillis(y[0].toString()), x[1], y[1]}))
            .collect(Collectors.toList());
    }

    public UtcTimestampEncoderUpdateValidCasesTest(
        final String timestamp,
        final long otherEpochMillis,
        final boolean firstvalidNanoSecondTestCase,
        final boolean secondValidNanoSecondTestCase)
    {
        this.expectedTimestamp = timestamp;
        this.otherEpochMillis = otherEpochMillis;
        validNanoSecondTestCase = firstvalidNanoSecondTestCase && secondValidNanoSecondTestCase;
        epochMillis = toEpochMillis(expectedTimestamp);
        expectedLength = expectedTimestamp.length();
        expectedLengthMicros = expectedLength + 3;
        expectedLengthNanos = expectedLength + 6;
        expectedTimestampMicros = expectedTimestamp + "001";
        expectedTimestampNanos = expectedTimestamp + "000001";
        epochMicros = epochMillis * MICROS_IN_MILLIS + 1;
        epochNanos = epochMillis * NANOS_IN_MILLIS + 1;
        otherEpochMicros = otherEpochMillis * MICROS_IN_MILLIS + 1;
        otherEpochNanos = otherEpochMillis * NANOS_IN_MILLIS + 1;
    }

    @Test
    public void canUpdateTimestamp()
    {
        final UtcTimestampEncoder encoder = new UtcTimestampEncoder();
        encoder.initialise(otherEpochMillis);

        final int length = encoder.update(epochMillis);

        assertEquals("encoded wrong length", expectedLength, length);
        assertEquals(expectedTimestamp, new String(encoder.buffer(), 0, length, US_ASCII));
    }

    @Test
    public void canUpdateTimestampWithoutInitialize()
    {
        final UtcTimestampEncoder encoder = new UtcTimestampEncoder();

        final int length = encoder.update(epochMillis);

        assertEquals("encoded wrong length", expectedLength, length);
        assertEquals(expectedTimestamp, new String(encoder.buffer(), 0, length, US_ASCII));
    }

    @Test
    public void canUpdateTimestampMicros()
    {
        final UtcTimestampEncoder encoder = new UtcTimestampEncoder(MICROSECONDS);
        encoder.initialise(otherEpochMicros);

        final int length = encoder.update(epochMicros);

        assertEquals("encoded wrong length", expectedLengthMicros, length);
        assertEquals(expectedTimestampMicros, new String(encoder.buffer(), 0, length, US_ASCII));
    }

    @Test
    public void canUpdateTimestampMicrosWithoutInitialise()
    {
        final UtcTimestampEncoder encoder = new UtcTimestampEncoder(MICROSECONDS);

        final int length = encoder.update(epochMicros);

        assertEquals("encoded wrong length", expectedLengthMicros, length);
        assertEquals(expectedTimestampMicros, new String(encoder.buffer(), 0, length, US_ASCII));
    }

    @Test
    public void canUpdateTimestampNanos()
    {
        if (validNanoSecondTestCase)
        {
            final UtcTimestampEncoder encoder = new UtcTimestampEncoder(NANOSECONDS);
            encoder.initialise(otherEpochNanos);

            final int length = encoder.update(epochNanos);

            assertEquals("encoded wrong length", expectedLengthNanos, length);
            assertEquals(expectedTimestampNanos, new String(encoder.buffer(), 0, length, US_ASCII));
        }
    }

    @Test
    public void canUpdateTimestampNanosWithoutInitialise()
    {
        if (validNanoSecondTestCase)
        {
            final UtcTimestampEncoder encoder = new UtcTimestampEncoder(NANOSECONDS);

            final int length = encoder.update(epochNanos);

            assertEquals("encoded wrong length", expectedLengthNanos, length);
            assertEquals(expectedTimestampNanos, new String(encoder.buffer(), 0, length, US_ASCII));
        }
    }

}
