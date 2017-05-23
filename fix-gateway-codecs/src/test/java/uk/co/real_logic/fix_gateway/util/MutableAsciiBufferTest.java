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
package uk.co.real_logic.fix_gateway.util;

import org.junit.Test;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static uk.co.real_logic.fix_gateway.util.CustomMatchers.sequenceEqualsAscii;

@RunWith(Theories.class)
public class MutableAsciiBufferTest
{

    private MutableAsciiBuffer string = new MutableAsciiBuffer(new byte[8 * 1024]);

    @Test
    public void shouldWriteIntZero()
    {
        final int length = string.putAsciiInt(1, 0);

        assertEquals(1, length);
        assertThat(string, sequenceEqualsAscii("0", 1, 1));
    }

    @Test
    public void shouldWritePositiveIntValues()
    {
        final int length = string.putAsciiInt(1, 123);

        assertEquals(3, length);
        assertThat(string, sequenceEqualsAscii("123", 1, 3));
    }

    @Test
    public void shouldWriteNegativeIntValues()
    {
        final int length = string.putAsciiInt(1, -123);

        assertEquals(4, length);
        assertThat(string, sequenceEqualsAscii("-123", 1, 4));
    }

    @Test
    public void shouldWriteMaxIntValue()
    {
        final int length = string.putAsciiInt(1, Integer.MAX_VALUE);

        assertThat(string, sequenceEqualsAscii(String.valueOf(Integer.MAX_VALUE), 1, length));
    }

    @Test
    public void shouldWriteMinIntValue()
    {
        final int length = string.putAsciiInt(1, Integer.MIN_VALUE);

        assertThat(string, sequenceEqualsAscii(String.valueOf(Integer.MIN_VALUE), 1, length));
    }

    @Test
    public void shouldWriteLongZero()
    {
        final int length = string.putAsciiLong(1, 0L);

        assertEquals(1, length);
        assertThat(string, sequenceEqualsAscii("0", 1, 1));
    }

    @Test
    public void shouldWritePositiveLongValues()
    {
        final int length = string.putAsciiLong(1, 123L);

        assertEquals(3, length);
        assertThat(string, sequenceEqualsAscii("123", 1, 3));
    }

    @Test
    public void shouldWriteNegativeLongValues()
    {
        final int length = string.putAsciiLong(1, -123L);

        assertEquals(4, length);
        assertThat(string, sequenceEqualsAscii("-123", 1, 4));
    }

    @Test
    public void shouldWriteMaxLongValue()
    {
        final int length = string.putAsciiLong(1, Long.MAX_VALUE);

        assertThat(string, sequenceEqualsAscii(String.valueOf(Long.MAX_VALUE), 1, length));
    }

    @Test
    public void shouldWriteMinLongValue()
    {
        final int length = string.putAsciiLong(1, Long.MIN_VALUE);

        assertThat(string, sequenceEqualsAscii(String.valueOf(Long.MIN_VALUE), 1, length));
    }

    @DataPoints
    public static int[][] valuesAndLengths()
    {
        return new int[][]
        {
            {1, 1},
            {10, 2},
            {100, 3},
            {1000, 4},
            {12, 2},
            {123, 3},
            {2345, 4},
            {9, 1},
            {99, 2},
            {999, 3},
            {9999, 4},
        };
    }

    @Theory
    public void shouldCalculateCorrectAsciiLength(final int[] valueAndLength)
    {
        final int value = valueAndLength[0];
        final int length = valueAndLength[1];
        assertEquals("Wrong length for " + value, length, MutableAsciiBuffer.lengthInAscii(value));
    }

    @Theory
    public void shouldPutNaturalFromEnd(final int[] valueAndLength)
    {
        final int value = valueAndLength[0];
        final int length = valueAndLength[1];

        final int start = string.putNaturalFromEnd(value, length);
        assertEquals("for " + Arrays.toString(valueAndLength), 0, start);

        assertEquals(
            "for " + Arrays.toString(valueAndLength),
            String.valueOf(value),
            string.getAscii(0, length));
    }

}
