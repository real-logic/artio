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
package uk.co.real_logic.fix_gateway.util;

import org.junit.Test;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static uk.co.real_logic.fix_gateway.util.CustomMatchers.asciiString;

public class MutableAsciiFlyweightTest
{

    private UnsafeBuffer buffer = new UnsafeBuffer(new byte[8 * 1024]);
    private MutableAsciiFlyweight string = new MutableAsciiFlyweight(buffer);

    @Test
    public void shouldWriteIntZero()
    {
        final int length = string.putInt(0, 0);

        assertEquals(1, length);
        assertThat(string, asciiString("0", 0, 1));
    }

    @Test
    public void shouldWritePositiveIntValues()
    {
        final int length = string.putInt(0, 123);

        assertEquals(3, length);
        assertThat(string, asciiString("123", 0, 3));
    }

    @Test
    public void shouldWriteNegativeIntValues()
    {
        final int length = string.putInt(0, -123);

        assertEquals(4, length);
        assertThat(string, asciiString("-123", 0, 4));
    }

    @Test
    public void shouldWriteMaxIntValue()
    {
        final int length = string.putInt(0, Integer.MAX_VALUE);

        assertThat(string, asciiString(String.valueOf(Integer.MAX_VALUE), 0, length));
    }

    @Test
    public void shouldWriteMinIntValue()
    {
        final int length = string.putInt(0, Integer.MIN_VALUE);

        assertThat(string, asciiString(String.valueOf(Integer.MIN_VALUE), 0, length));
    }

    @Test
    public void shouldWriteLongZero()
    {
        final int length = string.putLong(0, 0L);

        assertEquals(1, length);
        assertThat(string, asciiString("0", 0, 1));
    }

    @Test
    public void shouldWritePositiveLongValues()
    {
        final int length = string.putLong(0, 123L);

        assertEquals(3, length);
        assertThat(string, asciiString("123", 0, 3));
    }

    @Test
    public void shouldWriteNegativeLongValues()
    {
        final int length = string.putLong(0, -123L);

        assertEquals(4, length);
        assertThat(string, asciiString("-123", 0, 4));
    }

    @Test
    public void shouldWriteMaxLongValue()
    {
        final int length = string.putLong(0, Long.MAX_VALUE);

        assertThat(string, asciiString(String.valueOf(Long.MAX_VALUE), 0, length));
    }

    @Test
    public void shouldWriteMinLongValue()
    {
        final int length = string.putLong(0, Long.MIN_VALUE);

        assertThat(string, asciiString(String.valueOf(Long.MIN_VALUE), 0, length));
    }

}
