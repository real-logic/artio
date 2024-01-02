/*
 * Copyright 2015-2024 Real Logic Limited.
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
package uk.co.real_logic.artio.util;

import org.junit.Before;
import org.junit.Test;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static uk.co.real_logic.artio.util.AsciiBuffer.UNKNOWN_INDEX;

public class AsciiBufferTest
{
    private static final int OFFSET = 3;
    private static final byte[] BYTES = "8=FIX.4.2A 9=145A ".getBytes(US_ASCII);

    private final MutableAsciiBuffer buffer = new MutableAsciiBuffer(new byte[1024 * 16]);

    private int value;

    @Before
    public void setUp()
    {
        buffer.putBytes(OFFSET, BYTES);
    }

    @Test
    public void shouldReadDigits()
    {
        value = buffer.getDigit(OFFSET);

        assertEquals(value, 8);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldValidateDigits()
    {
        buffer.getDigit(OFFSET + 1);
    }

    @Test
    public void shouldFindCharactersWhenScanningBackwards()
    {
        value = buffer.scanBack(BYTES.length, 0, '=');

        assertEquals(15, value);
    }

    @Test
    public void shouldNotFindCharactersIfTheyDontExist()
    {
        value = buffer.scanBack(BYTES.length, 0, 'Z');

        assertEquals(UNKNOWN_INDEX, value);
    }

    @Test
    public void shouldGetIntegerValuesAtSpecifiedOffset()
    {
        value = buffer.getNatural(16, 19);

        assertEquals(145, value);
    }

    @Test
    public void shouldGetLongValuesAtSpecifiedOffset()
    {
        final long value = buffer.getNaturalLong(16, 19);

        assertEquals(145L, value);
    }

    @Test
    public void shouldDecodeSimpleMessageTypes()
    {
        putAscii("0");

        final long value = buffer.getMessageType(0, 1);

        assertEquals('0', value);
    }

    @Test
    public void shouldDecodeNewOrderSingleMessageTypes()
    {
        putAscii("D");

        final long value = buffer.getMessageType(0, 1);

        assertEquals('D', value);
    }

    @Test
    public void shouldDecodeTwoCharMessageTypes()
    {
        putAscii("AO");

        final long value = buffer.getMessageType(0, 2);

        assertEquals(20289, value);
    }

    @Test
    public void shouldGenerateTwoCharMessageTypes()
    {
        putAscii("AM");
        final long value = buffer.getMessageType(0, 2);

        putAscii("AL");
        final long second = buffer.getMessageType(0, 2);

        putAscii("AQ");
        final long third = buffer.getMessageType(0, 2);

        assertNotEquals(value, second);
        assertNotEquals(second, third);
    }

    @Test
    public void shouldDecodeNegativeIntegers()
    {
        putAscii("-1");

        value = buffer.getInt(0, 2);

        assertEquals(-1, value);
    }

    private void putAscii(final String value)
    {
        buffer.putBytes(0, value.getBytes(US_ASCII));
    }
}
