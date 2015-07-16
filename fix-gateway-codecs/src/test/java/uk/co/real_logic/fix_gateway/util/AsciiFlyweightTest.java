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

import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.agrona.concurrent.AtomicBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static uk.co.real_logic.fix_gateway.util.AsciiFlyweight.UNKNOWN_INDEX;

public class AsciiFlyweightTest
{
    private static final int OFFSET = 3;
    private static final byte[] BYTES = "8=FIX.4.2A 9=145A ".getBytes(US_ASCII);

    private final AtomicBuffer buffer = new UnsafeBuffer(new byte[1024 * 16]);

    private final AsciiFlyweight string = new AsciiFlyweight(buffer);

    private int value;
    private int second;
    private int third;

    @Before
    public void setUp()
    {
        buffer.putBytes(OFFSET, BYTES);
    }

    @Test
    public void shouldReadDigits()
    {
        value = string.getDigit(OFFSET);

        assertEquals(value, 8);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldValidateDigits()
    {
        string.getDigit(OFFSET + 1);
    }

    @Test
    public void shouldFindCharactersWhenScanningBackwards()
    {
        value = string.scanBack(BYTES.length, 0, '=');

        assertEquals(15, value);
    }

    @Test
    public void shouldNotFindCharactersIfTheyDontExist()
    {
        value = string.scanBack(BYTES.length, 0, 'Z');

        assertEquals(UNKNOWN_INDEX, value);
    }

    @Test
    public void shouldGetIntegerValuesAtSpecifiedOffset()
    {
        value = string.getNatural(16, 19);

        assertEquals(145, value);
    }

    @Test
    public void shouldDecodeSimpleMessageTypes()
    {
        putAscii("0");

        value = string.getMessageType(0, 1);

        assertEquals('0', value);
    }

    @Test
    public void shouldDecodeNewOrderSingleMessageTypes()
    {
        putAscii("D");

        value = string.getMessageType(0, 1);

        assertEquals('D', value);
    }

    @Test
    public void shouldDecodeTwoCharMessageTypes()
    {
        putAscii("AO");

        value = string.getMessageType(0, 2);

        assertEquals(20289, value);
    }

    @Test
    public void shouldGenerateTwoCharMessageTypes()
    {
        putAscii("AM");
        value = string.getMessageType(0, 2);

        putAscii("AL");
        second = string.getMessageType(0, 2);

        putAscii("AQ");
        third = string.getMessageType(0, 2);

        assertNotEquals(value, second);
        assertNotEquals(second, third);
    }

    @Test
    public void shouldDecodeNegativeIntegers()
    {
        putAscii("-1");

        value = string.getInt(0, 2);

        assertEquals(-1, value);
    }

    private void putAscii(final String value)
    {
        buffer.putBytes(0, value.getBytes(US_ASCII));
    }
}
