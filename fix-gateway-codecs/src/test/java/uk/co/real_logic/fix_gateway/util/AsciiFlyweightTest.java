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
import static uk.co.real_logic.fix_gateway.util.AsciiFlyweight.UNKNOWN_INDEX;

public class AsciiFlyweightTest
{
    private static final int OFFSET = 3;
    private static final byte[] BYTES = "8=FIX.4.2A 9=145A ".getBytes(US_ASCII);

    private final AtomicBuffer buffer = new UnsafeBuffer(new byte[1024 * 16]);

    private final AsciiFlyweight string = new AsciiFlyweight(buffer);

    private int value;

    @Before
    public void setUp()
    {
        given:
        buffer.putBytes(OFFSET, BYTES);
    }

    @Test
    public void shouldReadDigits()
    {
        when:
        value = string.getDigit(OFFSET);

        then:
        assertEquals(value, 8);
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldValidateDigits()
    {
        when:
        string.getDigit(OFFSET + 1);
    }

    @Test
    public void shouldFindCharactersWhenScanningBackwards()
    {
        when:
        value = string.scanBack(BYTES.length, 0, '=');

        then:
        assertEquals(15, value);
    }

    @Test
    public void shouldNotFindCharactersIfTheyDontExist()
    {
        when:
        value = string.scanBack(BYTES.length, 0, 'Z');

        then:
        assertEquals(UNKNOWN_INDEX, value);
    }

    @Test
    public void shouldGetIntegerValuesAtSpecifiedOffset()
    {
        when:
        value = string.getInt(16, 19);

        then:
        assertEquals(145, value);
    }

    @Test
    public void shouldDecodeSimpleMessageTypes()
    {
        given:
        putAscii("0");

        when:
        value = string.getMessageType(0, 1);

        then:
        assertEquals('0', value);
    }

    @Test
    public void shouldDecodeTwoCharMessageTypes()
    {
        given:
        putAscii("AO");

        when:
        value = string.getMessageType(0, 2);

        then:
        assertEquals(103, value);
    }

    private void putAscii(final String value)
    {
        buffer.putBytes(0, value.getBytes(US_ASCII));
    }
}
