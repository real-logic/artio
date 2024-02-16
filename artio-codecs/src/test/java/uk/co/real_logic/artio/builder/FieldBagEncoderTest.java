/*
 * Copyright 2024 Adaptive Financial Consulting Limited.
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
package uk.co.real_logic.artio.builder;

import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class FieldBagEncoderTest
{
    private final FieldBagEncoder encoder = new FieldBagEncoder(1);

    @Test
    void testEmpty()
    {
        assertTrue(encoder.isEmpty());
        assertEncodedEquals("");
    }

    @Test
    void testIntFields()
    {
        encoder.addIntField(1, 0);
        assertEncodedEquals("1=0\u0001");
        assertFalse(encoder.isEmpty());

        encoder.addIntField(10, 500);
        assertEncodedEquals("1=0\u000110=500\u0001");
        assertFalse(encoder.isEmpty());
    }

    @Test
    void testLongFields()
    {
        encoder.addLongField(2, 0);
        assertEncodedEquals("2=0\u0001");
        assertFalse(encoder.isEmpty());

        encoder.addLongField(20, 10000000000L);
        assertEncodedEquals("2=0\u000120=10000000000\u0001");
        assertFalse(encoder.isEmpty());
    }

    @Test
    void testAsciiStringFields()
    {
        encoder.addAsciiStringField(3, "foo");
        assertEncodedEquals("3=foo\u0001");
        assertFalse(encoder.isEmpty());

        encoder.addAsciiStringField(30, "lorem ipsum");
        assertEncodedEquals("3=foo\u000130=lorem ipsum\u0001");
        assertFalse(encoder.isEmpty());
    }

    @Test
    void testAsciiCharFields()
    {
        encoder.addAsciiCharField(4, 'Y');
        assertEncodedEquals("4=Y\u0001");
        assertFalse(encoder.isEmpty());

        encoder.addAsciiCharField(40, 'N');
        assertEncodedEquals("4=Y\u000140=N\u0001");
        assertFalse(encoder.isEmpty());
    }

    @Test
    void testReset()
    {
        encoder.addIntField(1, 0);
        encoder.reset();
        assertTrue(encoder.isEmpty());
        assertEncodedEquals("");
    }

    private void assertEncodedEquals(final String expected)
    {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
        final String actual = encodeToString(encoder, buffer, 7);
        assertEquals(expected, actual);

        final StringBuilder builder = new StringBuilder();
        encoder.appendTo(builder);
        assertEquals(expected, builder.toString());

        final FieldBagEncoder copy = new FieldBagEncoder(5);
        encoder.copyTo(copy);
        final String encodedCopy = encodeToString(copy, buffer, 3);
        assertEquals(expected, encodedCopy);
    }

    private String encodeToString(final FieldBagEncoder encoder, final MutableDirectBuffer buffer, final int offset)
    {
        final int length = encoder.encode(buffer, offset);
        return buffer.getStringWithoutLengthAscii(offset, length);
    }
}
