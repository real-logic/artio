/*
 * Copyright 2020 Real Logic Limited., Monotonic Ltd.
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

import org.junit.Test;

import java.util.Arrays;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.junit.Assert.assertEquals;

public class CharFormatterTest
{
    @Test
    public void shouldFormatNoFields()
    {
        final String format = "abc";
        final CharFormatter formatter = new CharFormatter(format);

        assertFormatsTo(format, formatter);
    }

    @Test
    public void shouldFormatSingleField()
    {
        final String format = "ab%sc";
        final CharFormatter formatter = new CharFormatter(format)
            .with("D");

        assertFormatsTo("abDc", formatter);
    }

    @Test
    public void shouldFormatNewlines()
    {
        final String format = "ab%sc%n";
        final CharFormatter formatter = new CharFormatter(format)
            .with("D");

        assertFormatsTo("abDc" + System.lineSeparator(), formatter);
    }

    @Test
    public void shouldFormatByteArraysField()
    {
        final String format = "ab%sc";
        final CharFormatter formatter = new CharFormatter(format)
            .with("D".getBytes(US_ASCII), 1);

        assertFormatsTo("abDc", formatter);
    }

    @Test
    public void shouldFormatTwoFields()
    {
        final String format = "ab%sc%s";
        final CharFormatter formatter = new CharFormatter(format)
            .with("D")
            .with("E");

        assertFormatsTo("abDcE", formatter);
    }

    @Test
    public void shouldClearFormatter()
    {
        final String format = "ab%sc%s";
        final CharFormatter formatter = new CharFormatter(format)
            .with("D")
            .with("E")
            .clear()
            .with("F")
            .with("G");

        assertFormatsTo("abFcG", formatter);
    }


    @Test
    public void shouldFormatIntegers()
    {
        final String format = "ab%sc%s";
        final CharFormatter formatter = new CharFormatter(format)
            .with("D")
            .with(123);

        assertFormatsTo("abDc123", formatter);
    }

    @Test
    public void shouldSupportLongFormatString()
    {
        final String format = "MsgSeqNum too low, expecting %s but received %s";
        final CharFormatter formatter = new CharFormatter(format)
            .with("0")
            .with("1");

        assertFormatsTo("MsgSeqNum too low, expecting 0 but received 1", formatter);
    }

    @Test
    public void shouldEncodeIntToAscii()
    {
        final CharFormatter formatter = new CharFormatter("%s");

        assertPutIntAscii(formatter, 2, 0, "0");
        assertPutIntAscii(formatter, 1, 1, "1");
        assertPutIntAscii(formatter, 8, -21, "-21");
        assertPutIntAscii(formatter, 0, 12345678, "12345678");
        assertPutIntAscii(formatter, 3, Integer.MAX_VALUE, String.valueOf(Integer.MAX_VALUE));
        assertPutIntAscii(formatter, 4, Integer.MIN_VALUE, String.valueOf(Integer.MIN_VALUE));
    }

    @Test
    public void shouldEncodeLongToAscii()
    {
        final CharFormatter formatter = new CharFormatter("%s");

        assertPutLongAscii(formatter, 10, 0, "0");
        assertPutLongAscii(formatter, 0, -42, "-42");
        assertPutLongAscii(formatter, 3, 123, "123");
        assertPutLongAscii(formatter, 5, 999_999_999_999L, "999999999999");
        assertPutLongAscii(formatter, 3, Long.MAX_VALUE, String.valueOf(Long.MAX_VALUE));
        assertPutLongAscii(formatter, 4, Long.MIN_VALUE, String.valueOf(Long.MIN_VALUE));
    }

    private void assertFormatsTo(final String format, final CharFormatter formatter)
    {
        final StringBuilder builder = new StringBuilder();
        formatter.appendTo(builder);

        assertEquals(format, builder.toString());
    }

    private static void assertPutIntAscii(
        final CharFormatter formatter, final int index, final int value, final String encodedValue)
    {
        final char[] buffer = new char[16];
        final int length = formatter.putIntAscii(buffer, index, value);
        assertEquals(encodedValue.length(), length);
        assertEquals(encodedValue, new String(Arrays.copyOfRange(buffer, index, index + length)));
    }

    private static void assertPutLongAscii(
        final CharFormatter formatter, final int index, final long value, final String encodedValue)
    {
        final char[] buffer = new char[32];
        final int length = formatter.putLongAscii(buffer, index, value);
        assertEquals(encodedValue.length(), length);
        assertEquals(encodedValue, new String(Arrays.copyOfRange(buffer, index, index + length)));
    }
}
