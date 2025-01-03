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
package uk.co.real_logic.artio.util;

import org.junit.Test;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.junit.Assert.assertEquals;

public class AsciiFormatterTest
{
    @Test
    public void shouldFormatNoFields()
    {
        final String format = "abc";
        final AsciiFormatter formatter = new AsciiFormatter(format);

        assertFormatsTo(format, formatter);
    }

    @Test
    public void shouldFormatSingleField()
    {
        final String format = "ab%sc";
        final AsciiFormatter formatter = new AsciiFormatter(format)
            .with("D".getBytes(US_ASCII));

        assertFormatsTo("abDc", formatter);
    }

    @Test
    public void shouldFormatTwoFields()
    {
        final String format = "ab%sc%s";
        final AsciiFormatter formatter = new AsciiFormatter(format)
            .with("D".getBytes(US_ASCII))
            .with("E".getBytes(US_ASCII));

        assertFormatsTo("abDcE", formatter);
    }

    @Test
    public void shouldClearFormatter()
    {
        final String format = "ab%sc%s";
        final AsciiFormatter formatter = new AsciiFormatter(format)
            .with("D".getBytes(US_ASCII))
            .with("E".getBytes(US_ASCII))
            .clear()
            .with("F".getBytes(US_ASCII))
            .with("G".getBytes(US_ASCII));

        assertFormatsTo("abFcG", formatter);
    }


    @Test
    public void shouldFormatIntegers()
    {
        final String format = "ab%sc%s";
        final AsciiFormatter formatter = new AsciiFormatter(format)
            .with("D".getBytes(US_ASCII))
            .with(123);

        assertFormatsTo("abDc123", formatter);
    }

    @Test
    public void shouldSupportLongFormatString()
    {
        final String format = "MsgSeqNum too low, expecting %s but received %s";
        final AsciiFormatter formatter = new AsciiFormatter(format)
            .with("0".getBytes(US_ASCII))
            .with("1".getBytes(US_ASCII));

        assertFormatsTo("MsgSeqNum too low, expecting 0 but received 1", formatter);
    }

    private void assertFormatsTo(final String format, final AsciiFormatter formatter)
    {
        assertEquals(format, new String(formatter.value(), 0, formatter.length(), US_ASCII));
    }
}
