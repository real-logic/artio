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

    private void assertFormatsTo(final String format, final CharFormatter formatter)
    {
        final StringBuilder builder = new StringBuilder();
        formatter.appendTo(builder);

        assertEquals(format, builder.toString());
    }
}
