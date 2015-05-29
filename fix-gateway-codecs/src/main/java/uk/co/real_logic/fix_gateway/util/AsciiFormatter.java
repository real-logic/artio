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

import java.util.regex.Pattern;

import static java.nio.charset.StandardCharsets.US_ASCII;

/**
 * String formatting class with low garbage creation.
 */
public class AsciiFormatter
{
    private static final int DEFAULT_LENGTH = 10;
    private static final Pattern PATTERN = Pattern.compile("%s");

    private final byte[][] segments;

    private byte[] value = new byte[DEFAULT_LENGTH];
    private int index = 0;
    private int encodedSoFar = 0;

    public AsciiFormatter(final CharSequence formatString)
    {
        final String[] splitFormatString = PATTERN.split(formatString);
        final int numberOfSegments = splitFormatString.length;
        segments = new byte[numberOfSegments][];
        for (int i = 0; i < numberOfSegments; i++)
        {
            segments[i] = splitFormatString[i].getBytes(US_ASCII);
        }
        append(segments[0]);
    }

    public AsciiFormatter with(final byte[] field)
    {
        append(field);
        encodedSoFar++;
        if (encodedSoFar < segments.length)
        {
            append(segments[encodedSoFar]);
        }
        return this;
    }

    public AsciiFormatter clear()
    {
        encodedSoFar = 0;
        index = 0;
        append(segments[0]);
        return this;
    }

    public int length()
    {
        return index;
    }

    public byte[] value()
    {
        return value;
    }

    private void append(byte[] toAppend)
    {
        byte[] value = this.value;
        int index = this.index;

        final int requiredLength = index + toAppend.length;
        value = (value.length < index) ? new byte[index] : value;

        System.arraycopy(toAppend, 0, value, index, toAppend.length);
        index += toAppend.length;

        this.index = index;
        this.value = value;
    }
}
