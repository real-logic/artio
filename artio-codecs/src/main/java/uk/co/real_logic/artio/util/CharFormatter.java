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

import java.util.Arrays;
import java.util.regex.Pattern;

import static org.agrona.AsciiEncoding.digitCount;
import static uk.co.real_logic.artio.util.AsciiBuffer.LONGEST_INT_LENGTH;
import static uk.co.real_logic.artio.util.AsciiBuffer.LONGEST_LONG_LENGTH;

/**
 * String formatting class, backed by a char[]. This is used extensively by the logging framework within
 * Artio in order to provide zero-allocation at steady state logging. Related to {@link AsciiFormatter}.
 */
public class CharFormatter
{
    private static final int DEFAULT_LENGTH = LONGEST_LONG_LENGTH;
    private static final Pattern PATTERN = Pattern.compile("%s");
    private static final Pattern NEWLINE = Pattern.compile("%n");
    private static final char[] MIN_INTEGER_VALUE = String.valueOf(Integer.MIN_VALUE).toCharArray();
    private static final char[] MIN_LONG_VALUE = String.valueOf(Long.MIN_VALUE).toCharArray();

    private final String formatString;
    private final char[][] segments;
    private final char[][] values;
    private final int[] lengths;

    private int encodedSoFar = 0;

    public CharFormatter(final String formatString)
    {
        this.formatString = formatString;
        final String[] splitFormatString = PATTERN.split(formatString);
        final int numberOfSegments = splitFormatString.length;
        final int numberOfValues = formatString.endsWith("%s") ? numberOfSegments : numberOfSegments - 1;
        segments = new char[numberOfSegments][];
        lengths = new int[numberOfValues];
        values = new char[numberOfValues][];

        for (int i = 0; i < numberOfSegments; i++)
        {
            final String strSegment = NEWLINE
                .matcher(splitFormatString[i])
                .replaceAll(System.lineSeparator());
            final char[] segment = strSegment.toCharArray();
            segments[i] = segment;
        }

        for (int i = 0; i < numberOfValues; i++)
        {
            values[i] = new char[DEFAULT_LENGTH];
        }
    }

    public CharFormatter with(final String string)
    {
        final int length = string.length();

        final char[] value = ensureLength(length);
        lengths[encodedSoFar] = length;

        string.getChars(0, length, value, 0);

        encodedSoFar++;
        return this;
    }

    public CharFormatter with(final byte[] asciiBytes, final int length)
    {
        final char[] value = ensureLength(length);
        lengths[encodedSoFar] = length;

        for (int i = 0; i < length; i++)
        {
            value[i] = (char)asciiBytes[i];
        }

        encodedSoFar++;
        return this;
    }

    public CharFormatter with(final int number)
    {
        final char[] buffer = ensureLength(LONGEST_INT_LENGTH);
        lengths[encodedSoFar] = putIntAscii(buffer, 0, number);

        encodedSoFar++;
        return this;
    }

    public CharFormatter with(final boolean value)
    {
        final char[] buffer = ensureLength(1);
        buffer[0] = value ? 'Y' : 'N';
        lengths[encodedSoFar] = 1;

        encodedSoFar++;
        return this;
    }

    public CharFormatter with(final long number)
    {
        final char[] buffer = ensureLength(LONGEST_LONG_LENGTH);
        lengths[encodedSoFar] = putLongAscii(buffer, 0, number);

        encodedSoFar++;
        return this;
    }

    public CharFormatter clear()
    {
        encodedSoFar = 0;
        Arrays.fill(lengths, 0);
        return this;
    }

    public void appendTo(final StringBuilder builder)
    {
        final char[][] values = this.values;
        final int length = values.length;
        if (length != encodedSoFar)
        {
            throw new IllegalStateException("Unable to append formatter hasn't been completely initialized");
        }

        final char[][] segments = this.segments;
        final int[] lengths = this.lengths;
        for (int i = 0; i < length; i++)
        {
            builder.append(segments[i]);
            builder.append(values[i], 0, lengths[i]);
        }
        if (segments.length > length)
        {
            builder.append(segments[length]);
        }
    }

    public int putIntAscii(final char[] buffer, final int index, final int value)
    {
        if (value == 0)
        {
            buffer[index] = '0';
            return 1;
        }

        if (value == Integer.MIN_VALUE)
        {
            final int length = MIN_INTEGER_VALUE.length;
            System.arraycopy(MIN_INTEGER_VALUE, 0, buffer, index, length);
            return length;
        }

        int start = index;
        int quotient = value;
        int length = 1;
        if (value < 0)
        {
            buffer[index] = '-';
            start++;
            length++;
            quotient = -quotient;
        }

        int i = digitCount(quotient) - 1;
        length += i;

        while (i >= 0)
        {
            final int remainder = quotient % 10;
            quotient = quotient / 10;
            buffer[i + start] = (char)('0' + remainder);
            i--;
        }

        return length;
    }

    public int putLongAscii(final char[] buffer, final int index, final long value)
    {
        if (value == 0)
        {
            buffer[index] = '0';
            return 1;
        }

        if (value == Long.MIN_VALUE)
        {
            final int length = MIN_LONG_VALUE.length;
            System.arraycopy(MIN_LONG_VALUE, 0, buffer, index, length);
            return length;
        }

        int start = index;
        long quotient = value;
        int length = 1;
        if (value < 0)
        {
            buffer[index] = '-';
            start++;
            length++;
            quotient = -quotient;
        }

        int i = digitCount(quotient) - 1;
        length += i;

        while (i >= 0)
        {
            final long remainder = quotient % 10L;
            quotient = quotient / 10L;
            buffer[i + start] = (char)('0' + remainder);
            i--;
        }

        return length;
    }

    private char[] ensureLength(final int length)
    {
        final int encodedSoFar = this.encodedSoFar;
        final char[][] values = this.values;
        if (encodedSoFar >= values.length)
        {
            throw new IllegalStateException("Attempting to add argument number " +
                (encodedSoFar + 1) + " to a " + values.length + " argument CharFormatter: " + formatString);
        }

        char[] value = values[encodedSoFar];
        if (value.length < length)
        {
            value = new char[length];
            values[encodedSoFar] = value;
        }
        return value;
    }
}
