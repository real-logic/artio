/*
 * Copyright 2015-2023 Real Logic Limited.
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
package uk.co.real_logic.artio.dictionary.generation;

import java.util.Arrays;

import org.agrona.MutableDirectBuffer;


import uk.co.real_logic.artio.fields.ReadOnlyDecimalFloat;

import static java.nio.charset.StandardCharsets.US_ASCII;

public final class CodecUtil
{
    private CodecUtil()
    {
    }

    public static final byte[] BODY_LENGTH = "9=0000\001".getBytes(US_ASCII);

    public static final int MISSING_INT = Integer.MIN_VALUE;
    public static final char MISSING_CHAR = '\001';
    public static final long MISSING_LONG = Long.MIN_VALUE;

    public static final char ENUM_MISSING_CHAR = MISSING_CHAR;
    public static final int ENUM_MISSING_INT = MISSING_INT;
    public static final String ENUM_MISSING_STRING = Character.toString(ENUM_MISSING_CHAR);

    public static final char ENUM_UNKNOWN_CHAR = '\002';
    public static final int ENUM_UNKNOWN_INT = Integer.MAX_VALUE;
    public static final String ENUM_UNKNOWN_STRING = Character.toString(ENUM_UNKNOWN_CHAR);

    private static final char ZERO = '0';
    private static final char DOT = '.';

    // NB: only valid for ASCII bytes.
    @Deprecated // Will be removed in a future version
    public static byte[] toBytes(final CharSequence value, final byte[] oldBuffer)
    {
        final int length = value.length();
        final byte[] buffer = (oldBuffer.length < length) ? new byte[length] : oldBuffer;
        for (int i = 0; i < length; i++)
        {
            buffer[i] = (byte)value.charAt(i);
        }

        return buffer;
    }

    // NB: only valid for ASCII bytes.
    @Deprecated // Will be removed in a future version
    public static byte[] toBytes(final char[] value, final byte[] oldBuffer, final int length)
    {
        return toBytes(value, oldBuffer, 0, length);
    }

    // NB: only valid for ASCII bytes.
    @Deprecated // Will be removed in a future version
    public static byte[] toBytes(final char[] value, final byte[] oldBuffer, final int offset, final int length)
    {
        final byte[] buffer = (oldBuffer.length < length) ? new byte[length] : oldBuffer;
        for (int i = 0; i < length; i++)
        {
            buffer[i] = (byte)value[i + offset];
        }
        return buffer;
    }

    // NB: only valid for ASCII bytes.
    public static byte[] toBytes(final char[] value, final int length)
    {
        final byte[] buffer = new byte[length];
        for (int i = 0; i < length; i++)
        {
            buffer[i] = (byte)value[i];
        }

        return buffer;
    }

    // NB: only valid for ASCII bytes.
    public static char[] fromBytes(final byte[] value)
    {
        final int length = value.length;
        final char[] buffer = new char[length];
        for (int i = 0; i < length; i++)
        {
            buffer[i] = (char)value[i];
        }

        return buffer;
    }

    // NB: only valid for ASCII bytes.
    public static boolean toBytes(final CharSequence value, final MutableDirectBuffer buffer)
    {
        boolean bufferChanged = false;

        final int length = value.length();
        if (buffer.capacity() < length)
        {
            buffer.wrap(new byte[length]);
            bufferChanged = true;
        }

        for (int i = 0; i < length; i++)
        {
            buffer.putByte(i, (byte)value.charAt(i));
        }

        return bufferChanged;
    }

    // NB: only valid for ASCII bytes.
    public static void toBytes(final char[] value, final MutableDirectBuffer oldBuffer, final int length)
    {
        toBytes(value, oldBuffer, 0, length);
    }

    // NB: only valid for ASCII bytes.
    public static boolean toBytes(
        final char[] value, final MutableDirectBuffer buffer, final int offset, final int length)
    {
        boolean bufferChanged = false;

        if (buffer.capacity() < length)
        {
            buffer.wrap(new byte[length]);
            bufferChanged = true;
        }

        for (int i = 0; i < length; i++)
        {
            buffer.putByte(i, (byte)value[i + offset]);
        }

        return bufferChanged;
    }

    public static boolean equals(
        final char[] value,
        final char[] expected,
        final int offset,
        final int expectedOffset,
        final int length)
    {
        if (value.length < offset + length || expected.length < expectedOffset + length)
        {
            return false;
        }

        for (int i = 0; i < length; i++)
        {
            if (value[i + offset] != expected[i + expectedOffset])
            {
                return false;
            }
        }

        return true;
    }

    /**
     * Compares first {@code length} characters of {@code value} with all {@code expected} characters.
     *
     * @param value    the array containing the actual value, might be longer than the value itself
     * @param expected the array containing the expected value, both equal in length
     * @param length   the length of the actual value
     * @return true if the actual value is equal to the expected one
     */
    public static boolean equals(final char[] value, final char[] expected, final int length)
    {
        if (length != expected.length)
        {
            return false;
        }
        return equals(value, expected, 0, 0, length);
    }

    public static boolean equals(final char[] value, final String expected, final int length)
    {
        if (value.length < length || expected.length() != length)
        {
            return false;
        }

        for (int i = 0; i < length; i++)
        {
            if (value[i] != expected.charAt(i))
            {
                return false;
            }
        }

        return true;
    }

    public static int hashCode(final char[] value, final int offset, final int length)
    {
        int result = 1;
        for (int i = offset; i < offset + length; i++)
        {
            result = 31 * result + value[i];
        }

        return result;
    }

    private static final char[] WHITESPACE = "                                                         ".toCharArray();

    public static void indent(final StringBuilder builder, final int level)
    {
        final int numberOfSpaces = 2 * level;
        final char[] whitespace = WHITESPACE;
        if (numberOfSpaces > whitespace.length)
        {
            for (int i = 0; i < level; i++)
            {
                builder.append(whitespace, 0, 2);
            }
        }
        else
        {
            builder.append(whitespace, 0, numberOfSpaces);
        }
    }

    public static void appendData(final StringBuilder builder, final byte[] dataField, final int length)
    {
        for (int i = 0; i < length; i++)
        {
            builder.append((char)dataField[i]);
        }
    }

    public static boolean copyInto(
        final MutableDirectBuffer buffer, final byte[] data, final int offset, final int length)
    {
        final byte[] dest = buffer.byteArray();
        if (dest != null && dest.length >= length)
        {
            System.arraycopy(data, offset, dest, 0, length);
            return false;
        }
        else
        {
            buffer.wrap(Arrays.copyOfRange(data, offset, offset + length));
            return true;
        }
    }

    public static byte[] copyInto(
        final byte[] dest, final byte[] data, final int offset, final int length)
    {
        if (dest != null && dest.length >= length)
        {
            System.arraycopy(data, offset, dest, 0, length);
            return dest;
        }
        else
        {
            return Arrays.copyOfRange(data, offset, offset + length);
        }
    }

    public static void appendBuffer(
        final StringBuilder builder, final MutableDirectBuffer buffer, final int offset, final int length)
    {
        final int end = offset + length;
        for (int i = offset; i < end; i++)
        {
            builder.append((char)buffer.getByte(i));
        }
    }

    public static void appendFloat(final StringBuilder builder, final ReadOnlyDecimalFloat price)
    {
        final long value = price.value();
        final int scale = price.scale();

        final long remainder;
        if (value < 0)
        {
            builder.append('-');
            remainder = -value;
        }
        else
        {
            remainder = value;
        }

        if (scale > 0)
        {
            final int start = builder.length();
            builder.append(remainder);
            final int digitsBeforeDot = builder.length() - start - scale;
            if (digitsBeforeDot <= 0)
            {
                int cursor = start;
                builder.insert(cursor++, ZERO);
                builder.insert(cursor++, DOT);
                final int numberOfZeros = -digitsBeforeDot;
                for (int i = 0; i < numberOfZeros; i++)
                {
                    builder.insert(cursor, ZERO);
                }
            }
            else
            {
                builder.insert(start + digitsBeforeDot, DOT);
            }
        }
        else
        {
            builder.append(remainder);
            final int trailingZeros = -scale;
            if (trailingZeros > 0)
            {
                putTrailingZero(builder, trailingZeros);
            }
        }
    }

    private static void putTrailingZero(final StringBuilder builder, final int zerosCount)
    {
        for (int ix = 0; ix < zerosCount; ix++)
        {
            builder.append('0');
        }
    }
}
