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

import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.fix_gateway.fields.DecimalFloat;

import java.nio.charset.StandardCharsets;

public final class MutableAsciiFlyweight extends AsciiFlyweight
{
    public static final int LONGEST_INT_LENGTH = String.valueOf(Integer.MIN_VALUE).length();
    public static final int LONGEST_LONG_LENGTH = String.valueOf(Long.MIN_VALUE).length();
    public static final int LONGEST_FLOAT_LENGTH = LONGEST_LONG_LENGTH + 3;

    private static final byte ZERO = '0';
    private static final byte SEPARATOR = (byte) '\001';
    private static final byte DOT = (byte) '.';

    private static final byte Y = (byte) 'Y';
    private static final byte N = (byte) 'N';

    private final MutableDirectBuffer buffer;

    public MutableAsciiFlyweight(final MutableDirectBuffer buffer)
    {
        super(buffer);
        this.buffer = buffer;
    }

    public int putAscii(final int index, final String string)
    {
        final byte[] bytes = string.getBytes(StandardCharsets.US_ASCII);
        buffer.putBytes(index, bytes);

        return bytes.length;
    }

    public void putSeparator(final int index)
    {
        buffer.putByte(index, SEPARATOR);
    }

    public int putBytes(final int index, final byte[] src)
    {
        buffer.putBytes(index, src);
        return src.length;
    }

    public void putBytes(final int index, final byte[] src, final int srcOffset, final int srcLength)
    {
        buffer.putBytes(index, src, srcOffset, srcLength);
    }

    public int putBoolean(final int offset, final boolean value)
    {
        buffer.putByte(offset, value ? Y : N);
        return 1;
    }

    public void putChar(final int index, final char value)
    {
        buffer.putByte(index, (byte) value);
    }

    public void putNatural(final int offset, final int length, final int value)
    {
        final int end = offset + length;
        int remainder = value;
        for (int index = end - 1; index >= offset; index--)
        {
            final int digit = remainder % 10;
            remainder = remainder / 10;
            buffer.putByte(index, (byte) (ZERO + digit));
        }

        if (remainder != 0)
        {
            throw new IllegalArgumentException(String.format("Cannot write %d in %d bytes", value, length));
        }
    }

    public int putInt(final int offset, final int value)
    {
        if (zero(offset, value))
        {
            return 1;
        }

        int start = offset;
        int remainder = value;
        int length = 0;
        if (value < 0)
        {
            putChar(offset, '-');
            start++;
            length++;
        }
        else
        {
            // Deal with negatives to avoid overflow for Integer.MAX_VALUE
            remainder = -1 * remainder;
        }

        final int end = start + LONGEST_INT_LENGTH;
        int index = end;
        while (remainder < 0)
        {
            final int digit = remainder % 10;
            remainder = remainder / 10;
            buffer.putByte(index, (byte) (ZERO + (-1 * digit)));
            index--;
        }

        length += end - index;
        buffer.putBytes(start, buffer, index + 1, length);
        return length;
    }

    public int putLong(final int offset, final long value)
    {
        if (zero(offset, value))
        {
            return 1;
        }

        final long remainder = calculateRemainder(offset, value);
        final int minusAdj = value < 0 ? 1 : 0;
        final int start = offset + minusAdj;

        final int end = start + LONGEST_LONG_LENGTH;
        final int index = putLong(remainder, end);
        final int length = minusAdj + end - index;
        buffer.putBytes(start, buffer, index + 1, length);
        return length;
    }

    public int putFloat(final int offset, final DecimalFloat price)
    {
        final long value = price.value();
        final int scale = price.scale();
        if (zero(offset, value))
        {
            return 1;
        }

        final long remainder = calculateRemainder(offset, value);
        final int minusAdj = value < 0 ? 1 : 0;
        final int start = offset + minusAdj;

        final int end = start + LONGEST_LONG_LENGTH;
        int index = putLong(remainder, end);
        final int length = minusAdj + end - index;
        index++;
        if (scale < (length - minusAdj))
        {
            final int split = start + scale;
            buffer.putBytes(start, buffer, index, scale);
            buffer.putByte(split, DOT);
            buffer.putBytes(split + 1, buffer, index + scale, length - scale);
            return length + 1;
        }
        else
        {
            buffer.putBytes(start, buffer, index, length);
            return length;
        }
    }

    private boolean zero(final int offset, final long value)
    {
        if (value == 0)
        {
            buffer.putByte(offset, ZERO);
            return true;
        }
        return false;
    }

    private long calculateRemainder(final int offset, final long value)
    {
        if (value < 0)
        {
            putChar(offset, '-');
            return value;
        }
        else
        {
            // Deal with negatives to avoid overflow for Integer.MAX_VALUE
            return -1L * value;
        }
    }

    private int putLong(long remainder, final int end)
    {
        int index = end;
        while (remainder < 0)
        {
            final long digit = remainder % 10;
            remainder = remainder / 10;
            buffer.putByte(index, (byte) (ZERO + (-1L * digit)));
            index--;
        }
        return index;
    }

}
