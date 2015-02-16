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

import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.fix_gateway.fields.DecimalFloat;

/**
 * Mutable String class that flyweights a data buffer. This assumes a US-ASCII encoding
 * and should only be used for performance sensitive decoding/encoding tasks.
 */
public class AsciiFlyweight
{
    public static final int UNKNOWN_INDEX = -1;

    private DirectBuffer buffer;

    public AsciiFlyweight()
    {
        this(null);
    }

    public AsciiFlyweight(final DirectBuffer buffer)
    {
        this.buffer = buffer;
    }

    public void wrap(final DirectBuffer buffer)
    {
        this.buffer = buffer;
    }

    public int getInt(final int startInclusive, final int endExclusive)
    {
        int tally = 0;
        for (int index = startInclusive; index < endExclusive; index++)
        {
            tally = (tally * 10) + getDigit(index);
        }

        return tally;
    }

    public int getDigit(final int index)
    {
        final byte value = buffer.getByte(index);
        return getDigit(index, value);
    }

    private int getDigit(final int index, final byte value)
    {
        if (value < 0x30 || value > 0x39)
        {
            throw new IllegalArgumentException("'" + ((char)value) + "' isn't a valid digit @ " + index);
        }

        return value - 0x30;
    }

    public char getChar(final int index)
    {
        return (char)buffer.getByte(index);
    }

    public int scanBack(final int startInclusive, final int endExclusive, final char terminatingCharacter)
    {
        return scanBack(startInclusive, endExclusive, (byte)terminatingCharacter);
    }

    public int scanBack(final int startInclusive, final int endExclusive, final byte terminator)
    {
        for (int index = startInclusive; index >= endExclusive; index--)
        {
            final byte value = buffer.getByte(index);
            if (value == terminator)
            {
                return index;
            }
        }

        return UNKNOWN_INDEX;
    }

    public int scan(final int startInclusive, final int endExclusive, final char terminatingCharacter)
    {
        return scan(startInclusive, endExclusive, (byte)terminatingCharacter);
    }

    public int scan(final int startInclusive, final int endExclusive, final byte terminator)
    {
        int indexValue = UNKNOWN_INDEX;
        for (int i = startInclusive; i <= endExclusive; i++)
        {
            final byte value = buffer.getByte(i);
            //System.out.println(value + " @ " + i);
            if (value == terminator)
            {
                indexValue = i;
                break;
            }
        }

        return indexValue;
    }

    // TODO: improve debug logging
    public void log(final int offset, final int length)
    {
        final byte[] buff = new byte[length];
        buffer.getBytes(offset, buff);
        System.out.println(new String(buff, 0, length));
    }

    public int getMessageType(final int offset, final int length)
    {
        // message types can only be 1 or 2 bytes in size
        int messageType = buffer.getByte(offset);

        if (length == 2)
        {
            messageType |= buffer.getByte(offset + 1) >> 1;
        }

        return messageType;
    }

    public void parseFloat(int offset, int length, final DecimalFloat number)
    {
        // Throw away trailing zeros
        int end = offset + length;
        for (int index = end - 1; isDispensableCharacter(index) && index > offset; index--)
        {
            end--;
        }

        // Is it negative?
        final boolean negative = buffer.getByte(offset) == '-';
        if (negative)
        {
            offset++;
            length--;
        }

        // Throw away leading zeros
        for (int index = offset; isDispensableCharacter(index) && index < end; index++)
        {
            offset++;
        }

        int scale = length;
        long value = 0;
        for (int index = offset; index < end; index++)
        {
            final byte byteValue = buffer.getByte(index);
            if (byteValue == '.')
            {
                scale = index - offset;
            }
            else
            {
                final int digit = getDigit(index);
                value = value * 10 + digit;
            }
        }

        number.value(negative ? -1 * value : value);
        number.scale(scale);
    }

    private boolean isDispensableCharacter(final int index)
    {
        final byte character = buffer.getByte(index);
        return character == '0' || character == ' ';
    }

}
