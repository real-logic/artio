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

/**
 * Mutable String class that flyweights a data buffer. This assumes a US-ASCII encoding
 * and should only be used for performance sensitive decoding/encoding tasks.
 */
// TODO: add ability to wrap
public class StringFlyweight
{
    public static final int UNKNOWN_INDEX = -1;

    private final MutableDirectBuffer buffer;

    public StringFlyweight(final MutableDirectBuffer buffer)
    {
        this.buffer = buffer;
    }

    public int getInt(final int startInclusive, final int endExclusive)
    {
        int tally = 0;
        for (int index = startInclusive; index < endExclusive; index++)
        {
            tally = tally * 10 + toDigit(buffer.getByte(index));
        }
        return tally;
    }

    public int getDigit(final int index)
    {
        return toDigit(buffer.getByte(index));
    }

    private static int toDigit(final byte value)
    {
        if (value < 0x30 || value > 0x39)
        {
            throw new IllegalArgumentException("'" + ((char)value) + "' isn't a valid digit");
        }

        return value - 0x30;
    }

    public char getChar(final int index)
    {
        return (char) buffer.getByte(index);
    }

    public int scanBack(final int startInclusive, final int endExclusive, final char terminatingCharacter)
    {
        return scanBack(startInclusive, endExclusive, (byte) terminatingCharacter);
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
        return scan(startInclusive, endExclusive, (byte) terminatingCharacter);
    }

    public int scan(final int startInclusive, final int endExclusive, final byte terminator)
    {
        for (int index = startInclusive; index <= endExclusive; index++)
        {
            final byte value = buffer.getByte(index);
            //System.out.println(value + " @ " + index);
            if (value == terminator)
            {
                return index;
            }
        }

        return UNKNOWN_INDEX;
    }

}
