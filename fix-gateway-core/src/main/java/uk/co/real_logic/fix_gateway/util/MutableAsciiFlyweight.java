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

import java.nio.charset.StandardCharsets;

public final class MutableAsciiFlyweight extends AsciiFlyweight
{
    private static final int ZERO = '0';

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
            // TODO: figure out if there's a cleaner way of doing this
            final int digit = remainder % 10;
            buffer.putByte(index, (byte) (ZERO + digit));
            remainder = remainder / 10;
        }

        if (remainder != 0)
        {
            throw new IllegalArgumentException(String.format("Cannot write %d in %d bytes", value, length));
        }
    }
}
