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
package uk.co.real_logic.fix_gateway.fields;

import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.fix_gateway.util.AsciiFlyweight;

import static java.nio.charset.StandardCharsets.US_ASCII;

/**
 * .
 */
public class AsciiFieldFlyweight
{
    protected final AsciiFlyweight ascii = new AsciiFlyweight();
    protected DirectBuffer buffer;
    protected int offset;
    protected int length;

    public void wrap(final DirectBuffer buffer, final int offset, final int length)
    {
        this.buffer = buffer;
        this.offset = offset;
        this.length = length;
        ascii.wrap(buffer);
    }

    public String toString()
    {
        return new String(toByteArray(), US_ASCII);
    }

    public char[] toCharArray()
    {
        final char[] characters = new char[length];
        for (int i = 0; i < length; i++)
        {
            characters[i] = ascii.getChar(i + offset);
        }
        return characters;
    }

    public byte[] toByteArray()
    {
        final byte[] bytes = new byte[length];
        buffer.getBytes(offset, bytes);
        return bytes;
    }

    public void getBytes(MutableDirectBuffer dest, int offset)
    {
        dest.putBytes(offset, buffer, this.offset, this.length);
    }
}
