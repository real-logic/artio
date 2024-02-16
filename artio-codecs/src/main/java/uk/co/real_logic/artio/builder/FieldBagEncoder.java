/*
 * Copyright 2024 Adaptive Financial Consulting Limited.
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
package uk.co.real_logic.artio.builder;

import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;
import uk.co.real_logic.artio.dictionary.generation.CodecUtil;

/**
 * A collection of fields which can be added to or cleared. Fields can't be accessed individually, adding the same
 * tag multiple times appends a new field each time. It does not perform any tag or value validation, it's the user's
 * responsibility.
 */
public final class FieldBagEncoder
{
    private final MutableDirectBuffer buffer;
    private int position;

    public FieldBagEncoder()
    {
        this(64);
    }

    FieldBagEncoder(final int initialCapacity)
    {
        buffer = new ExpandableArrayBuffer(initialCapacity);
    }

    public void reset()
    {
        position = 0;
    }

    public boolean isEmpty()
    {
        return position == 0;
    }

    public void addIntField(final int tag, final int value)
    {
        beginField(tag);
        position += buffer.putIntAscii(position, value);
        endField();
    }

    public void addLongField(final int tag, final long value)
    {
        beginField(tag);
        position += buffer.putLongAscii(position, value);
        endField();
    }

    public void addAsciiCharField(final int tag, final char value)
    {
        beginField(tag);
        putAsciiChar(value);
        endField();
    }

    public void addAsciiStringField(final int tag, final CharSequence value)
    {
        beginField(tag);
        position += buffer.putStringWithoutLengthAscii(position, value);
        endField();
    }

    public int encode(final MutableDirectBuffer dstBuffer, final int dstOffset)
    {
        dstBuffer.putBytes(dstOffset, buffer, 0, position);
        return position;
    }

    public void appendTo(final StringBuilder builder)
    {
        CodecUtil.appendBuffer(builder, buffer, 0, position);
    }

    public void copyTo(final FieldBagEncoder dst)
    {
        dst.position = position;
        dst.buffer.putBytes(0, buffer, 0, position);
    }

    private void beginField(final int tag)
    {
        position += buffer.putNaturalIntAscii(position, tag);
        putAsciiChar('=');
    }

    private void endField()
    {
        putAsciiChar('\1');
    }

    private void putAsciiChar(final char value)
    {
        buffer.putByte(position++, (byte)value);
    }
}
