package uk.co.real_logic.artio.util;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

/**
 * View over a {@link DirectBuffer} which contains an ASCII string for a given range.
 */
public class AsciiSequenceView implements CharSequence
{
    private DirectBuffer buffer;
    private int offset;
    private int length;

    public int length()
    {
        return length;
    }

    public char charAt(final int index)
    {
        if (buffer == null || index < 0 || index >= length)
        {
            throw new IndexOutOfBoundsException("AsciiSequenceView index out of range: " + index);
        }
        return (char)buffer.getByte(offset + index);
    }

    public CharSequence subSequence(final int start, final int end)
    {
        throw new UnsupportedOperationException();
    }

    public AsciiSequenceView wrap(final DirectBuffer buffer, final int offset, final int length)
    {
        this.buffer = buffer;
        this.offset = offset;
        this.length = length;
        return this;
    }

    public void reset()
    {
        this.buffer = null;
        this.offset = 0;
        this.length = 0;
    }

    public void getBytes(final MutableDirectBuffer dstBuffer, final int dstOffset)
    {
        dstBuffer.putBytes(dstOffset, this.buffer, offset, length);
    }

    public String toString()
    {
        if (buffer == null)
        {
            return "";
        }
        return buffer.getStringWithoutLengthAscii(offset, length);
    }
}
