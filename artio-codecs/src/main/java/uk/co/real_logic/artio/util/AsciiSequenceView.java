package uk.co.real_logic.artio.util;

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;

public class AsciiSequenceView implements CharSequence
{
    private DirectBuffer buffer;
    private int valueOffset;
    private int valueLength;

    @Override
    public int length()
    {
        return valueLength;
    }

    @Override
    public char charAt(final int index)
    {
        return (char)buffer.getByte(valueOffset + index);
    }

    @Override
    public CharSequence subSequence(final int start, final int end)
    {
        throw new UnsupportedOperationException("Not supported");
    }

    public AsciiSequenceView wrap(final DirectBuffer buffer, final int valueOffset, final int valueLength)
    {
        this.buffer = buffer;
        this.valueOffset = valueOffset;
        this.valueLength = valueLength;
        return this;
    }

    public void reset()
    {
        this.buffer = null;
        this.valueOffset = 0;
        this.valueLength = 0;
    }

    public void getBytes(final MutableDirectBuffer dstBuffer, final int dstOffset)
    {
        dstBuffer.putBytes(dstOffset, this.buffer, valueOffset, valueLength);
    }

    @Override
    public String toString()
    {
        return buffer.getStringWithoutLengthAscii(valueOffset, valueLength);
    }
}
