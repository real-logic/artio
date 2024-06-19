package uk.co.real_logic.artio.builder;

import uk.co.real_logic.artio.util.MutableAsciiBuffer;

/**
 * Class provides common implementation methods used by encoders.
 */
public class CommonEncoderImpl
{
    protected MutableAsciiBuffer customTagsBuffer = new MutableAsciiBuffer(new byte[128]);

    public void setCustomTagsBuffer(MutableAsciiBuffer customTagsBuffer)
    {
        this.customTagsBuffer = customTagsBuffer;
    }

    protected int customTagsLength = 0;

    private int putTagHeader(final int tag)
    {
        int pos = customTagsLength;
        pos += customTagsBuffer.putIntAscii(pos, tag);
        customTagsBuffer.putByte(pos++, (byte)'=');
        return pos;
    }

    public CommonEncoderImpl customTag(final int tag, final boolean value)
    {
        int pos = putTagHeader(tag);
        pos += customTagsBuffer.putCharAscii(pos, value ? 'Y' : 'N');
        customTagsBuffer.putSeparator(pos++);
        customTagsLength = pos;
        return this;
    }

    public CommonEncoderImpl customTag(final int tag, final int value)
    {
        int pos = putTagHeader(tag);
        pos += customTagsBuffer.putIntAscii(pos, value);
        customTagsBuffer.putSeparator(pos++);
        customTagsLength = pos;
        return this;
    }

    public CommonEncoderImpl customTag(final int tag, final char value)
    {
        int pos = putTagHeader(tag);
        pos += customTagsBuffer.putCharAscii(pos, value);
        customTagsBuffer.putSeparator(pos++);
        customTagsLength = pos;
        return this;
    }

    public CommonEncoderImpl customTag(final int tag, final long value)
    {
        int pos = putTagHeader(tag);
        pos += customTagsBuffer.putLongAscii(pos, value);
        customTagsBuffer.putSeparator(pos++);
        customTagsLength = pos;
        return this;
    }

    public CommonEncoderImpl customTag(final int tag, final double value)
    {
        int pos = putTagHeader(tag);
        pos += customTagsBuffer.putAscii(pos, String.valueOf(value));
        customTagsBuffer.putSeparator(pos++);
        customTagsLength = pos;
        return this;
    }

    public CommonEncoderImpl customTagAscii(final int tag, final CharSequence value)
    {
        int pos = putTagHeader(tag);
        pos += customTagsBuffer.putStringWithoutLengthAscii(pos, value);
        customTagsBuffer.putSeparator(pos++);
        customTagsLength = pos;
        return this;
    }

    public CommonEncoderImpl customTagAscii(final int tag, final byte[] value)
    {
        int pos = putTagHeader(tag);
        customTagsBuffer.putBytes(pos, value);
        pos += value.length;
        customTagsBuffer.putSeparator(pos++);
        customTagsLength = pos;
        return this;
    }

    public void resetCustomTags()
    {
        customTagsLength = 0;
    }
}
