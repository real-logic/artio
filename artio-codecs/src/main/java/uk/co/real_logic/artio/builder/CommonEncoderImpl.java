package uk.co.real_logic.artio.builder;

import uk.co.real_logic.artio.util.MutableAsciiBuffer;

/**
 * Class provides common implementation methods used by encoders.
 */
public class CommonEncoderImpl
{
    // TODO resizable buffer or way to set a size
    protected MutableAsciiBuffer customTagsBuffer = new MutableAsciiBuffer(new byte[64]);

    protected int customTagsLength = 0;

    private int putTagHeader(final int tag)
    {
        int pos = customTagsLength;
        pos += customTagsBuffer.putIntAscii(pos, tag);
        customTagsBuffer.putByte(pos++, (byte)'=');
        return pos;
    }

    public CommonEncoderImpl customTag(final int tag, final int value)
    {
        int pos = putTagHeader(tag);
        pos += customTagsBuffer.putIntAscii(pos, value);
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

    public void resetCustomTags()
    {
        customTagsLength = 0;
    }
}
