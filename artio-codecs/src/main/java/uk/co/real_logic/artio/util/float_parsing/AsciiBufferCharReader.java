package uk.co.real_logic.artio.util.float_parsing;

import uk.co.real_logic.artio.util.AsciiBuffer;

public final class AsciiBufferCharReader implements CharReader<AsciiBuffer>
{
    private static final byte SPACE = ' ';
    private static final byte ZERO = '0';

    public static final AsciiBufferCharReader INSTANCE = new AsciiBufferCharReader();

    private AsciiBufferCharReader()
    {

    }

    @Override
    public boolean isSpace(final AsciiBuffer data, final int index)
    {
        return data.getByte(index) == SPACE;
    }

    @Override
    public char charAt(final AsciiBuffer data, final int index)
    {
        return (char)data.getByte(index);
    }

    @Override
    public CharSequence asString(final AsciiBuffer data, final int offset, final int length)
    {
        return data.getAscii(offset, length);
    }

    @Override
    public boolean isZero(final AsciiBuffer data, final int index)
    {
        return data.getByte(index) == ZERO;
    }

    @Override
    public int getDigit(final AsciiBuffer data, final int index, final char charValue)
    {
        if (charValue < 0x30 || charValue > 0x39)
        {
            throw new NumberFormatException("'" + charValue + "' isn't a valid digit @ " + index);
        }

        return charValue - 0x30;
    }
}
