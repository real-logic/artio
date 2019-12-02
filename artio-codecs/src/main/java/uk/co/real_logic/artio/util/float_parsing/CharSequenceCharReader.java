package uk.co.real_logic.artio.util.float_parsing;

public final class CharSequenceCharReader implements CharReader<CharSequence>
{

    private static final char ZERO = '0';

    public static final CharSequenceCharReader INSTANCE = new CharSequenceCharReader();

    private CharSequenceCharReader()
    {

    }

    @Override
    public boolean isSpace(final CharSequence data, final int index)
    {
        return Character.isSpaceChar(data.charAt(index));
    }

    @Override
    public char charAt(final CharSequence data, final int index)
    {
        return data.charAt(index);
    }

    @Override
    public CharSequence asString(final CharSequence data, final int offset, final int length)
    {
        return data.subSequence(offset, offset + length);
    }

    @Override
    public boolean isZero(final CharSequence data, final int index)
    {
        return data.charAt(index) == ZERO;
    }

    @Override
    public int getDigit(final CharSequence data, final int index, final char charValue)
    {
        if (charValue < '0' || charValue > '9')
        {
            throw new NumberFormatException("'" + charValue + "' isn't a valid digit @ " + index);
        }

        return charValue - '0';
    }
}
