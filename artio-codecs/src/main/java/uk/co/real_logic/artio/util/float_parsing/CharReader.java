package uk.co.real_logic.artio.util.float_parsing;

public interface CharReader<Data>
{
    boolean isSpace(Data data, int index);

    char charAt(Data data, int index);

    CharSequence asString(Data data, int offset, int length);

    boolean isZero(Data data, int index);

    int getDigit(Data data, int index, char charValue);
}
