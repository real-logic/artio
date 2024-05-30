package uk.co.real_logic.artio.util.float_parsing;


import uk.co.real_logic.artio.fields.DecimalFloat;

import static uk.co.real_logic.artio.util.PowerOf10.pow10;

public final class DecimalFloatParser
{
    private static final char LOWER_CASE_E = 'e';
    private static final char UPPER_CASE_E = 'E';
    private static final char PLUS = '+';
    private static final char MINUS = '-';
    private static final byte DOT = '.';

    public static <Data> DecimalFloat extract(
        final DecimalFloat number,
        final CharReader<Data> charReader,
        final Data data,
        final int offset,
        final int length)
    {
        // Throw away trailing spaces
        int workingOffset = offset;
        int end = workingOffset + length;
        for (int index = end - 1; charReader.isSpace(data, index) && index > workingOffset; index--)
        {
            end--;
        }

        int endOfSignificand = findEndOfSignificand(charReader, workingOffset, end, data);
        final int startOfExponent = endOfSignificand + 1;

        if (isFloatingPoint(charReader, workingOffset, endOfSignificand, data))
        {
            // Throw away trailing zeros
            for (int index = endOfSignificand - 1; charReader.isZero(data, index) && index > workingOffset; index--)
            {
                endOfSignificand--;
            }
        }

        // Throw away leading spaces
        for (int index = workingOffset; index < endOfSignificand && charReader.isSpace(data, index); index++)
        {
            workingOffset++;
        }

        // Is it negative?
        final boolean negative = charReader.charAt(data, workingOffset) == MINUS;
        if (negative)
        {
            workingOffset++;
        }

        // Throw away leading zeros
        for (int index = workingOffset; index < endOfSignificand && charReader.isZero(data, index); index++)
        {
            workingOffset++;
        }

        int workingScale = 0;
        long value = 0;
        for (int index = workingOffset; index < endOfSignificand; index++)
        {
            final char charValue = charReader.charAt(data, index);
            if (charValue == DOT)
            {
                // number of digits after the dot
                workingScale = endOfSignificand - (index + 1);
            }
            else
            {
                final int digit = charReader.getDigit(data, index, charValue);
                value = value * 10 + digit;
                if (value < 0)
                {
                    throw new ArithmeticException(
                        "Out of range: when parsing " + charReader.asString(data, offset, length));
                }
            }
        }

        int exponent = 0;
        final int exponentLength = end - startOfExponent;
        if (exponentLength > 0)
        {
            // scientific notation
            exponent = parseExponent(charReader, data, offset, length, startOfExponent, end);
        }
        else if (exponentLength == 0)
        {
            throw new NumberFormatException(charReader.asString(data, offset, length).toString());
        }

        final int scale = workingScale - exponent;
        final long signedValue = negative ? -1 * value : value;
        return number.set(
            (scale >= 0) ? signedValue : signedValue * pow10(-scale),
            Math.max(scale, 0)
        );
    }

    private static <Data> int parseExponent(
        final CharReader<Data> charReader,
        final Data data,
        final int offset,
        final int length,
        final int startOfExponent,
        final int end)
    {
        int exponent = 0;
        boolean negative = false;
        int position = startOfExponent;

        final char firstChar = charReader.charAt(data, position);
        if (firstChar == MINUS)
        {
            position++;
            negative = true;
        }
        else if (firstChar == PLUS)
        {
            position++;
        }

        while (position < end)
        {
            final char charValue = charReader.charAt(data, position);
            final int digit = charReader.getDigit(data, position, charValue);
            position++;
            exponent = exponent * 10 + digit;
            if (exponent > 1000) // overflow and arbitrary limit check
            {
                throw new NumberFormatException(charReader.asString(data, offset, length).toString());
            }
        }

        return negative ? -exponent : exponent;
    }

    private static <Data> int findEndOfSignificand(
        final CharReader<Data> dataExtractor,
        final int offset,
        final int end,
        final Data data
    )
    {
        for (int index = end - 1; index > offset; index--)
        {
            final char charValue = dataExtractor.charAt(data, index);
            if (charValue == LOWER_CASE_E || charValue == UPPER_CASE_E)
            {
                return index;
            }
        }
        return end;
    }

    private static <Data> boolean isFloatingPoint(
        final CharReader<Data> dataExtractor,
        final int offset,
        final int end,
        final Data data
    )
    {
        for (int index = end - 1; index >= offset; index--)
        {
            if (dataExtractor.charAt(data, index) == '.')
            {
                return true;
            }
        }
        return false;
    }
}
