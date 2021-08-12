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
        // Throw away trailing spaces or zeros
        int workingOffset = offset;
        int end = workingOffset + length;
        for (int index = end - 1; charReader.isSpace(data, index) && index > workingOffset; index--)
        {
            end--;
        }

        int endDiff = 0;
        for (int index = end - 1; charReader.isZero(data, index) && index > workingOffset; index--)
        {
            endDiff++;
        }

        if (isFloatingPoint(charReader, workingOffset, end, endDiff, data))
        {
            end -= endDiff;
        }

        // Throw away leading spaces
        for (int index = workingOffset; charReader.isSpace(data, index) && index < end; index++)
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
        for (int index = workingOffset; index < end && charReader.isZero(data, index); index++)
        {
            workingOffset++;
        }

        int workingScale = 0;
        long value = 0;
        int base10exponent = 0;
        boolean isScientificNotation = false;
        short scaleDecrementValue = 0;
        short scientificExponentMultiplier = -1;
        for (int index = workingOffset; index < end; index++)
        {
            final char charValue = charReader.charAt(data, index);
            if (charValue == DOT)
            {
                // number of digits after the dot
                workingScale = end - (index + 1);
                scaleDecrementValue = 1;
            }
            else if (charValue == LOWER_CASE_E || charValue == UPPER_CASE_E)
            {
                isScientificNotation = true;

                workingScale -= scaleDecrementValue;
            }
            else if (isScientificNotation && charValue == PLUS)
            {
                workingScale -= scaleDecrementValue;
            }
            else if (isScientificNotation && charValue == MINUS)
            {
                workingScale -= scaleDecrementValue;
                scientificExponentMultiplier = 1;
            }
            else
            {
                final int digit = charReader.getDigit(data, index, charValue);
                if (isScientificNotation)
                {
                    base10exponent = base10exponent * 10 + digit;
                    workingScale -= scaleDecrementValue;
                }
                else
                {
                    value = value * 10 + digit;
                    if (value < 0)
                    {
                        throw new ArithmeticException(
                                "Out of range: when parsing " + charReader.asString(data, offset, length));
                    }
                }
            }
        }

        final int scale = workingScale + (scientificExponentMultiplier * base10exponent);
        final long signedValue = negative ? -1 * value : value;
        return number.set(
                (scale >= 0) ? signedValue : signedValue * pow10(-scale),
                Math.max(scale, 0)
        );
    }

    private static <Data> boolean isFloatingPoint(
        final CharReader<Data> dataExtractor,
        final int offset,
        final int end,
        final int endDiff,
        final Data data
    )
    {
        for (int index = end - endDiff - 1; index > offset; index--)
        {
            if (dataExtractor.charAt(data, index) == '.')
            {
                return true;
            }
        }
        return false;
    }
}
