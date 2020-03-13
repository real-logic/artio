/*
 * Copyright 2015-2020 Real Logic Limited., Adaptive Financial Consulting Ltd.
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
package uk.co.real_logic.artio.fields;

import uk.co.real_logic.artio.dictionary.generation.CodecUtil;
import uk.co.real_logic.artio.util.PowerOf10;
import uk.co.real_logic.artio.util.float_parsing.CharSequenceCharReader;
import uk.co.real_logic.artio.util.float_parsing.DecimalFloatParser;

import static uk.co.real_logic.artio.util.PowerOf10.HIGHEST_POWER_OF_TEN;
import static uk.co.real_logic.artio.util.PowerOf10.POWERS_OF_TEN;

/**
 * Fix float data type. Floats are used for a variety of things, including price.
 * <p>
 * Must support 15 significant digits, decimal places variable.
 * <p>
 * Decimal float represents the significant digits using a value field,
 * and the location of the point using the scale field.
 * <p>
 * See http://fixwiki.org/fixwiki/FloatDataType for details. Examples:
 * <p>
 * 55.36
 * 55.3600
 * 0055.36
 * 0055.3600
 * .995
 * 0.9950
 * 25
 * -55.36
 * -0055.3600
 * -55.3600
 * -.995
 * -0.9950
 * -25
 */
public final class DecimalFloat implements Comparable<DecimalFloat>
{
    private static final int SCALE_NAN_VALUE = -128;
    private static final long VALUE_NAN_VALUE = Long.MIN_VALUE;
    private static final double DOUBLE_NAN_VALUE = Double.NaN;

    private static final long VALUE_MAX_VAL = 999_999_999_999_999_999L;
    private static final double VALUE_MAX_VAL_AS_DOUBLE = VALUE_MAX_VAL;
    private static final long VALUE_MIN_VAL = -VALUE_MAX_VAL;
    private static final double VALUE_MIN_VAL_AS_DOUBLE = VALUE_MIN_VAL;
    private static final int SCALE_MAX_VAL = 127;
    private static final int SCALE_MIN_VAL = 0;

    public static final DecimalFloat MIN_VALUE = new DecimalFloat(VALUE_MIN_VAL, 0);
    public static final DecimalFloat MAX_VALUE = new DecimalFloat(VALUE_MAX_VAL, 0);
    public static final DecimalFloat ZERO = new DecimalFloat();
    public static final DecimalFloat NAN = newNaNValue();
    public static final DecimalFloat MISSING_FLOAT = NAN;

    // FRACTION_LOWER_THRESHOLD and FRACTION_UPPER_THRESHOLD are used when converting
    // the fractional part of a double to ensure that discretisation errors are corrected.
    // E.g. The actual number 0.123 might be held in binary as something like 0.1230000000000000001
    //      and actual number 0.456 might be held in binary as 0.4559999999999999999999
    // The while-loop and if-statement below using the *_THRESHOLD constants will ensure the
    // correct number (0.123 and 0.456) is represented in the DecimalFloat.
    private static final double FRACTION_LOWER_THRESHOLD = 1e-7;
    private static final double FRACTION_UPPER_THRESHOLD = 1.0 - FRACTION_LOWER_THRESHOLD;

    private long value;
    private int scale;

    public DecimalFloat()
    {
        this(0, 0);
    }

    public DecimalFloat(final long value)
    {
        this(value, 0);
    }

    public DecimalFloat(final long value, final int scale)
    {
        setAndNormalise(value, scale);
    }

    /**
     * Resets the encoder to the NAN value. This can checked using the {@link #isNaNValue()} method.
     */
    public void reset()
    {
        this.value = VALUE_NAN_VALUE;
        this.scale = SCALE_NAN_VALUE;

        // Deliberately doesn't normalise, as that validates the arithmetic range.
    }

    public long value()
    {
        return this.value;
    }

    /**
     * Get the number of digits to the right of the decimal point.
     *
     * @return the number of digits to the right of the decimal point.
     */
    public int scale()
    {
        return this.scale;
    }

    public DecimalFloat set(final DecimalFloat other)
    {
        this.value = other.value;
        this.scale = other.scale;
        return this;
    }

    /**
     * Sets the value of the DecimalFloat to the same as a long value.
     *
     * @param value the value to set
     * @return this
     */
    public DecimalFloat fromLong(final long value)
    {
        setAndNormalise(value, 0);
        return this;
    }

    public DecimalFloat set(final long value, final int scale)
    {
        setAndNormalise(value, scale);
        return this;
    }

    /*
     * Please use set(newValue, newScale) instead of value(newValue) and scale(newScale)
     */
    @Deprecated
    public DecimalFloat value(final long value)
    {
        this.value = value;
        return this;
    }

    /*
     * Please use set(newValue, newScale) instead of value(newValue) and scale(newScale)
     */
    @Deprecated
    public DecimalFloat scale(final int scale)
    {
        this.scale = scale;
        return this;
    }

    public boolean equals(final Object o)
    {
        if (this == o)
        {
            return true;
        }

        if (o == null || getClass() != o.getClass())
        {
            return false;
        }

        final DecimalFloat that = (DecimalFloat)o;
        return scale == that.scale && value == that.value;
    }

    public int hashCode()
    {
        final int result = (int)(value ^ (value >>> 32));
        return 31 * result + scale;
    }

    public void appendTo(final StringBuilder builder)
    {
        CodecUtil.appendFloat(builder, this);
    }

    public String toString()
    {
        final StringBuilder builder = new StringBuilder();
        appendTo(builder);
        return builder.toString();
    }

    public DecimalFloat negate()
    {
        this.value *= -1;
        return this;
    }

    public DecimalFloat copy()
    {
        return new DecimalFloat(value, scale);
    }

    public int compareTo(final DecimalFloat other)
    {
        final long value = this.value;
        final int scale = this.scale;

        final long otherValue = other.value;
        final int otherScale = other.scale;

        final long decimalPointDivisor = PowerOf10.pow10(scale);
        final long otherDecimalPointDivisor = PowerOf10.pow10(otherScale);

        final long valueBeforeDecimalPoint = value / decimalPointDivisor;
        final long otherValueBeforeDecimalPoint = otherValue / otherDecimalPointDivisor;

        final int beforeDecimalPointComparison = Long.compare(valueBeforeDecimalPoint, otherValueBeforeDecimalPoint);

        if (beforeDecimalPointComparison != 0)
        {
            // Can be determined using just the long value before decimal point
            return beforeDecimalPointComparison;
        }

        // values after decimal point, but has removed scale entirely
        long valueAfterDecimalPoint = (value % decimalPointDivisor);
        long otherValueAfterDecimalPoint = (otherValue % otherDecimalPointDivisor);

        // re-normalise with scales by multiplying the lower scale number up
        if (scale > otherScale)
        {
            final int differenceInScale = scale - otherScale;
            otherValueAfterDecimalPoint *= PowerOf10.pow10(differenceInScale);
        }
        else
        {
            final int differenceInScale = otherScale - scale;
            valueAfterDecimalPoint *= PowerOf10.pow10(differenceInScale);
        }

        return Long.compare(valueAfterDecimalPoint, otherValueAfterDecimalPoint);
    }

    public double toDouble()
    {
        if (isNaNValue())
        {
            return DOUBLE_NAN_VALUE;
        }
        return toDouble(value, scale);
    }

    public boolean fromDouble(final double doubleValue)
    {
        if (Double.isNaN(doubleValue))
        {
            value = VALUE_NAN_VALUE;
            scale = SCALE_NAN_VALUE;
            return true;
        }
        if (!Double.isFinite(doubleValue) ||
            isOutsideLimits(doubleValue, VALUE_MIN_VAL_AS_DOUBLE, VALUE_MAX_VAL_AS_DOUBLE))
        {
            return false;
        }
        if (doubleValue == 0.0)
        {
            value = 0L;
            scale = 0;
            return true;
        }
        final boolean isNegative = doubleValue < 0.0;
        double remainingValue = Math.abs(doubleValue);
        long newValue = (long)remainingValue;
        // Have to repeat the limit verification, as the test above on the doubleValue is not exact.
        // It cuts the worst offenders and allows us to cast to a long,
        // but it lets through bad values within about 64 of the limits.
        if (isOutsideLimits(newValue, VALUE_MIN_VAL, VALUE_MAX_VAL))
        {
            return false;
        }
        int newScale = 0;
        remainingValue -= newValue;
        if (remainingValue == 0.0)
        {
            setAndNormalise(signedValue(isNegative, newValue), newScale);
            return true;
        }
        while (canValueAcceptMoreDigits(newValue, newScale) &&
            (newValue == 0L ||
            !isOutsideLimits(remainingValue, FRACTION_LOWER_THRESHOLD, FRACTION_UPPER_THRESHOLD)))
        {
            remainingValue *= 10.0;
            final double digit = Math.floor(remainingValue);
            remainingValue -= digit;
            newValue = newValue * 10L + (long)digit;
            ++newScale;
        }
        if (FRACTION_UPPER_THRESHOLD < remainingValue)
        {
            newValue++;
        }
        setAndNormalise(signedValue(isNegative, newValue), newScale);
        return true;
    }

    public DecimalFloat fromString(final CharSequence string)
    {
        return fromString(string, 0, string.length());
    }

    public DecimalFloat fromString(final CharSequence string, final int start, final int length)
    {
        return DecimalFloatParser.extract(this, CharSequenceCharReader.INSTANCE, string, start, length);
    }

    public boolean isNaNValue()
    {
        return isNaNValue(value, scale);
    }

    public static boolean isNaNValue(final long value, final int scale)
    {
        return value == VALUE_NAN_VALUE && scale == SCALE_NAN_VALUE;
    }

    public static DecimalFloat newNaNValue()
    {
        final DecimalFloat nanFloat = new DecimalFloat();
        nanFloat.value = VALUE_NAN_VALUE;
        nanFloat.scale = SCALE_NAN_VALUE;
        return nanFloat;
    }

    private void setAndNormalise(final long value, final int scale)
    {
        this.value = value;
        this.scale = scale;
        normalise();
    }

    private void normalise()
    {
        long value = this.value;
        int scale = this.scale;
        if (value == 0)
        {
            scale = 0;
        }
        else if (0 < scale)
        {
            while (value % 10 == 0 && 0 < scale)
            {
                value /= 10;
                --scale;
            }
        }
        else if (scale < 0)
        {
            while (!isOutsideLimits(value, VALUE_MIN_VAL, VALUE_MAX_VAL) && scale < 0)
            {
                value *= 10;
                ++scale;
            }
        }
        if (isOutsideLimits(scale, SCALE_MIN_VAL, SCALE_MAX_VAL) ||
            isOutsideLimits(value, VALUE_MIN_VAL, VALUE_MAX_VAL))
        {
            throw new ArithmeticException("Out of range: value: " + this.value + ", scale: " + this.scale);
        }
        this.value = value;
        this.scale = scale;
    }

    private static double toDouble(final long value, final int scale)
    {
        int remainingPowersOfTen = scale;
        double divisor = 1.0;
        while (remainingPowersOfTen >= HIGHEST_POWER_OF_TEN)
        {
            divisor *= POWERS_OF_TEN[HIGHEST_POWER_OF_TEN];
            remainingPowersOfTen -= HIGHEST_POWER_OF_TEN;
        }
        divisor *= POWERS_OF_TEN[remainingPowersOfTen];
        return value / divisor;
    }

    private static boolean canValueAcceptMoreDigits(final long value, final int scale)
    {
        return value <= POWERS_OF_TEN[HIGHEST_POWER_OF_TEN - 1] && scale < SCALE_MAX_VAL;
    }

    private static boolean isOutsideLimits(final long value, final long lowerBound, final long upperBound)
    {
        return value < lowerBound || upperBound < value;
    }

    private static boolean isOutsideLimits(final double value, final double lowerBound, final double upperBound)
    {
        return value < lowerBound || upperBound < value;
    }

    private static long signedValue(final boolean isNegative, final long value)
    {
        return isNegative ? -value : value;
    }
}
