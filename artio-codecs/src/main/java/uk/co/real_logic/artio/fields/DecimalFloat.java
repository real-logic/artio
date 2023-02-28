/*
 * Copyright 2015-2023 Real Logic Limited., Adaptive Financial Consulting Ltd.
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
public final class DecimalFloat extends ReadOnlyDecimalFloat
{
    private static final int SCALE_NAN_VALUE = -128;
    private static final long VALUE_NAN_VALUE = Long.MIN_VALUE;

    private static final long VALUE_MAX_VAL = 999_999_999_999_999_999L;
    private static final double VALUE_MAX_VAL_AS_DOUBLE = VALUE_MAX_VAL;
    private static final long VALUE_MIN_VAL = -VALUE_MAX_VAL;
    private static final double VALUE_MIN_VAL_AS_DOUBLE = VALUE_MIN_VAL;
    private static final int SCALE_MAX_VAL = 127;

    // FRACTION_LOWER_THRESHOLD and FRACTION_UPPER_THRESHOLD are used when converting
    // the fractional part of a double to ensure that discretisation errors are corrected.
    // E.g. The actual number 0.123 might be held in binary as something like 0.1230000000000000001
    //      and actual number 0.456 might be held in binary as 0.4559999999999999999999
    // The while-loop and if-statement below using the *_THRESHOLD constants will ensure the
    // correct number (0.123 and 0.456) is represented in the DecimalFloat.
    private static final double FRACTION_LOWER_THRESHOLD = 1e-7;
    private static final double FRACTION_UPPER_THRESHOLD = 1.0 - FRACTION_LOWER_THRESHOLD;

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
        super(value, scale);
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

    public DecimalFloat set(final ReadOnlyDecimalFloat other)
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

    public DecimalFloat negate()
    {
        this.value *= -1;
        return this;
    }

    public DecimalFloat copy()
    {
        return new DecimalFloat(value, scale);
    }

    /**
     * @return a copy that is guaranteed not to change value
     */
    public ReadOnlyDecimalFloat immutableCopy()
    {
        return isNaNValue() ? NAN : new ReadOnlyDecimalFloat(value, scale);
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

    public static DecimalFloat newNaNValue()
    {
        return ReadOnlyDecimalFloat.NAN.mutableCopy();
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
