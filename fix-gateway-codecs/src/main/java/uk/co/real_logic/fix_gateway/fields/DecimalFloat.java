/*
 * Copyright 2015-2017 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.co.real_logic.fix_gateway.fields;

import java.util.Arrays;

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
    public static final DecimalFloat MIN_VALUE = new DecimalFloat(Long.MIN_VALUE, 0);
    public static final DecimalFloat MAX_VALUE = new DecimalFloat(Long.MAX_VALUE, 0);
    public static final DecimalFloat ZERO = new DecimalFloat();
    public static final DecimalFloat MISSING_FLOAT = ZERO;

    private long value;
    private int scale;

    public DecimalFloat()
    {
        this(0, 0);
    }

    public DecimalFloat(final long value, final int scale)
    {
        this.value = value;
        this.scale = scale;
    }

    public void reset()
    {
        value(0);
        scale(0);
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

    public DecimalFloat value(final long value)
    {
        this.value = value;
        return this;
    }

    /**
     * Set the number of digits to the right of the decimal point.
     *
     * @param scale the number of digits to the right of the decimal point.
     * @return this
     */
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

    public String toString()
    {
        String value = String.valueOf(this.value);
        if (scale > 0)
        {
            final boolean isNegative = this.value < 0;
            final int split = value.length() - scale;
            final int splitWithNegative = split - (isNegative ? 1 : 0);
            if (splitWithNegative < 0)
            {
                // We have to add extra zeros between the start or '-' and the First Digit.
                final StringBuilder builder = new StringBuilder();

                if (isNegative)
                {
                    value = String.valueOf(-this.value());
                    builder.append('-');
                }

                final char[] zeros = new char[-splitWithNegative];
                Arrays.fill(zeros, '0');
                return builder
                    .append('.')
                    .append(zeros)
                    .append(value).toString();
            }
            return value.substring(0, split) + "." + value.substring(split);
        }
        else
        {
            return value;
        }
    }

    public int compareTo(final DecimalFloat other)
    {
        final boolean isPositive = value >= 0;
        final int negativeComparison = Boolean.compare(isPositive, other.value >= 0);
        if (negativeComparison != 0)
        {
            return negativeComparison;
        }

        final int scaleComparison = Integer.compare(scale, other.scale);
        return scaleComparison == 0 ?
            Long.compare(value, other.value) :
            !isPositive ? -1 * scaleComparison : scaleComparison;
    }
}
