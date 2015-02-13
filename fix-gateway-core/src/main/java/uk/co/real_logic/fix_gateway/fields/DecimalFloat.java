/*
 * Copyright 2015 Real Logic Ltd.
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

/**
 * Fix float data type. Floats are used for a variety of things, including price.
 *
 * Must support 15 significant digits, decimal places variable.
 *
 * See http://fixwiki.org/fixwiki/FloatDataType for details. Examples:
 *
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
 *
 * TODO: document range of valid values
 */
public final class DecimalFloat implements Comparable<DecimalFloat>
{
    private long value;
    private int scale;

    public DecimalFloat()
    {
        this(0,0);
    }

    public DecimalFloat(final long value, final int scale)
    {
        this.value = value;
        this.scale = scale;
    }

    public long value()
    {
        return this.value;
    }

    public int scale()
    {
        return this.scale;
    }

    public DecimalFloat value(final long value)
    {
        this.value = value;
        return this;
    }

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

        final DecimalFloat that = (DecimalFloat) o;
        return scale == that.scale && value == that.value;
    }

    public int hashCode()
    {
        int result = (int) (value ^ (value >>> 32));
        return 31 * result + scale;
    }

    public String toString()
    {
        final String value = String.valueOf(this.value);
        return value.substring(0, scale) + "." + value.substring(scale);
    }

    public int compareTo(final DecimalFloat other)
    {
        final boolean isPositive = value >= 0;
        final int negativeComparison = Boolean.compare(isPositive, other.value >= 0);
        if (negativeComparison != 0)
        {
            return negativeComparison;
        }

        final int scaleComparison = Long.compare(scale, other.scale);
        return (scaleComparison == 0)
             ? Long.compare(value, other.value)
             : !isPositive ? -1 * scaleComparison : scaleComparison;
    }
}
