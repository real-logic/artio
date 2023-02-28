/*
 * Copyright 2015-2023 Real Logic Limited.
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

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import uk.co.real_logic.artio.dictionary.generation.CodecUtil;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static uk.co.real_logic.artio.util.MutableAsciiBuffer.LONGEST_FLOAT_LENGTH;

@RunWith(Parameterized.class)
public class DecimalFloatEncodingTest
{
    @Parameters(name = "{index}: {1},{2} => {0}")
    public static Iterable<Object[]> decimalFloatCodecData()
    {
        return Arrays.asList(new Object[][]
        {
            {"55.36", 5536L, 2},
            {"0.995", 995L, 3},
            {"25", 25L, 0},
            {"-55.36", -5536L, 2},
            {"-0.995", -995L, 3},
            {"-25", -25L, 0},
            {"0.6", 6L, 1},
            {"-0.6", -6L, 1},
            {"10", 10L, 0},
            {"-10", -10L, 0},

            {"0.92117125", 92117125L, 8},
            {"-0.92117125", -92117125L, 8},
            {"0.92125", 92125L, 5},
            {"-0.92125", -92125L, 5},
            {"0.00007875", 7875, 8},
            {"-0.00007875", -7875, 8},
            {"1.00109125", 100109125, 8},
            {"-1.00109125", -100109125, 8},

            // negative scale
            {"26000", 26L, -3},
            {"-26000", -26L, -3},

            // edge values
            {"0.000000000000000003", 3, 18},
            {"123456789012345678", 123456789012345678L, 0},
            {"-123456789012345678", -123456789012345678L, 0},
            {"0.123456789012345678", 123456789012345678L, 18},
            {"-0.123456789012345678", -123456789012345678L, 18},
            {"1.23456789012345678", 123456789012345678L, 17},
            {"-1.23456789012345678", -123456789012345678L, 17},

            // zero values
            {"0", 0, 0},
            {"0.000", 0, 3},
            {"0", 0, 4},
            {"0", 0, -5},

            // trailing zeros
            {"12.7460", 127460, 4},
            {"-12.7460", -127460, 4},
            {"0.03400", 3400, 5},
            {"-0.03400", -3400, 5},
            {"400", 40L, -1},
            {"-400", -40L, -1},

            // same positive value, range scale -2 to 19
            {"7400", 74, -2},
            {"740", 74, -1},
            {"74", 74, 0},
            {"7.4", 74, 1},
            {"0.74", 74, 2},
            {"0.074", 74, 3},
            {"0.0074", 74, 4},
            {"0.00074", 74, 5},
            {"0.000074", 74, 6},
            {"0.0000074", 74, 7},
            {"0.00000074", 74, 8},
            {"0.000000074", 74, 9},
            {"0.0000000074", 74, 10},
            {"0.00000000074", 74, 11},
            {"0.000000000074", 74, 12},
            {"0.0000000000074", 74, 13},
            {"0.00000000000074", 74, 14},
            {"0.000000000000074", 74, 15},
            {"0.0000000000000074", 74, 16},
            {"0.00000000000000074", 74, 17},
            {"0.000000000000000074", 74, 18},
            {"0.0000000000000000074", 74, 19},

            // same negative value, range scale -2 to 19
            {"-53700", -537, -2},
            {"-5370", -537, -1},
            {"-537", -537, 0},
            {"-53.7", -537, 1},
            {"-5.37", -537, 2},
            {"-0.537", -537, 3},
            {"-0.0537", -537, 4},
            {"-0.00537", -537, 5},
            {"-0.000537", -537, 6},
            {"-0.0000537", -537, 7},
            {"-0.00000537", -537, 8},
            {"-0.000000537", -537, 9},
            {"-0.0000000537", -537, 10},
            {"-0.00000000537", -537, 11},
            {"-0.000000000537", -537, 12},
            {"-0.0000000000537", -537, 13},
            {"-0.00000000000537", -537, 14},
            {"-0.000000000000537", -537, 15},
            {"-0.0000000000000537", -537, 16},
            {"-0.00000000000000537", -537, 17},
            {"-0.000000000000000537", -537, 18},
            {"-0.0000000000000000537", -537, 19},
        });
    }

    private final String input;
    private final String expectedOutput;
    private final long value;
    private final int scale;

    public DecimalFloatEncodingTest(final String input, final long value, final int scale)
    {
        this.input = input;
        this.expectedOutput = input;
        this.value = value;
        this.scale = scale;
    }

    private boolean isExpectedOutputContainDecimalPoint()
    {
        return expectedOutput.indexOf('.') >= 0;
    }

    private boolean isExpectedOutputContainTrailingZeros()
    {
        if (isExpectedOutputContainDecimalPoint())
        {
            final String trimmed = expectedOutput.trim();
            return (trimmed.charAt(trimmed.length() - 1) == '0');
        }
        return false;
    }

    @Test
    public void canEncodeDecimalFloat()
    {
        // ignoring test since expected output has Trailing Zeros
        if (isExpectedOutputContainTrailingZeros())
        {
            return;
        }

        final int length = input.length();
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[LONGEST_FLOAT_LENGTH]);
        final MutableAsciiBuffer string = new MutableAsciiBuffer(buffer);
        final DecimalFloat price = new DecimalFloat(value, scale);

        final int encodedLength = string.putFloatAscii(1, price);

        assertEquals(input, string.getAscii(1, length));
        assertEquals(length, encodedLength);
    }

    @Test
    public void canEncodeValueAndScale()
    {
        // ignoring test since expected output has no Trailing Zeros for input value 0 (with positive scale)
        if (value == 0 && scale > 0 && !isExpectedOutputContainTrailingZeros())
        {
            return;
        }

        final int length = input.length();
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[LONGEST_FLOAT_LENGTH]);
        final MutableAsciiBuffer string = new MutableAsciiBuffer(buffer);

        final int encodedLength = string.putFloatAscii(1, value, scale);

        assertEquals(input, string.getAscii(1, length));
        assertEquals(length, encodedLength);
    }

    @Test
    public void canFormatToString()
    {
        // ignoring test since expected output has Trailing Zeros
        if (isExpectedOutputContainTrailingZeros())
        {
            return;
        }

        final DecimalFloat price = new DecimalFloat(value, scale);
        final StringBuilder builder = new StringBuilder();
        CodecUtil.appendFloat(builder, price);

        assertEquals(input, builder.toString());
    }
}
