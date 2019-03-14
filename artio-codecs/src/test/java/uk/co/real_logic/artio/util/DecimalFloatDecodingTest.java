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
package uk.co.real_logic.artio.util;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import uk.co.real_logic.artio.fields.DecimalFloat;

import java.util.Arrays;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class DecimalFloatDecodingTest
{
    @Parameters(name = "{index}: {0} => {1},{2}")
    public static Iterable<Object[]> decimalFloatCodecData()
    {
        return Arrays.asList(new Object[][]
        {
            {"55.36", 5536L, 2},
            {"55.3600", 5536L, 2},
            {"0055.36", 5536L, 2},
            {"0055.3600", 5536L, 2},
            {".995", 995L, 3},
            {"0.9950", 995L, 3},
            {"25", 25L, 0},
            {"-55.36", -5536L, 2},
            {"-0055.3600", -5536L, 2},
            {"-55.3600", -5536L, 2},
            {"-.995", -995L, 3},
            {"-0.9950", -995L, 3},
            {"-25", -25L, 0},
            {".6", 6L, 1},
            {".06", 6L, 2},
            {"-.6", -6L, 1},
            {"-.06", -6L, 2},
            {"10", 10L, 0},
            {"-10", -10L, 0},
            {"10.", 10L, 0},
            {"-10.", -10L, 0},

            {"1.00000000", 1L, 0},
            {"-1.00000000", -1L, 0},
            {"0.92117125", 92117125L, 8},
            {"-0.92117125", -92117125L, 8},
            {"0.92125000", 92125L, 5},
            {"-0.92125000", -92125L, 5},
            {"0.00007875", 7875, 8},
            {"-0.00007875", -7875, 8},
            {"1.00109125", 100109125, 8},
            {"-1.00109125", -100109125, 8},
            {"6456.00000001", 645600000001L, 8}
        });
    }

    private final String input;
    private final long value;
    private final int scale;

    public DecimalFloatDecodingTest(final String input, final long value, final int scale)
    {
        this.input = input;
        this.value = value;
        this.scale = scale;
    }

    @Test
    public void shouldDecodeFromString()
    {
        final DecimalFloat price = new DecimalFloat();

        price.fromString(input);

        assertEquals("Incorrect Value", value, price.value());
        assertEquals("Incorrect Scale", scale, price.scale());
    }

    @Test
    public void shouldDecodeFromStringWithOffset()
    {
        final DecimalFloat price = new DecimalFloat();

        final String extendedInput = ' ' + input + ' ';
        price.fromString(extendedInput, 1, input.length());

        assertEquals("Incorrect Value", value, price.value());
        assertEquals("Incorrect Scale", scale, price.scale());
    }

    @Test
    public void canDecodeDecimalFloat()
    {
        final byte[] bytes = input.getBytes(US_ASCII);
        canDecodeDecimalFloatFromBytes(bytes);
    }

    @Test
    public void canDecodeDecimalFloatWithSpacePrefixOrSuffix()
    {
        final String paddedInput = "  " + this.input + "  ";
        final byte[] bytes = paddedInput.getBytes(US_ASCII);
        canDecodeDecimalFloatFromBytes(bytes);
    }

    private void canDecodeDecimalFloatFromBytes(final byte[] bytes)
    {
        final MutableAsciiBuffer string = new MutableAsciiBuffer(new byte[bytes.length + 2]);
        string.putBytes(1, bytes);
        final DecimalFloat price = new DecimalFloat();

        string.getFloat(price, 1, bytes.length);

        assertEquals("Incorrect Value", value, price.value());
        assertEquals("Incorrect Scale", scale, price.scale());
    }

}
