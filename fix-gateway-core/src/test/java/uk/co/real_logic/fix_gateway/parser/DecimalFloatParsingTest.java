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
package uk.co.real_logic.fix_gateway.parser;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.util.AsciiFlyweight;

import java.util.Arrays;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class DecimalFloatParsingTest
{
    @Parameters(name = "{index}: {0} => {1},{2}")
    public static Iterable<Object[]> data1()
    {
        return Arrays.asList(new Object[][]{
                {"55.36", 5536L, 2},
                {"55.3600", 5536L, 2},
                {"0055.36", 5536L, 2},
                {"0055.3600", 5536L, 2},
                {"  55.36 ", 5536L, 2},
                {"  55.3600", 5536L, 2},
                {" 0055.36 ", 5536L, 2},
                {"  0055.3600 ", 5536L, 2},
                {".995", 995L, 0},
                {"0.9950", 995L, 0},
                {"25", 25L, 2},
                {"-55.36", -5536L, 2},
                {"-0055.3600", -5536L, 2},
                {"-55.3600", -5536L, 2},
                {"-.995", -995L, 0},
                {"-0.9950", -995L, 0},
                {"-25", -25L, 2},
        });
    }

    private final String input;
    private final long value;
    private final int scale;

    public DecimalFloatParsingTest(final String input, final long value, final int scale)
    {
        this.input = input;
        this.value = value;
        this.scale = scale;
    }

    @Test
    public void parseTestData()
    {
        final UnsafeBuffer buffer = new UnsafeBuffer(input.getBytes(US_ASCII));
        final AsciiFlyweight string = new AsciiFlyweight(buffer);
        final DecimalFloat price = new DecimalFloat();

        string.parseFloat(0, buffer.capacity(), price);

        assertEquals("Incorrect Value", value, price.value());
        assertEquals("Incorrect Scale", scale, price.scale());
    }
}
