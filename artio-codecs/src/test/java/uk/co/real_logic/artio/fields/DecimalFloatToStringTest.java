/*
 * Copyright 2015-2025 Real Logic Limited.
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

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class DecimalFloatToStringTest
{
    @Parameters(name = "{index}: {1},{2} => {0}")
    public static Iterable<Object[]> data1()
    {
        return DecimalFloatEncodingTest.decimalFloatCodecData();
    }

    private final String input;
    private final long value;
    private final int scale;

    public DecimalFloatToStringTest(final String input, final long value, final int scale)
    {
        this.input = input;
        this.value = value;
        this.scale = scale;
    }

    @Test
    public void canEncodeDecimalFloat()
    {
        final DecimalFloat price = new DecimalFloat(value, scale);

        assertEquals(Float.valueOf(input), Float.valueOf(price.toString()));
    }
}
