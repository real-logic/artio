/*
 * Copyright 2015-2022 Real Logic Limited, Adaptive Financial Consulting Ltd.
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
package uk.co.real_logic.artio.dictionary.generation;

import org.junit.BeforeClass;
import org.junit.Test;
import uk.co.real_logic.artio.builder.Decoder;

import static uk.co.real_logic.artio.dictionary.ExampleDictionary.INVALID_FLOAT_VALUE_MESSAGE;
import static uk.co.real_logic.artio.dictionary.ExampleDictionary.INVALID_INT_VALUE_MESSAGE;
import static uk.co.real_logic.artio.util.CustomMatchers.assertTargetThrows;

public class DecoderGeneratorFlyweightTest extends AbstractDecoderGeneratorTest
{
    @BeforeClass
    public static void generate() throws Exception
    {
        generate(true);
    }

    @Test
    public void shouldValidateDataFormatForInts() throws Exception
    {
        final Decoder decoder = decodeHeartbeat(INVALID_INT_VALUE_MESSAGE);

        assertTargetThrows(() -> getIntField(decoder), NumberFormatException.class,
            "error parsing int: A tag=116");
    }

    @Test
    public void shouldValidateDataFormatForFloats() throws Exception
    {
        final Decoder decoder = decodeHeartbeat(INVALID_FLOAT_VALUE_MESSAGE);

        assertTargetThrows(() -> getFloatField(decoder), NumberFormatException.class,
            "'A' isn't a valid digit @ 39 tag=117");
    }
}
