/*
 * Copyright 2015-2025 Real Logic Limited, Adaptive Financial Consulting Ltd.
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
import uk.co.real_logic.artio.fields.RejectReason;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static uk.co.real_logic.artio.dictionary.ExampleDictionary.*;

public class DecoderGeneratorCopyingTest extends AbstractDecoderGeneratorTest
{
    @BeforeClass
    public static void generate() throws Exception
    {
        generate(false);
    }

    @Test
    public void shouldValidateDataFormatForInts() throws Exception
    {
        final Decoder decoder = newHeartbeat();

        decode(INVALID_INT_VALUE_MESSAGE, decoder);

        assertFalse(decoder.validate());
        assertEquals(RejectReason.INCORRECT_DATA_FORMAT_FOR_VALUE.representation(), decoder.rejectReason());
        assertEquals(INT_FIELD_TAG, decoder.invalidTagId());
    }

    @Test
    public void shouldValidateDataFormatForFloats() throws Exception
    {
        final Decoder decoder = newHeartbeat();

        decode(INVALID_FLOAT_VALUE_MESSAGE, decoder);

        assertFalse(decoder.validate());
        assertEquals(RejectReason.INCORRECT_DATA_FORMAT_FOR_VALUE.representation(), decoder.rejectReason());
        assertEquals(FLOAT_FIELD_TAG, decoder.invalidTagId());
    }

    @Test
    public void shouldValidateDataFormatForOutOfRangeFloats() throws Exception
    {
        final Decoder decoder = newHeartbeat();

        decode(OUT_OF_RANGE_FLOAT_VALUE_MESSAGE, decoder);

        assertFalse(decoder.validate());
        assertEquals(RejectReason.INCORRECT_DATA_FORMAT_FOR_VALUE.representation(), decoder.rejectReason());
        assertEquals(FLOAT_FIELD_TAG, decoder.invalidTagId());
    }

}
