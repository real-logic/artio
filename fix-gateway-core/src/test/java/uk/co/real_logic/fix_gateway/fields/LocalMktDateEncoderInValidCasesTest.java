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

import org.junit.Test;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.util.MutableAsciiFlyweight;

import static uk.co.real_logic.fix_gateway.fields.LocalMktDateEncoder.LENGTH;

public class LocalMktDateEncoderInValidCasesTest
{

    @Test(expected = IllegalArgumentException.class)
    public void cannotEncodeBelowMinimum()
    {
        encode(LocalMktDateEncoder.MIN_EPOCH_DAYS - 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void cannotEncodeAboveMaximum()
    {
        encode(LocalMktDateEncoder.MAX_EPOCH_DAYS + 1);
    }

    private void encode(final int timestamp)
    {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[LENGTH]);
        final MutableAsciiFlyweight timestampBytes = new MutableAsciiFlyweight(buffer);
        LocalMktDateEncoder.encode(timestamp, timestampBytes, 0);
    }
}
