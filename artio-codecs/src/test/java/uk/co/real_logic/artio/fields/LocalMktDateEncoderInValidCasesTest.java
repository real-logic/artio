/*
 * Copyright 2015-2024 Real Logic Limited.
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
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

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
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[LocalMktDateEncoder.LENGTH]);
        final MutableAsciiBuffer timestampBytes = new MutableAsciiBuffer(buffer);
        LocalMktDateEncoder.encode(timestamp, timestampBytes, 0);
    }
}
