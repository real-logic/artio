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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.util.MutableAsciiFlyweight;

import java.util.Arrays;

import static uk.co.real_logic.fix_gateway.fields.UtcTimestampEncoder.LENGTH_WITH_MILLISECONDS;

@RunWith(Parameterized.class)
public class UtcTimestampEncoderInvalidCasesTest
{
    private final long timestamp;

    @Parameters
    public static Iterable<Object> data()
    {
        return Arrays.asList(
            new Long[] {UtcTimestampEncoder.MIN_EPOCH_MILLIS - 1},
            new Long[] {UtcTimestampEncoder.MAX_EPOCH_MILLIS + 1}
        );
    }

    public UtcTimestampEncoderInvalidCasesTest(final long timestamp)
    {
        this.timestamp = timestamp;
    }

    @Test(expected = IllegalArgumentException.class)
    public void cannotParseTimestamp()
    {
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[LENGTH_WITH_MILLISECONDS]);
        final MutableAsciiFlyweight timestampBytes = new MutableAsciiFlyweight(buffer);
        UtcTimestampEncoder.encode(timestamp, timestampBytes, 0);
    }
}
