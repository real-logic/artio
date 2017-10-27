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
package uk.co.real_logic.artio.fields;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

import java.util.Arrays;

@RunWith(Parameterized.class)
public class UtcTimestampEncoderInvalidCasesTest
{
    private final long timestamp;

    @Parameters(name = "{0}")
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
        final UnsafeBuffer buffer = new UnsafeBuffer(new byte[UtcTimestampEncoder.LENGTH_WITH_MILLISECONDS]);
        final MutableAsciiBuffer timestampBytes = new MutableAsciiBuffer(buffer);
        UtcTimestampEncoder.encode(timestamp, timestampBytes, 0);
    }
}
