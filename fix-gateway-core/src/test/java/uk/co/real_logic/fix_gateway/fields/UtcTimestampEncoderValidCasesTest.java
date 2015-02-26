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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static uk.co.real_logic.fix_gateway.fields.UtcTimestampDecoderValidCasesTest.toEpochMillis;
import static uk.co.real_logic.fix_gateway.util.CustomMatchers.equalsString;

@RunWith(Parameterized.class)
public class UtcTimestampEncoderValidCasesTest
{

    private final String timestamp;

    @Parameters
    public static Iterable<Object> data()
    {
        return UtcTimestampDecoderValidCasesTest.data();
    }

    public UtcTimestampEncoderValidCasesTest(final String timestamp)
    {
        this.timestamp = timestamp;
    }

    @Test
    public void canEncodeTimestamp()
    {
        final long epochMillis = toEpochMillis(timestamp);
        final int expectedLength = timestamp.length();
        final MutableAsciiFlyweight string = new MutableAsciiFlyweight(new UnsafeBuffer(new byte[expectedLength]));

        final int length = UtcTimestampEncoder.encode(epochMillis, string, 0);

        assertEquals("encoded wrong length", expectedLength, length);
        assertThat(string, equalsString(timestamp, 0, length));
    }

}
