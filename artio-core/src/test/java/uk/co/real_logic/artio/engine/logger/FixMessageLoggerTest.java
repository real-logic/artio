/*
 * Copyright 2015-2019 Adaptive Financial Consulting Ltd.
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
package uk.co.real_logic.artio.engine.logger;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Before;
import uk.co.real_logic.artio.decoder.LogonDecoder;
import uk.co.real_logic.artio.messages.MessageStatus;
import uk.co.real_logic.artio.protocol.GatewayPublication;

import java.util.stream.IntStream;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.util.stream.Collectors.joining;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;

public class FixMessageLoggerTest extends AbstractFixMessageLoggerTest
{
    final byte[] fakeFixMessage = IntStream
        .range(0, String.valueOf(Long.MIN_VALUE).length())
        .mapToObj(i -> " ").collect(joining()).getBytes(US_ASCII);

    final UnsafeBuffer fakeMessageBuffer = new UnsafeBuffer(fakeFixMessage);

    {
        compactionSize = 500;
    }

    @Before
    public void setup()
    {
        setup(null);
    }

    void onMessage(final GatewayPublication inboundPublication, final long timestamp)
    {
        fakeMessageBuffer.putLongAscii(0, timestamp);
        final long position = inboundPublication.saveMessage(
            fakeMessageBuffer,
            0,
            fakeMessageBuffer.capacity(),
            LIBRARY_ID,
            LogonDecoder.MESSAGE_TYPE,
            SESSION_ID,
            SEQUENCE_INDEX,
            CONNECTION_ID,
            MessageStatus.OK,
            (int)timestamp,
            timestamp);
        assertThat(position, greaterThan(0L));
    }
}
