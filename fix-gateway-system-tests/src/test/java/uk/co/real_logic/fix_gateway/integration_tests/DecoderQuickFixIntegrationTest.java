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
package uk.co.real_logic.fix_gateway.integration_tests;

import org.junit.Test;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.DebugLogger;
import uk.co.real_logic.fix_gateway.builder.Decoder;
import uk.co.real_logic.fix_gateway.decoder.HeaderDecoder;
import uk.co.real_logic.fix_gateway.decoder.LogonDecoder;
import uk.co.real_logic.fix_gateway.decoder.TestRequestDecoder;
import uk.co.real_logic.fix_gateway.util.MutableAsciiFlyweight;

import static org.junit.Assert.assertEquals;
import static uk.co.real_logic.fix_gateway.integration_tests.QuickFixUtil.logon;
import static uk.co.real_logic.fix_gateway.integration_tests.QuickFixUtil.testRequest;
import static uk.co.real_logic.fix_gateway.util.CustomMatchers.assertCharsEquals;

public class DecoderQuickFixIntegrationTest
{
    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[8 * 1024]);
    private final MutableAsciiFlyweight string = new MutableAsciiFlyweight(buffer);

    @Test
    public void decodesTestRequest()
    {
        TestRequestDecoder decoder = new TestRequestDecoder();
        decode(testRequest(), decoder);

        assertCharsEquals("abc", decoder.testReqID(), decoder.testReqIDLength());
    }

    @Test
    public void decodesLogon()
    {
        LogonDecoder decoder = new LogonDecoder();
        decode(logon(), decoder);

        DebugLogger.log("Decoder: %s\n", decoder);

        assertEquals(0, decoder.encryptMethod());
        assertEquals(10, decoder.heartBtInt());

        final HeaderDecoder header = decoder.header();
        assertCharsEquals("LEH_LZJ02", header.senderCompID(), header.senderCompIDLength());
        assertCharsEquals("CCG", header.targetCompID(), header.targetCompIDLength());
        assertEquals(1, header.msgSeqNum());
        assertEquals(0, header.sendingTime());

    }

    private void decode(final Object encoder, final Decoder decoder)
    {
        final String message = encoder.toString();
        DebugLogger.log(message);
        string.putAscii(0, message);
        decoder.decode(string, 0, message.length());
    }
}
