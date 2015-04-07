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
import quickfix.DataDictionary;
import quickfix.Message;
import quickfix.field.*;
import quickfix.fix44.Logon;
import quickfix.fix44.TestRequest;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.builder.Encoder;
import uk.co.real_logic.fix_gateway.builder.LogonEncoder;
import uk.co.real_logic.fix_gateway.builder.TestRequestEncoder;
import uk.co.real_logic.fix_gateway.util.MutableAsciiFlyweight;

import static org.junit.Assert.assertEquals;
import static uk.co.real_logic.fix_gateway.integration_tests.QuickFixUtil.assertFieldEquals;

public class EncoderQuickFixIntegrationTest
{
    public static final String TEST_REQ_ID = "abc";

    private UnsafeBuffer buffer = new UnsafeBuffer(new byte[8 * 1024]);
    private MutableAsciiFlyweight string = new MutableAsciiFlyweight(buffer);

    @Test
    public void encodesTestRequest() throws Exception
    {
        final TestRequestEncoder encoder = new TestRequestEncoder()
            .testReqID(TEST_REQ_ID);

        final TestRequest decoder = new TestRequest();
        encode(encoder, decoder);

        assertFieldEquals(TEST_REQ_ID, decoder, new TestReqID());
    }

    @Test
    public void encodesLogon() throws Exception
    {
        final LogonEncoder encoder = new LogonEncoder()
            .heartBtInt(10)
            .encryptMethod(0);

        encoder
            .header()
            .senderCompID("LEH_LZJ02")
            .targetCompID("CCG")
            .msgSeqNum(1)
            .sendingTime(0);

        final Logon decoder = new Logon();
        encode(encoder, decoder);

        assertFieldEquals(0, decoder, new EncryptMethod());
        assertFieldEquals(10, decoder, new HeartBtInt());

        final Message.Header header = decoder.getHeader();
        assertFieldEquals("LEH_LZJ02", header, new SenderCompID());
        assertFieldEquals("CCG", header, new TargetCompID());
        assertFieldEquals(1, header, new MsgSeqNum());

        final SendingTime sendingTime = new SendingTime();
        header.getField(sendingTime);
        assertEquals(0, sendingTime.getValue().getTime());
    }

    private void encode(final Encoder encoder, final Message decoder) throws Exception
    {
        final int length = encoder.encode(string, 0);
        final String message = string.getAscii(0, length);
        decoder.fromString(message, new DataDictionary("FIX44.xml"), true);
    }

}
