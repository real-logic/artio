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

import org.junit.experimental.theories.DataPoint;
import org.junit.experimental.theories.Theory;
import org.mockito.InOrder;
import uk.co.real_logic.fix_gateway.DebugLogger;
import uk.co.real_logic.fix_gateway.builder.LogonEncoder;
import uk.co.real_logic.fix_gateway.builder.TestRequestEncoder;

import static org.mockito.Mockito.inOrder;
import static uk.co.real_logic.fix_gateway.decoder.Constants.*;

public class OtfParsesBytesFromEncoderTest extends AbstractOtfParserTest
{

    @DataPoint
    public static final int NO_OFFSET = 0;

    @DataPoint
    public static final int OFFSET = 1;

    @Theory
    public void shouldParseLogon(final int offset)
    {
        final int length = encodeLogon(offset);

        DebugLogger.log("%s\n", buffer, offset, length);

        parseLogon(length, offset);
    }

    @Theory
    public void shouldParseTestRequest(final int offset)
    {
        final int length = encodeTestRequest(offset);

        DebugLogger.log("%s\n", buffer, offset, length);

        parseTestRequest(offset, length);
    }

    private void parseLogon(final int length, final int offset)
    {
        parser.onMessage(buffer, offset, length);

        final InOrder inOrder = inOrder(acceptor);
        verifyNext(inOrder);
        verifyField(inOrder, BEGIN_STRING, "FIX.4.4");
        verifyField(inOrder, BODY_LENGTH);
        verifyField(inOrder, MSG_TYPE, "A");
        verifyField(inOrder, SENDER_COMP_ID, "abc");
        verifyField(inOrder, TARGET_COMP_ID, "def");
        verifyField(inOrder, MSG_SEQ_NUM, "1");
        verifyField(inOrder, SENDING_TIME, "19700101-00:00:00.010");
        verifyField(inOrder, ENCRYPT_METHOD, "0");
        verifyField(inOrder, HEART_BT_INT, "10");
        verifyField(inOrder, CHECK_SUM);
        verifyComplete(inOrder);
    }

    private int encodeLogon(final int offset)
    {
        final LogonEncoder encoder = new LogonEncoder()
            .heartBtInt(10)
            .encryptMethod(0);

        encoder.header()
            .senderCompID("abc")
            .targetCompID("def")
            .msgSeqNum(1)
            .sendingTime(10);

        return encoder.encode(string, offset);
    }

    private int encodeTestRequest(final int offset)
    {
        // 8=FIX.4.49=005835=149=LEH_LZJ0256=CCG34=352=19700101-00:00:00112=hi10=140
        final TestRequestEncoder testRequest = new TestRequestEncoder();
        testRequest.testReqID("hi");

        testRequest.header()
            .msgSeqNum(3)
            .senderCompID("LEH_LZJ02")
            .targetCompID("CCG");

        return testRequest.encode(string, offset);
    }

}
