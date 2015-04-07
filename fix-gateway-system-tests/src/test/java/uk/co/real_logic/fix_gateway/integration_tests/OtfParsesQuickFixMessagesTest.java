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
import org.mockito.InOrder;
import quickfix.IntField;
import quickfix.Message;
import quickfix.field.*;
import quickfix.fix44.Logon;
import quickfix.fix44.TestRequest;
import uk.co.real_logic.fix_gateway.DebugLogger;
import uk.co.real_logic.fix_gateway.decoder.LogonDecoder;

import java.util.Date;

import static org.mockito.Mockito.inOrder;
import static uk.co.real_logic.fix_gateway.decoder.Constants.*;

public class OtfParsesQuickFixMessagesTest extends AbstractOtfParserTest
{
    @Test
    public void parsesTestRequest()
    {
        final TestRequest message = new TestRequest(
            new TestReqID("abc")
        );

        setupHeader(message);

        final int length = encodeMessage(message);

        parseTestRequest(0, length);
    }

    @Test
    public void parsesLogon()
    {
        final Logon message = new Logon();
        message.set(new HeartBtInt(10));
        setupHeader(message);

        final int length = encodeMessage(message);
        parser.onMessage(buffer, 0, length, SESSION_ID, LogonDecoder.MESSAGE_TYPE);

        final InOrder inOrder = inOrder(acceptor);
        verifyNext(inOrder);
        verifyField(inOrder, BEGIN_STRING, "FIX.4.4");
        verifyField(inOrder, BODY_LENGTH);
        verifyField(inOrder, MSG_TYPE, "A");
        verifyField(inOrder, MSG_SEQ_NUM, "1");
        verifyField(inOrder, SENDER_COMP_ID, "LEH_LZJ02");
        verifyField(inOrder, SENDING_TIME, "19700101-00:00:00.000");
        verifyField(inOrder, TARGET_COMP_ID, "CCG");
        verifyField(inOrder, ENCRYPT_METHOD, "0");
        verifyField(inOrder, HEART_BT_INT, "10");
        verifyField(inOrder, CHECK_SUM);
        verifyComplete(inOrder);
    }

    private void setupHeader(final Message message)
    {
        final Message.Header header = message.getHeader();
        header.setField(new SenderCompID("LEH_LZJ02"));
        header.setField(new TargetCompID("CCG"));
        header.setField(new MsgSeqNum(1));
        header.setField(new SendingTime(new Date(0)));
        header.setField(new IntField(EncryptMethod.FIELD, EncryptMethod.NONE_OTHER));
    }

    private int encodeMessage(final Message message)
    {
        final String encodedMessage = message.toString();
        DebugLogger.log(encodedMessage);
        string.putAscii(0, encodedMessage);
        return encodedMessage.length();
    }
}
