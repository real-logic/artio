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
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.DebugLogger;
import uk.co.real_logic.fix_gateway.builder.LogonEncoder;
import uk.co.real_logic.fix_gateway.builder.TestRequestEncoder;
import uk.co.real_logic.fix_gateway.decoder.LogonDecoder;
import uk.co.real_logic.fix_gateway.dictionary.IntDictionary;
import uk.co.real_logic.fix_gateway.otf.OtfMessageAcceptor;
import uk.co.real_logic.fix_gateway.otf.OtfParser;
import uk.co.real_logic.fix_gateway.util.MutableAsciiFlyweight;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

public class OtfParsesBytesFromEncoderTest
{

    private static final int OFFSET = 1;
    private static final int SESSION_ID = 0;

    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[8 * 1024]);
    private final MutableAsciiFlyweight string = new MutableAsciiFlyweight(buffer);
    private final OtfMessageAcceptor acceptor = mock(OtfMessageAcceptor.class);
    // new FakeOtfAcceptor()
    private final OtfParser parser = new OtfParser(acceptor, new IntDictionary());

    @Test
    public void shouldParseLogon()
    {
        final LogonEncoder encoder = new LogonEncoder()
            .heartBtInt(10)
            .encryptMethod(0);

        encoder.header()
            .senderCompID("abc")
            .targetCompID("def")
            .msgSeqNum(1)
            .sendingTime(10);

        final int length = encoder.encode(string, OFFSET);
        DebugLogger.log("%s\n", buffer, OFFSET, length);
        parser.onMessage(buffer, OFFSET, length, SESSION_ID, LogonDecoder.MESSAGE_TYPE);

        final InOrder inOrder = inOrder(acceptor);
        // TODO: generate constants and use them.
        once(inOrder).onNext();
        verifyField(inOrder, 8);
        verifyField(inOrder, 9);
        verifyField(inOrder, 35);
        verifyField(inOrder, 49);
        verifyField(inOrder, 56);
        verifyField(inOrder, 34);
        verifyField(inOrder, 52);
        verifyField(inOrder, 98);
        verifyField(inOrder, 108);
        verifyField(inOrder, 10);
        once(inOrder).onComplete();
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void shouldParseTestRequestId()
    {
        final TestRequestEncoder testRequest = new TestRequestEncoder();
        testRequest.testReqID("hi");

        testRequest.header()
            .msgSeqNum(1)
            .senderCompID("LEH_LZJ02")
            .targetCompID("CCG");

        final int length = testRequest.encode(string, OFFSET);
        DebugLogger.log("%s\n", buffer, OFFSET, length);
        parser.onMessage(buffer, OFFSET, length, SESSION_ID, LogonDecoder.MESSAGE_TYPE);

        verify(acceptor, times(1)).onField(eq(35), anyBuffer(), anyInt(), anyInt());
        verify(acceptor, times(1)).onComplete();
    }

    private void verifyField(final InOrder inOrder, final int tag)
    {
        once(inOrder).onField(eq(tag), anyBuffer(), anyInt(), anyInt());
    }

    private OtfMessageAcceptor once(final InOrder inOrder)
    {
        return inOrder.verify(acceptor, times(1));
    }

    private DirectBuffer anyBuffer()
    {
        return any(DirectBuffer.class);
    }

}
