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
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
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

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

@RunWith(Theories.class)
public class OtfParsesBytesFromEncoderTest
{

    /*TODO: @DataPoint*/
    public static int NO_OFFSET = 0;

    @DataPoint
    public static int OFFSET = 1;

    private static final int SESSION_ID = 0;

    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[8 * 1024]);
    private final MutableAsciiFlyweight string = new MutableAsciiFlyweight(buffer);
    private final OtfMessageAcceptor acceptor = mock(OtfMessageAcceptor.class);
    private final OtfParser parser = new OtfParser(acceptor, new IntDictionary());


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

        parseTestRequest(length, offset);
    }

    private void parseTestRequest(final int length, final int offset)
    {
        parser.onMessage(buffer, offset, length, SESSION_ID, LogonDecoder.MESSAGE_TYPE);

        verify(acceptor, times(1)).onField(eq(35), anyBuffer(), anyInt(), anyInt());
        verify(acceptor, times(1)).onComplete();
    }

    private void parseLogon(final int length, final int offset)
    {
        parser.onMessage(buffer, offset, length, SESSION_ID, LogonDecoder.MESSAGE_TYPE);

        final InOrder inOrder = inOrder(acceptor);
        // TODO: generate constants and use them.
        once(inOrder).onNext();
        verifyField(inOrder, 8, "FIX.4.4");
        verifyField(inOrder, 9);
        verifyField(inOrder, 35, "A");
        verifyField(inOrder, 49, "abc");
        verifyField(inOrder, 56, "def");
        verifyField(inOrder, 34, "1");
        verifyField(inOrder, 52, "19700101-00:00:00.010");
        verifyField(inOrder, 98, "0");
        verifyField(inOrder, 108, "10");
        verifyField(inOrder, 10);
        once(inOrder).onComplete();
        inOrder.verifyNoMoreInteractions();
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

    private void verifyField(final InOrder inOrder, final int tag, final String expectedValue)
    {
        ArgumentCaptor<Integer> offset = ArgumentCaptor.forClass(Integer.class);
        ArgumentCaptor<Integer> length = ArgumentCaptor.forClass(Integer.class);
        once(inOrder).onField(eq(tag), anyBuffer(), offset.capture(), length.capture());

        final String value = string.getRangeAsString(offset.getValue(), length.getValue());
        assertEquals(expectedValue, value);
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
