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
package uk.co.real_logic.framer;

import org.junit.Test;
import uk.co.real_logic.agrona.concurrent.AtomicBuffer;
import uk.co.real_logic.fix_gateway.framer.MessageHandler;
import uk.co.real_logic.fix_gateway.framer.ReceiverEndPoint;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.*;

public class ReceiverEndPointTest
{

    private static final byte[] EG_MESSAGE = ("8=FIX.4.2 9=145 35=D 34=4 49=ABC_DEFG01 52=20090323-15:40:29 " +
            "56=CCG 115=XYZ 11=NF 0542/03232009 54=1 38=100 55=CVS 40=1 59=0 47=A" +
            "60=20090323-15:40:29 21=1 207=N 10=139 ").replace(' ', '\1').getBytes(US_ASCII);

    private static final int MSG_LENGTH = EG_MESSAGE.length;

    private SocketChannel mockChannel = mock(SocketChannel.class);
    private MessageHandler mockHandler = mock(MessageHandler.class);
    private ReceiverEndPoint endPoint = new ReceiverEndPoint(mockChannel, 16 * 1024, mockHandler);

    @Test
    public void shouldValidateAmountOfDataReceivedBeforePassingOn()
    {
        given:
        theEndpointReceives(EG_MESSAGE, 0, 3);

        when:
        endPoint.receiveData();

        then:
        verifyHandlerNotcalled();
    }

    @Test
    public void shouldHandleValidFixMessageInOneGo()
    {
        given:
        theEndpointReceives(EG_MESSAGE, 0, MSG_LENGTH);

        when:
        endPoint.receiveData();

        then:
        verify(mockHandler, times(1)).onMessage(any(AtomicBuffer.class), eq(0), eq(MSG_LENGTH));
    }

    private void verifyHandlerNotcalled()
    {
        verify(mockHandler, never()).onMessage(any(AtomicBuffer.class), anyInt(), anyInt());
    }

    private void theEndpointReceives(byte[] data, int offset, int length)
    {
        try
        {
            doAnswer(invocation -> {
                ByteBuffer buffer = (ByteBuffer) invocation.getArguments()[0];
                buffer.put(data, offset, length);
                return length;
            }).when(mockChannel).read(any(ByteBuffer.class));
        }
        catch (IOException e)
        {
            // Should never happen, test in error
            throw new RuntimeException(e);
        }
    }

}
