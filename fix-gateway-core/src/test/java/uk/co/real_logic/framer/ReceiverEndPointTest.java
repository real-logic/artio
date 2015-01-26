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

    private static final byte[] EG_MESSAGE = ("8=FIX.4.2\1 9=145\1 35=D\1 34=4\1 49=ABC_DEFG01\1 52=20090323-15:40:29\1 " +
            "56=CCG\1\n115=XYZ\1 11=NF 0542/03232009\1 54=1\1 38=100\1 55=CVS\1 40=1\1 59=0\1 47=A\1\n" +
            "60=20090323-15:40:29\1 21=1\1 207=N\1 10=139\1 ").getBytes(US_ASCII);

    private SocketChannel mockChannel = mock(SocketChannel.class);
    private MessageHandler mockHandler = mock(MessageHandler.class);
    private ReceiverEndPoint endPoint = new ReceiverEndPoint(mockChannel, 16 * 1024, mockHandler);

    @Test
    public void shouldValidateAmountOfDataReceivedBeforePassingOn()
    {
        given:
        mockReadData(EG_MESSAGE, 0, 3);

        when:
        endPoint.receiveData();

        then:
        verify(mockHandler, never()).onMessage(any(AtomicBuffer.class), anyInt(), anyInt());
    }

    @Test
    public void shouldHandleValidFixMessageInOneGo()
    {
        given:
        mockReadData(EG_MESSAGE, 0, EG_MESSAGE.length);

        when:
        endPoint.receiveData();

        then:
        verify(mockHandler, times(1)).onMessage(any(AtomicBuffer.class), eq(0), anyInt());
    }

    private void mockReadData(byte[] data, int offset, int length)
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
