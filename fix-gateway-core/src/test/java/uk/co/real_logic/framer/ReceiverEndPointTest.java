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
import org.mockito.InOrder;
import org.mockito.Mockito;
import uk.co.real_logic.agrona.concurrent.AtomicBuffer;
import uk.co.real_logic.fix_gateway.framer.MessageHandler;
import uk.co.real_logic.fix_gateway.framer.ReceiverEndPoint;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.function.ToIntFunction;

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
    public void shouldHandleValidFixMessageInOneGo()
    {
        given:
        theEndpointReceivesACompleteMessage();

        when:
        endPoint.receiveData();

        then:
        handlerReceivesAFramedMessage();
    }

    @Test
    public void shouldOnlyFrameCompleteFixMessage()
    {
        given:
        theEndpointReceivesAnIncompleteMessage();

        when:
        endPoint.receiveData();

        then:
        handlerNotcalled();
    }

    @Test
    public void shouldFrameSplitFixMessage()
    {
        given:
        theEndpointReceivesAnIncompleteMessage();
        endPoint.receiveData();

        when:
        theEndpointReceivesTheRestOfTheMessage();
        endPoint.receiveData();

        then:
        handlerReceivesAFramedMessage();
    }

    @Test
    public void shouldFrameTwoCompleteFixMessagesInOnePacket()
    {
        given:
        theEndpointReceivesTwoCompleteMessages();

        when:
        endPoint.receiveData();

        then:
        handlerReceivesTwoFramedMessages();
    }

    @Test
    public void shouldFrameOneCompleteMessageWhenTheSecondMessageIsIncomplete()
    {
        given:
        theEndpointReceivesACompleteAndAnIncompleteMessage();

        when:
        endPoint.receiveData();

        then:
        handlerReceivesAFramedMessage();
    }

    @Test
    public void shouldFrameSecondSplitMessage()
    {
        given:
        theEndpointReceivesACompleteAndAnIncompleteMessage();
        endPoint.receiveData();

        when:
        theEndpointReceivesTheRestOfTheMessage();
        endPoint.receiveData();

        then:
        handlerReceivesFramedMessages(2);
    }

    private void handlerReceivesAFramedMessage()
    {
        handlerReceivesFramedMessages(1);
    }

    private void handlerReceivesFramedMessages(int numberOfMessages)
    {
        verify(mockHandler, times(numberOfMessages)).onMessage(any(AtomicBuffer.class), eq(0), eq(MSG_LENGTH));
    }

    private void handlerReceivesTwoFramedMessages()
    {
        InOrder inOrder = Mockito.inOrder(mockHandler);
        inOrder.verify(mockHandler, times(1)).onMessage(any(AtomicBuffer.class), eq(0), eq(MSG_LENGTH));
        inOrder.verify(mockHandler, times(1)).onMessage(any(AtomicBuffer.class), eq(MSG_LENGTH), eq(MSG_LENGTH));
        inOrder.verifyNoMoreInteractions();
    }

    private void handlerNotcalled()
    {
        verify(mockHandler, never()).onMessage(any(AtomicBuffer.class), anyInt(), anyInt());
    }

    private void theEndpointReceivesACompleteMessage()
    {
        theEndpointReceives(EG_MESSAGE, 0, MSG_LENGTH);
    }

    private void theEndpointReceivesTwoCompleteMessages()
    {
        theEndpointReceivesTwoMessages(0, MSG_LENGTH);
    }

    private void theEndpointReceivesAnIncompleteMessage()
    {
        theEndpointReceives(EG_MESSAGE, 0, MSG_LENGTH - 8);
    }

    private void theEndpointReceivesTheRestOfTheMessage()
    {
        theEndpointReceives(EG_MESSAGE, MSG_LENGTH - 8, 8);
    }

    private void theEndpointReceivesACompleteAndAnIncompleteMessage()
    {
        theEndpointReceivesTwoMessages(0, MSG_LENGTH - 8);
    }

    private void theEndpointReceives(byte[] data, int offset, int length)
    {
        endpointBufferUpdatedWith(buffer -> {
            buffer.put(data, offset, length);
            return length;
        });
    }

    private void theEndpointReceivesTwoMessages(int secondOffset, int secondLength)
    {
        endpointBufferUpdatedWith(buffer -> {
            buffer.put(EG_MESSAGE)
                  .put(EG_MESSAGE, secondOffset, secondLength);
            return MSG_LENGTH + secondLength;
        });
    }

    private void endpointBufferUpdatedWith(ToIntFunction<ByteBuffer> bufferUpdater)
    {
        try
        {
            doAnswer(invocation -> {
                ByteBuffer buffer = (ByteBuffer) invocation.getArguments()[0];
                return bufferUpdater.applyAsInt(buffer);
            }).when(mockChannel).read(any(ByteBuffer.class));
        }
        catch (IOException e)
        {
            // Should never happen, test in error
            throw new RuntimeException(e);
        }
    }

}
