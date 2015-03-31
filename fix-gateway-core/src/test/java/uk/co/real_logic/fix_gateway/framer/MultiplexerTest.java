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
package uk.co.real_logic.fix_gateway.framer;

import org.junit.Test;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.fix_gateway.commands.ReceiverProxy;

import static org.mockito.Mockito.*;

public class MultiplexerTest
{
    public static final long CONNECTION_ID = 1L;
    private SenderEndPoint mockSenderEndPoint = mock(SenderEndPoint.class);
    private ReceiverProxy mockReceiver = mock(ReceiverProxy.class);

    private Multiplexer multiplexer = new Multiplexer(mockReceiver);
    private DirectBuffer buffer = mock(DirectBuffer.class);

    private void connectedId(final long connectionId)
    {
        when(mockSenderEndPoint.connectionId()).thenReturn(connectionId);
        multiplexer.onNewConnection(mockSenderEndPoint);
    }

    @Test
    public void messagesAreSentToCorrectEndPoint()
    {
        given:
        connectedId(CONNECTION_ID);

        when:
        aMessageArrives();

        then:
        messagePassedToEndpoint();
    }

    @Test
    public void messagesAreNotSentToOtherEndPoints()
    {
        given:
        connectedId(2L);

        when:
        aMessageArrives();

        then:
        noFrameReceived();
    }

    @Test
    public void messagesAreNotMultiplexedAfterDisconnect()
    {
        given:
        connectedId(CONNECTION_ID);

        when:
        multiplexer.disconnect(CONNECTION_ID);
        aMessageArrives();

        then:
        noFrameReceived();
    }

    @Test
    public void receiverNotifiedOfDisconnect()
    {
        given:
        connectedId(CONNECTION_ID);

        when:
        multiplexer.disconnect(CONNECTION_ID);

        then:
        verify(mockReceiver).disconnect(CONNECTION_ID);
    }

    private void messagePassedToEndpoint()
    {
        verify(mockSenderEndPoint).onFramedMessage(buffer, 1, 1);
    }

    private void noFrameReceived()
    {
        verify(mockSenderEndPoint, never()).onFramedMessage(any(), anyInt(), anyInt());
    }

    private void aMessageArrives()
    {
        multiplexer.onMessage(buffer, 1, 1, CONNECTION_ID);
    }
}
