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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.agrona.concurrent.AtomicCounter;
import uk.co.real_logic.agrona.concurrent.OneToOneConcurrentArrayQueue;
import uk.co.real_logic.fix_gateway.commands.ReceiverCommand;
import uk.co.real_logic.fix_gateway.commands.ReceiverProxy;
import uk.co.real_logic.fix_gateway.commands.SenderProxy;
import uk.co.real_logic.fix_gateway.framer.session.Session;
import uk.co.real_logic.fix_gateway.util.MilliClock;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.notNull;
import static org.mockito.Mockito.*;

public class ReceiverTest
{
    private static final InetSocketAddress ADDRESS = new InetSocketAddress("localhost", 9999);
    private static final long CONNECTION_ID = 2L;

    private SocketChannel client;
    private ByteBuffer clientBuffer = ByteBuffer.allocate(1024);

    private SenderEndPoint mockSenderEndPoint = mock(SenderEndPoint.class);
    private ReceiverEndPoint mockReceiverEndPoint = mock(ReceiverEndPoint.class);
    private ConnectionHandler mockConnectionHandler = mock(ConnectionHandler.class);
    private OneToOneConcurrentArrayQueue<ReceiverCommand> commandQueue = new OneToOneConcurrentArrayQueue<>(10);
    private SenderProxy mockSender = mock(SenderProxy.class);
    private Session mockSession = mock(Session.class);
    private MilliClock mockClock = mock(MilliClock.class);

    private ReceiverProxy receiverProxy = new ReceiverProxy(commandQueue, mock(AtomicCounter.class));
    private Receiver receiver = new Receiver(mockClock, ADDRESS, mockConnectionHandler, commandQueue, mockSender);

    @Before
    public void setUp() throws IOException
    {
        clientBuffer.putInt(10, 5);

        when(mockConnectionHandler.receiverEndPoint(any(SocketChannel.class), anyLong(), any(Session.class)))
            .thenReturn(mockReceiverEndPoint);

        when(mockConnectionHandler.senderEndPoint(any(SocketChannel.class), anyLong()))
            .thenReturn(mockSenderEndPoint);

        when(mockReceiverEndPoint.session()).thenReturn(mockSession);
        when(mockReceiverEndPoint.connectionId()).thenReturn(CONNECTION_ID);
    }

    @After
    public void tearDown()
    {
        receiver.onClose();
    }

    @Test
    public void shouldListenOnSpecifiedPort() throws IOException
    {
        when:
        aClientConnects();

        then:
        assertTrue("Client has failed to connect", client.finishConnect());
    }

    @Test
    public void shouldCreateEndPointWhenClientConnects() throws Exception
    {
        given:
        aClientConnects();

        when:
        receiver.doWork();

        then:
        verify(mockConnectionHandler).receiverEndPoint(notNull(SocketChannel.class), anyLong(), any(Session.class));
    }

    @Test
    public void shouldNotifySenderWhenClientConnects() throws Exception
    {
        given:
        aClientConnects();

        when:
        receiver.doWork();

        then:
        verify(mockSender).newAcceptedConnection(mockSenderEndPoint);
    }

    @Test
    public void shouldPollSessionOfConnectedClient() throws Exception
    {
        given:
        aClientConnects();

        when:
        receiver.doWork();

        then:
        verify(mockSession).poll(0);
    }

    @Test
    public void shouldPassDataToEndPointWhenSent() throws Exception
    {
        given:
        aClientConnects();
        receiver.doWork();

        when:
        aClientSendsData();
        receiver.doWork();

        then:
        verify(mockReceiverEndPoint).receiveData();
    }

    @Test
    public void shouldCloseSocketUponDisconnect() throws Exception
    {
        given:
        aClientConnects();
        receiver.doWork();

        when:
        receiverProxy.disconnect(CONNECTION_ID);
        receiver.doWork();

        then:
        verify(mockReceiverEndPoint).close();
    }

    private void aClientConnects() throws IOException
    {
        client = SocketChannel.open(ADDRESS);
    }

    private void aClientSendsData() throws IOException
    {
        clientBuffer.position(0);
        assertEquals("Has written bytes", clientBuffer.remaining(), client.write(clientBuffer));
    }
}
