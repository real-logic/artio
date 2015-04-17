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
import org.mockito.ArgumentCaptor;
import uk.co.real_logic.agrona.concurrent.AtomicCounter;
import uk.co.real_logic.agrona.concurrent.NoOpIdleStrategy;
import uk.co.real_logic.agrona.concurrent.OneToOneConcurrentArrayQueue;
import uk.co.real_logic.fix_gateway.ConnectionHandler;
import uk.co.real_logic.fix_gateway.FixGateway;
import uk.co.real_logic.fix_gateway.SessionConfiguration;
import uk.co.real_logic.fix_gateway.replication.GatewaySubscription;
import uk.co.real_logic.fix_gateway.session.InitiatorSession;
import uk.co.real_logic.fix_gateway.session.Session;
import uk.co.real_logic.fix_gateway.util.MilliClock;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.notNull;
import static org.mockito.Mockito.*;

public class FramerTest
{
    private static final InetSocketAddress TEST_ADDRESS = new InetSocketAddress("localhost", 9998);

    private static final SessionConfiguration CONFIGURATION = SessionConfiguration
        .builder()
        .address(TEST_ADDRESS.getHostName(), TEST_ADDRESS.getPort())
        .senderCompId("LEH_LZJ02")
        .targetCompId("CCG")
        .build();

    private static final InetSocketAddress FRAMER_ADDRESS = new InetSocketAddress("localhost", 9999);
    private static final long CONNECTION_ID = 2L;

    private ServerSocketChannel server;

    private SocketChannel client;
    private ByteBuffer clientBuffer = ByteBuffer.allocate(1024);

    private SenderEndPoint mockSenderEndPoint = mock(SenderEndPoint.class);
    private ReceiverEndPoint mockReceiverEndPoint = mock(ReceiverEndPoint.class);
    private ConnectionHandler mockConnectionHandler = mock(ConnectionHandler.class);
    private FixGateway mockGateway = mock(FixGateway.class);
    private OneToOneConcurrentArrayQueue<FramerCommand> commandQueue = new OneToOneConcurrentArrayQueue<>(10);
    private InitiatorSession mockSession = mock(InitiatorSession.class);
    private MilliClock mockClock = mock(MilliClock.class);

    private FramerProxy proxy = new FramerProxy(commandQueue, mock(AtomicCounter.class),
        new NoOpIdleStrategy());
    private Framer framer = new Framer(mockClock, FRAMER_ADDRESS, mockConnectionHandler, commandQueue,
        mock(Multiplexer.class), mockGateway, mock(GatewaySubscription.class));

    private ArgumentCaptor<Long> connectionId = ArgumentCaptor.forClass(Long.class);

    @Before
    public void setUp() throws IOException
    {
        server = ServerSocketChannel.open().bind(TEST_ADDRESS);

        clientBuffer.putInt(10, 5);

        when(mockConnectionHandler.receiverEndPoint(any(SocketChannel.class), anyLong(), any(Session.class)))
            .thenReturn(mockReceiverEndPoint);

        when(mockConnectionHandler.senderEndPoint(any(SocketChannel.class), anyLong()))
            .thenReturn(mockSenderEndPoint);

        when(mockConnectionHandler.initiateSession(connectionId.capture(), eq(mockGateway), eq(CONFIGURATION)))
            .thenReturn(mockSession);

        when(mockReceiverEndPoint.session()).thenReturn(mockSession);
        when(mockReceiverEndPoint.connectionId()).thenReturn(CONNECTION_ID);
    }

    @After
    public void tearDown() throws IOException
    {
        framer.onClose();
        server.close();
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
        framer.doWork();

        then:
        verify(mockConnectionHandler).receiverEndPoint(notNull(SocketChannel.class), anyLong(), any(Session.class));
    }

    @Test
    public void shouldNotifySenderWhenClientConnects() throws Exception
    {
        given:
        aClientConnects();

        when:
        framer.doWork();

        // TODO:
        //then:
        //verify(mockSender).newAcceptedConnection(mockSenderEndPoint);
    }

    @Test
    public void shouldPollSessionOfConnectedClient() throws Exception
    {
        given:
        aClientConnects();

        when:
        framer.doWork();

        // TODO
        then:
        verify(mockSession).poll(0);
    }

    @Test
    public void shouldPassDataToEndPointWhenSent() throws Exception
    {
        given:
        aClientConnects();
        framer.doWork();

        when:
        aClientSendsData();
        framer.doWork();

        then:
        verify(mockReceiverEndPoint).receiveData();
    }

    @Test
    public void shouldCloseSocketUponDisconnect() throws Exception
    {
        given:
        aClientConnects();
        framer.doWork();

        when:
        proxy.disconnect(CONNECTION_ID);
        framer.doWork();

        then:
        verify(mockReceiverEndPoint).close();
    }

    private void connect() throws Exception
    {
        given:
        proxy.connect(CONFIGURATION);

        when:
        framer.doWork();
    }

    @Test
    public void shouldConnectToAddress() throws Exception
    {
        connect();

        then:
        assertNotNull("Sender hasn't connected to server", server.accept());
    }

    @Test
    public void shouldReplyWithSocketConnectionError() throws Exception
    {
        server.close();

        connect();

        verify(mockGateway).onInitiationError(any(IOException.class));
    }

    private void aClientConnects() throws IOException
    {
        client = SocketChannel.open(FRAMER_ADDRESS);
    }

    private void aClientSendsData() throws IOException
    {
        clientBuffer.position(0);
        assertEquals("Has written bytes", clientBuffer.remaining(), client.write(clientBuffer));
    }
}
