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
package uk.co.real_logic.fix_gateway.engine.framer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.fix_gateway.StaticConfiguration;
import uk.co.real_logic.fix_gateway.engine.ConnectionHandler;
import uk.co.real_logic.fix_gateway.replication.GatewayPublication;
import uk.co.real_logic.fix_gateway.session.SessionIdStrategy;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import static org.junit.Assert.*;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.notNull;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.fix_gateway.messages.ConnectionType.INITIATOR;
import static uk.co.real_logic.fix_gateway.messages.GatewayError.DUPLICATE_SESSION;
import static uk.co.real_logic.fix_gateway.messages.GatewayError.EXCEPTION;

public class FramerTest
{
    private static final InetSocketAddress TEST_ADDRESS = new InetSocketAddress("localhost", 9998);
    private static final InetSocketAddress FRAMER_ADDRESS = new InetSocketAddress("localhost", 9999);
    private static final long CONNECTION_ID = 2L;
    private static final int LIBRARY_ID = 3;

    private ServerSocketChannel server;

    private SocketChannel client;
    private ByteBuffer clientBuffer = ByteBuffer.allocate(1024);

    private SenderEndPoint mockSenderEndPoint = mock(SenderEndPoint.class);
    private ReceiverEndPoint mockReceiverEndPoint = mock(ReceiverEndPoint.class);
    private ConnectionHandler mockConnectionHandler = mock(ConnectionHandler.class);
    private GatewayPublication mockGatewayPublication = mock(GatewayPublication.class);
    private SessionIdStrategy mockSessionIdStrategy = mock(SessionIdStrategy.class);
    private final Multiplexer mockMultiplexer = mock(Multiplexer.class);

    private StaticConfiguration staticConfiguration = new StaticConfiguration()
        .bind(FRAMER_ADDRESS.getHostName(), FRAMER_ADDRESS.getPort());

    private Framer framer = new Framer(staticConfiguration, mockConnectionHandler, mockMultiplexer,
        mock(Subscription.class), mockGatewayPublication, mockSessionIdStrategy, new SessionIds());

    @Before
    public void setUp() throws IOException
    {
        server = ServerSocketChannel.open().bind(TEST_ADDRESS);

        clientBuffer.putInt(10, 5);

        when(mockConnectionHandler.receiverEndPoint(any(SocketChannel.class), anyLong(), anyLong()))
            .thenReturn(mockReceiverEndPoint);

        when(mockConnectionHandler.senderEndPoint(any(SocketChannel.class), anyLong()))
            .thenReturn(mockSenderEndPoint);

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
        aClientConnects();

        assertTrue("Client has failed to connect", client.finishConnect());
    }

    @Test
    public void shouldCreateEndPointWhenClientConnects() throws Exception
    {
        aClientConnects();

        framer.doWork();

        verify(mockConnectionHandler).receiverEndPoint(notNull(SocketChannel.class), anyLong(), anyLong());
    }

    @Test
    public void shouldPassDataToEndPointWhenSent() throws Exception
    {
        aClientConnects();
        framer.doWork();

        aClientSendsData();
        framer.doWork();

        verify(mockReceiverEndPoint).receiveData();
    }

    @Test
    public void shouldCloseSocketUponDisconnect() throws Exception
    {
        aClientConnects();
        framer.doWork();

        framer.onDisconnect(CONNECTION_ID);
        framer.doWork();

        verify(mockReceiverEndPoint).close();
    }

    @Test
    public void shouldConnectToAddress() throws Exception
    {
        intiateConnection();

        assertNotNull("Sender hasn't connected to server", server.accept());
    }

    @Test
    public void shouldNotifyLibraryOfInitiatedConnection() throws Exception
    {
        intiateConnection();

        notifyLibraryOfConnection();
    }

    @Test
    public void shouldReplyWithSocketConnectionError() throws Exception
    {
        server.close();

        intiateConnection();

        verify(mockGatewayPublication).saveError(eq(EXCEPTION), eq(LIBRARY_ID), anyString());
    }

    @Test
    public void shouldIdentifyDuplicateInitiatedSessions() throws Exception
    {
        intiateConnection();

        notifyLibraryOfConnection();

        intiateConnection();

        verify(mockGatewayPublication).saveError(DUPLICATE_SESSION, LIBRARY_ID, "");
    }

    @Test
    public void shouldRemoveDisconnectedEndpoint() throws Exception
    {
        intiateConnection();
        notifyLibraryOfConnection();

        when(mockReceiverEndPoint.hasDisconnected()).thenReturn(true);

        framer.doWork();

        verify(mockMultiplexer).onDisconnect(CONNECTION_ID);
    }

    private void intiateConnection() throws Exception
    {
        framer.onInitiateConnection(LIBRARY_ID, TEST_ADDRESS.getPort(), TEST_ADDRESS.getHostName(), "LEH_LZJ02", "CCG");

        framer.doWork();
    }

    private void aClientConnects() throws IOException
    {
        client = SocketChannel.open(FRAMER_ADDRESS);
    }

    private void notifyLibraryOfConnection()
    {
        verify(mockGatewayPublication).saveConnect(anyLong(), anyString(), eq(INITIATOR));
        verify(mockGatewayPublication).saveLogon(anyLong(), anyLong());
    }

    private void aClientSendsData() throws IOException
    {
        clientBuffer.position(0);
        assertEquals("Has written bytes", clientBuffer.remaining(), client.write(clientBuffer));
    }
}
