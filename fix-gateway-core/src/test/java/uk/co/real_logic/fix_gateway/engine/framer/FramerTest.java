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
import org.mockito.ArgumentCaptor;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.agrona.concurrent.QueuedPipe;
import uk.co.real_logic.fix_gateway.engine.ConnectionHandler;
import uk.co.real_logic.fix_gateway.engine.EngineConfiguration;
import uk.co.real_logic.fix_gateway.messages.GatewayError;
import uk.co.real_logic.fix_gateway.session.SessionIdStrategy;
import uk.co.real_logic.fix_gateway.streams.GatewayPublication;

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
import static uk.co.real_logic.fix_gateway.messages.GatewayError.*;

public class FramerTest
{
    private static final InetSocketAddress TEST_ADDRESS = new InetSocketAddress("localhost", 9998);
    private static final InetSocketAddress FRAMER_ADDRESS = new InetSocketAddress("localhost", 9999);
    private static final int LIBRARY_ID = 3;
    private static final int REPLY_TIMEOUT_IN_MS = 10;

    private ServerSocketChannel server;

    private SocketChannel client;
    private ByteBuffer clientBuffer = ByteBuffer.allocate(1024);

    private SenderEndPoint mockSenderEndPoint = mock(SenderEndPoint.class);
    private ReceiverEndPoint mockReceiverEndPoint = mock(ReceiverEndPoint.class);
    private ConnectionHandler mockConnectionHandler = mock(ConnectionHandler.class);
    private GatewayPublication mockGatewayPublication = mock(GatewayPublication.class);
    private SessionIdStrategy mockSessionIdStrategy = mock(SessionIdStrategy.class);
    private FakeEpochClock mockClock = new FakeEpochClock();

    private EngineConfiguration engineConfiguration = new EngineConfiguration()
        .bind(FRAMER_ADDRESS.getHostName(), FRAMER_ADDRESS.getPort())
        .replyTimeoutInMs(REPLY_TIMEOUT_IN_MS);

    @SuppressWarnings("unchecked")
    private Framer framer = new Framer(
        mockClock,
        engineConfiguration,
        mockConnectionHandler,
        mock(Subscription.class),
        mock(Subscription.class),
        mockGatewayPublication,
        mockSessionIdStrategy,
        new SessionIds(),
        mock(QueuedPipe.class)
    );

    private ArgumentCaptor<Long> connectionId = ArgumentCaptor.forClass(Long.class);

    @Before
    public void setUp() throws IOException
    {
        server = ServerSocketChannel.open().bind(TEST_ADDRESS);
        server.configureBlocking(false);

        clientBuffer.putInt(10, 5);

        when(mockConnectionHandler
            .receiverEndPoint(any(SocketChannel.class), connectionId.capture(), anyLong(), eq(framer), anyInt()))
            .thenReturn(mockReceiverEndPoint);

        when(mockConnectionHandler.senderEndPoint(any(SocketChannel.class), anyLong()))
            .thenReturn(mockSenderEndPoint);

        when(mockReceiverEndPoint.connectionId()).then(inv -> connectionId.getValue());

        when(mockSenderEndPoint.connectionId()).then(inv -> connectionId.getValue());

        when(mockReceiverEndPoint.libraryId()).thenReturn(LIBRARY_ID);
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

        verify(mockConnectionHandler).receiverEndPoint(
            notNull(SocketChannel.class), anyLong(), anyLong(), eq(framer), eq(LIBRARY_ID));

        verify(mockConnectionHandler).senderEndPoint(
            notNull(SocketChannel.class), anyLong());
    }

    @Test
    public void shouldDisconnectClientIfTheresNoAcceptorLibrary() throws Exception
    {
        client = SocketChannel.open(FRAMER_ADDRESS);

        framer.doWork();

        verify(mockGatewayPublication).saveError(eq(UNKNOWN_LIBRARY), eq(-1), anyString());
    }

    @Test
    public void shouldPassDataToEndPointWhenSent() throws Exception
    {
        aClientConnects();
        framer.doWork();

        aClientSendsData();
        framer.doWork();

        verify(mockReceiverEndPoint).pollForData();
    }

    @Test
    public void shouldCloseSocketUponDisconnect() throws Exception
    {
        aClientConnects();
        framer.doWork();

        framer.onDisconnect(LIBRARY_ID, connectionId.getValue());
        framer.doWork();

        verifyEndpointsClosed();
    }

    private void verifyEndpointsClosed()
    {
        verify(mockReceiverEndPoint).close();
        verify(mockSenderEndPoint).close();
    }

    @Test
    public void shouldConnectToAddress() throws Exception
    {
        intiateConnection();

        assertNotNull("Sender hasn't connected to server", server.accept());
    }

    @Test
    public void shouldNotConnectIfLibraryUnknown() throws Exception
    {
        framer.onInitiateConnection(
            LIBRARY_ID, TEST_ADDRESS.getPort(), TEST_ADDRESS.getHostName(), "LEH_LZJ02", null, null, "CCG");

        framer.doWork();

        assertNull("Sender has connected to server", server.accept());
        verifyErrorPublished(UNKNOWN_LIBRARY);
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

        verifyErrorPublished(UNABLE_TO_CONNECT);
    }

    private void verifyErrorPublished(final GatewayError error)
    {
        verify(mockGatewayPublication).saveError(eq(error), eq(LIBRARY_ID), anyString());
    }

    @Test
    public void shouldIdentifyDuplicateInitiatedSessions() throws Exception
    {
        intiateConnection();

        notifyLibraryOfConnection();

        intiateConnection();

        verifyErrorPublished(DUPLICATE_SESSION);
    }

    @Test
    public void shouldDisconnectInitiatedClientsWhenLibraryDisconnects() throws Exception
    {
        intiateConnection();

        timeoutLibrary();

        framer.doWork();

        verifyEndpointsClosed();
    }

    @Test
    public void shouldDisconnectAcceptedClientsWhenLibraryDisconnects() throws Exception
    {
        aClientConnects();

        timeoutLibrary();

        framer.doWork();

        verifyEndpointsClosed();
    }

    private void timeoutLibrary()
    {
        mockClock.advanceMilliSeconds(REPLY_TIMEOUT_IN_MS * 2);
    }

    private void connectLibrary()
    {
        framer.onApplicationHeartbeat(LIBRARY_ID);
    }

    private void intiateConnection() throws Exception
    {
        connectLibrary();

        framer.onInitiateConnection(
            LIBRARY_ID, TEST_ADDRESS.getPort(), TEST_ADDRESS.getHostName(), "LEH_LZJ02", null, null, "CCG");

        framer.doWork();
    }

    private void aClientConnects() throws IOException
    {
        connectLibrary();

        client = SocketChannel.open(FRAMER_ADDRESS);
    }

    private void notifyLibraryOfConnection()
    {
        verify(mockGatewayPublication).saveConnect(anyLong(), anyString(), eq(LIBRARY_ID), eq(INITIATOR));
        verify(mockGatewayPublication).saveLogon(eq(LIBRARY_ID), anyLong(), anyLong());
    }

    private void aClientSendsData() throws IOException
    {
        clientBuffer.position(0);
        assertEquals("Has written bytes", clientBuffer.remaining(), client.write(clientBuffer));
    }
}
