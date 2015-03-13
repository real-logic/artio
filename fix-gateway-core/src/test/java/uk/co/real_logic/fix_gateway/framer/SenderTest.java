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
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.agrona.concurrent.OneToOneConcurrentArrayQueue;
import uk.co.real_logic.fix_gateway.FixGateway;
import uk.co.real_logic.fix_gateway.SessionConfiguration;
import uk.co.real_logic.fix_gateway.commands.ReceiverProxy;
import uk.co.real_logic.fix_gateway.commands.SenderCommand;
import uk.co.real_logic.fix_gateway.commands.SenderProxy;
import uk.co.real_logic.fix_gateway.commands.SessionManagerProxy;
import uk.co.real_logic.fix_gateway.framer.session.InitiatorSession;
import uk.co.real_logic.fix_gateway.framer.session.Session;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

public class SenderTest
{
    private static final InetSocketAddress ADDRESS = new InetSocketAddress("localhost", 9999);

    private static final SessionConfiguration CONFIGURATION = SessionConfiguration
        .builder()
        .address(ADDRESS.getHostName(), ADDRESS.getPort())
        .senderCompId("LEH_LZJ02")
        .targetCompId("CCG")
        .build();

    private SenderEndPoint mockSenderEndPoint = mock(SenderEndPoint.class);
    private ReceiverEndPoint mockReceiverEndPoint = mock(ReceiverEndPoint.class);
    private ConnectionHandler mockConnectionHandler = mock(ConnectionHandler.class);
    private ReceiverProxy mockReceiver = mock(ReceiverProxy.class);
    private SessionManagerProxy mockSessionManager = mock(SessionManagerProxy.class);
    private FixGateway mockGateway = mock(FixGateway.class);
    private Multiplexer mockMultiplexer = mock(Multiplexer.class);
    private InitiatorSession mockSession = mock(InitiatorSession.class);
    private Subscription mockDataSubscription = mock(Subscription.class);

    private OneToOneConcurrentArrayQueue<SenderCommand> commandQueue = new OneToOneConcurrentArrayQueue<>(10);
    private SenderProxy proxy = new SenderProxy(commandQueue);

    private Sender sender = new Sender(commandQueue, mockConnectionHandler, mockReceiver, mockSessionManager,
        mockGateway, mockMultiplexer, mockDataSubscription);

    private ServerSocketChannel server;

    @Before
    public void setUp() throws IOException
    {
        server = ServerSocketChannel.open().bind(ADDRESS);
        server.configureBlocking(false);

        when(mockConnectionHandler.receiverEndPoint(any(SocketChannel.class), anyLong(), any(Session.class)))
            .thenReturn(mockReceiverEndPoint);

        when(mockConnectionHandler.senderEndPoint(any(SocketChannel.class), anyLong()))
            .thenReturn(mockSenderEndPoint);

        when(mockConnectionHandler.initiateSession(anyLong(), eq(mockGateway), eq(CONFIGURATION))).thenReturn(mockSession);
    }

    @After
    public void tearDown() throws IOException
    {
        server.close();
    }

    @Test
    public void shouldConnectToAddress() throws Exception
    {
        connect();

        then:
        assertNotNull("Sender hasn't connected to server", server.accept());
    }

    @Test
    public void shouldNotifyReceiverWhenConnectionEstablished() throws Exception
    {
        connect();

        then:
        verify(mockReceiver).newInitiatedConnection(mockReceiverEndPoint);
    }

    @Test
    public void shouldNotifySessionManagerWhenConnectionEstablished() throws Exception
    {
        connect();

        then:
        verify(mockSessionManager).newSession(mockSession);
    }

    private void connect() throws Exception
    {
        given:
        proxy.connect(ADDRESS, CONFIGURATION);

        when:
        sender.doWork();
    }
}
