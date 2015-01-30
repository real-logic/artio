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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.agrona.concurrent.OneToOneConcurrentArrayQueue;
import uk.co.real_logic.fix_gateway.framer.Connection;
import uk.co.real_logic.fix_gateway.framer.ConnectionHandler;
import uk.co.real_logic.fix_gateway.framer.Multiplexer;
import uk.co.real_logic.fix_gateway.framer.Sender;
import uk.co.real_logic.fix_gateway.framer.commands.ReceiverProxy;
import uk.co.real_logic.fix_gateway.framer.commands.SenderCommand;
import uk.co.real_logic.fix_gateway.framer.commands.SenderProxy;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.*;

public class SenderTest
{
    private static final InetSocketAddress ADDRESS = new InetSocketAddress("localhost", 9999);

    private ConnectionHandler mockConnectionHandler = mock(ConnectionHandler.class);
    private Connection mockConnection = mock(Connection.class);
    private OneToOneConcurrentArrayQueue<SenderCommand> commandQueue = new OneToOneConcurrentArrayQueue<>(10);
    private SenderProxy proxy = new SenderProxy(commandQueue);
    private ReceiverProxy mockReceiver = mock(ReceiverProxy.class);
    private Multiplexer mockMultiplexer = mock(Multiplexer.class);

    private Sender sender = new Sender(commandQueue, mockConnectionHandler, mockReceiver, mockMultiplexer);

    private ServerSocketChannel server;

    @Before
    public void setUp() throws IOException
    {
        server = ServerSocketChannel.open().bind(ADDRESS);
        server.configureBlocking(false);

        when(mockConnectionHandler.createConnection(any(SocketChannel.class)))
                .thenReturn(mockConnection);
    }

    @After
    public void tearDown() throws IOException
    {
        server.close();
    }

    @Test
    public void shouldConnectToAddress() throws Exception
    {
        given:
        proxy.connect(ADDRESS);

        when:
        sender.doWork();

        then:
        assertNotNull("Sender hasn't connected to server", server.accept());
    }

    @Test
    public void shouldNotifyReceiverWhenConnectionEstablished() throws Exception
    {
        given:
        proxy.connect(ADDRESS);

        when:
        sender.doWork();

        then:
        verify(mockReceiver).newConnection(mockConnection);
    }

}
