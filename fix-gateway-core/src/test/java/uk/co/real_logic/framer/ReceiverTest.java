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
import uk.co.real_logic.fix_gateway.framer.Connection;
import uk.co.real_logic.fix_gateway.framer.ConnectionHandler;
import uk.co.real_logic.fix_gateway.framer.Receiver;

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

    private SocketChannel client;
    private ByteBuffer clientBuffer = ByteBuffer.allocate(1024);

    private ConnectionHandler mockConnectionHandler = mock(ConnectionHandler.class);
    private Connection mockConnection = mock(Connection.class);

    private Receiver receiver = new Receiver(ADDRESS, mockConnectionHandler);

    @Before
    public void setUp() throws IOException
    {
        clientBuffer.putInt(10, 5);

        when(mockConnectionHandler.createConnection(any(SocketChannel.class)))
            .thenReturn(mockConnection);

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
        verify(mockConnectionHandler).createConnection(notNull(SocketChannel.class));
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
        verify(mockConnection).receiveData();
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
