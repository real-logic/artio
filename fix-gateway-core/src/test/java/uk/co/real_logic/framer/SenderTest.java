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

import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.agrona.concurrent.OneToOneConcurrentArrayQueue;
import uk.co.real_logic.fix_gateway.commands.SenderCommand;
import uk.co.real_logic.fix_gateway.framer.Sender;
import uk.co.real_logic.fix_gateway.framer.SenderProxy;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ServerSocketChannel;

import static org.junit.Assert.assertNotNull;

public class SenderTest
{
    private static final InetSocketAddress ADDRESS = new InetSocketAddress("localhost", 9999);

    private final OneToOneConcurrentArrayQueue<SenderCommand> commandQueue = new OneToOneConcurrentArrayQueue<>(10);
    private final SenderProxy proxy = new SenderProxy(commandQueue);
    private final Sender sender = new Sender(commandQueue);

    private ServerSocketChannel server;

    @Before
    public void setUp() throws IOException
    {
        server = ServerSocketChannel.open().bind(ADDRESS);
        server.configureBlocking(false);
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
}
