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

import org.junit.Ignore;
import org.junit.Test;
import uk.co.real_logic.fix_gateway.framer.Dispatcher;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;

import static org.junit.Assert.assertTrue;

public class DispatcherTest
{

    private static final InetSocketAddress ADDRESS = new InetSocketAddress("localhost", 9999);

    private SocketChannel client;

    private Dispatcher dispatcher = new Dispatcher(ADDRESS);

    @Test
    public void shouldListenOnSpecifiedPort() throws IOException
    {
        client = SocketChannel.open(ADDRESS);
        assertTrue("Client has failed to connect", client.finishConnect());
    }

    @Ignore
    @Test
    public void shouldInitiateFramerWhenClientConnects()
    {

    }

}
