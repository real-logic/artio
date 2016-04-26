/*
 * Copyright 2015-2016 Real Logic Ltd.
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
package uk.co.real_logic.fix_gateway.system_tests;

import io.aeron.driver.MediaDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import uk.co.real_logic.fix_gateway.builder.LogonEncoder;
import uk.co.real_logic.fix_gateway.builder.TestRequestEncoder;
import uk.co.real_logic.fix_gateway.engine.FixEngine;
import uk.co.real_logic.fix_gateway.fields.UtcTimestampEncoder;
import uk.co.real_logic.fix_gateway.library.FixLibrary;
import uk.co.real_logic.fix_gateway.session.Session;
import uk.co.real_logic.fix_gateway.util.MutableAsciiBuffer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import static org.agrona.CloseHelper.close;
import static org.junit.Assert.assertEquals;
import static uk.co.real_logic.fix_gateway.TestFixtures.launchMediaDriver;
import static uk.co.real_logic.fix_gateway.TestFixtures.unusedPort;
import static uk.co.real_logic.fix_gateway.system_tests.SystemTestUtil.*;

public class SlowConsumerTest
{
    private int port = unusedPort();
    private MediaDriver mediaDriver;
    private FixEngine engine;
    private FixLibrary library;
    private FakeOtfAcceptor acceptingOtfAcceptor = new FakeOtfAcceptor();
    private FakeHandler handler = new FakeHandler(acceptingOtfAcceptor);

    private LogonEncoder logon = new LogonEncoder();
    private MutableAsciiBuffer buffer = new MutableAsciiBuffer(ByteBuffer.allocate(8 * 1024));

    @Before
    public void setUp()
    {
        mediaDriver = launchMediaDriver();
        engine = launchAcceptingEngine(port, ACCEPTOR_ID, INITIATOR_ID);
        library = newAcceptingLibrary(handler);
    }

    @Ignore
    @Test
    public void shouldSeperateThenDisconnectSlowConsumer() throws IOException
    {
        final SocketChannel channel = SocketChannel.open(new InetSocketAddress("localhost", port));

        final UtcTimestampEncoder timestamp = new UtcTimestampEncoder();
        timestamp.encode(System.currentTimeMillis());
        logon
            .heartBtInt(10)
            .header()
                .sendingTime(timestamp.buffer())
                .msgSeqNum(1)
                .senderCompID(INITIATOR_ID)
                .targetCompID(ACCEPTOR_ID);

        final int length = logon.encode(buffer, 0);
        final ByteBuffer byteBuffer = buffer.byteBuffer();
        byteBuffer.limit(length);
        assertEquals(length, channel.write(byteBuffer));

        final TestRequestEncoder testRequest = new TestRequestEncoder();
        testRequest.testReqID("some relatively long test req id");

        final Session session = acquireSession(handler, library);
        while (channel.isConnected())
        {
            final long position = session.send(testRequest);
            if (position < 0)
            {
                break;
            }
        }
    }

    @After
    public void cleanup()
    {
        close(engine);
        close(mediaDriver);
    }
}
