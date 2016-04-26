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
import org.junit.Test;
import uk.co.real_logic.fix_gateway.builder.LogonEncoder;
import uk.co.real_logic.fix_gateway.builder.TestRequestEncoder;
import uk.co.real_logic.fix_gateway.engine.EngineConfiguration;
import uk.co.real_logic.fix_gateway.engine.FixEngine;
import uk.co.real_logic.fix_gateway.engine.SessionInfo;
import uk.co.real_logic.fix_gateway.engine.framer.LibraryInfo;
import uk.co.real_logic.fix_gateway.fields.UtcTimestampEncoder;
import uk.co.real_logic.fix_gateway.library.FixLibrary;
import uk.co.real_logic.fix_gateway.session.Session;
import uk.co.real_logic.fix_gateway.util.MutableAsciiBuffer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.List;

import static org.agrona.CloseHelper.close;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static uk.co.real_logic.fix_gateway.TestFixtures.launchMediaDriver;
import static uk.co.real_logic.fix_gateway.TestFixtures.unusedPort;
import static uk.co.real_logic.fix_gateway.system_tests.SystemTestUtil.*;

public class SlowConsumerTest
{
    private static final int MAX_BYTES_IN_BUFFER = 4 * 1024;

    private int port = unusedPort();
    private MediaDriver mediaDriver;
    private FixEngine engine;
    private FixLibrary library;
    private FakeOtfAcceptor acceptingOtfAcceptor = new FakeOtfAcceptor();
    private FakeHandler handler = new FakeHandler(acceptingOtfAcceptor);

    private LogonEncoder logon = new LogonEncoder();
    private MutableAsciiBuffer buffer = new MutableAsciiBuffer(ByteBuffer.allocate(8 * 1024));
    private ByteBuffer byteBuffer = buffer.byteBuffer();
    private SocketChannel socket;

    @Before
    public void setUp()
    {
        mediaDriver = launchMediaDriver();
        delete(ACCEPTOR_LOGS);
        final EngineConfiguration config = acceptingConfig(port, "engineCounters", ACCEPTOR_ID, INITIATOR_ID);
        config.senderMaxBytesInBuffer(MAX_BYTES_IN_BUFFER);
        engine = FixEngine.launch(config);
        library = newAcceptingLibrary(handler);
    }

    @Test(timeout = 10000)
    public void shouldQuarantineThenDisconnectASlowConsumer() throws IOException
    {
        initiateConnection();

        final TestRequestEncoder testRequest = new TestRequestEncoder();
        testRequest.testReqID("some relatively long test req id");

        final Session session = acquireSession(handler, library);
        final SessionInfo sessionInfo = getSessionInfo();

        while (socketIsConnected() || session.canSendMessage())
        {
            if (session.canSendMessage())
            {
                session.send(testRequest);
            }

            library.poll(1);
        }

        assertThat(sessionInfo.bytesInBuffer(), greaterThanOrEqualTo((long) MAX_BYTES_IN_BUFFER));
    }

    private SessionInfo getSessionInfo()
    {
        final List<LibraryInfo> libraries = engine.libraries(ADMIN_IDLE_STRATEGY);
        assertThat(libraries, hasSize(1));
        final LibraryInfo libraryInfo = libraries.get(0);
        final List<SessionInfo> sessions = libraryInfo.sessions();
        assertThat(sessions, hasSize(1));
        return sessions.get(0);
    }

    private void initiateConnection() throws IOException
    {
        socket = SocketChannel.open(new InetSocketAddress("localhost", port));

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
        byteBuffer.limit(length);
        assertEquals(length, socket.write(byteBuffer));
    }

    private boolean socketIsConnected()
    {
        // Need to poke to TCP connection to detect disconnection
        try
        {
            byteBuffer.position(0).limit(1);
            socket.write(byteBuffer);
            return true;
        }
        catch (final IOException e)
        {
            return false;
        }
    }

    @After
    public void cleanup()
    {
        close(engine);
        close(mediaDriver);
        close(socket);
    }
}
