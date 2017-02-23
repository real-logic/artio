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
import org.junit.Test;
import uk.co.real_logic.fix_gateway.Timing;
import uk.co.real_logic.fix_gateway.builder.LogonEncoder;
import uk.co.real_logic.fix_gateway.builder.TestRequestEncoder;
import uk.co.real_logic.fix_gateway.engine.EngineConfiguration;
import uk.co.real_logic.fix_gateway.engine.FixEngine;
import uk.co.real_logic.fix_gateway.engine.SessionInfo;
import uk.co.real_logic.fix_gateway.engine.framer.LibraryInfo;
import uk.co.real_logic.fix_gateway.fields.UtcTimestampEncoder;
import uk.co.real_logic.fix_gateway.library.FixLibrary;
import uk.co.real_logic.fix_gateway.library.LibraryConfiguration;
import uk.co.real_logic.fix_gateway.messages.SessionReplyStatus;
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
import static org.junit.Assert.*;
import static uk.co.real_logic.fix_gateway.TestFixtures.*;
import static uk.co.real_logic.fix_gateway.engine.EngineConfiguration.DEFAULT_SENDER_MAX_BYTES_IN_BUFFER;
import static uk.co.real_logic.fix_gateway.messages.SessionState.ACTIVE;
import static uk.co.real_logic.fix_gateway.system_tests.SystemTestUtil.*;

public class SlowConsumerTest
{
    private static final int BUFFER_CAPACITY = 8 * 1024;
    private static final int TEST_TIMEOUT = 20_000;

    private int port = unusedPort();
    private MediaDriver mediaDriver;
    private FixEngine engine;
    private FixLibrary library;
    private FakeOtfAcceptor acceptingOtfAcceptor = new FakeOtfAcceptor();
    private FakeHandler handler = new FakeHandler(acceptingOtfAcceptor);

    private TestRequestEncoder testRequest = newTestRequest();
    private LogonEncoder logon = new LogonEncoder();
    private ByteBuffer byteBuffer = ByteBuffer.allocateDirect(BUFFER_CAPACITY);
    private MutableAsciiBuffer buffer = new MutableAsciiBuffer(byteBuffer);
    private SteppingIdleStrategy framerIdleStrategy = new SteppingIdleStrategy();
    private SocketChannel socket;
    private Session session;

    @Test(timeout = TEST_TIMEOUT)
    public void shouldQuarantineThenDisconnectASlowConsumer() throws IOException
    {
        final int senderMaxBytesInBuffer = 8 * 1024;
        setup(senderMaxBytesInBuffer);

        initiateConnection();

        session = acquireSession(handler, library);
        final SessionInfo sessionInfo = getSessionInfo();

        while (!socketIsConnected())
        {
            library.poll(1);
        }

        assertNotSlow(session);
        framerIdleStrategy.startStepping();

        while (socketIsConnected())
        {
            if (session.canSendMessage())
            {
                session.send(testRequest);
            }

            library.poll(1);
            framerIdleStrategy.step();
            framerIdleStrategy.step();
        }

        bytesInBufferAtLeast(sessionInfo, (long) senderMaxBytesInBuffer);

        framerIdleStrategy.stopStepping();
    }

    @Test(timeout = TEST_TIMEOUT)
    public void shouldRestoreConnectionFromSlowGroupWhenItCatchesUp() throws IOException
    {
        final SessionInfo sessionInfo = sessionBecomesSlow();
        socket.configureBlocking(false);

        // Get out of slow state
        while (sessionInfo.bytesInBuffer() > 0 || handler.isSlow(session))
        {
            int bytesRead;
            do
            {
                byteBuffer.position(1).limit(BUFFER_CAPACITY);
                bytesRead = socket.read(byteBuffer);
            }
            while (bytesRead > 0);

            session.send(testRequest);

            library.poll(1);
            framerIdleStrategy.step();
        }

        assertNotSlow(session);
        framerIdleStrategy.stopStepping();

        assertEquals(ACTIVE, session.state());
        assertTrue(socketIsConnected());
    }

    @Test(timeout = TEST_TIMEOUT)
    public void shouldNotifyLibraryOfSlowConnectionWhenAcquired() throws IOException
    {
        sessionBecomesSlow();

        framerIdleStrategy.stopStepping();

        assertEquals(SessionReplyStatus.OK, releaseToGateway(library, session));

        session = acquireSession(handler, library, session.id());

        assertTrue("Session not slow", handler.lastSessionWasSlow());
    }

    private SessionInfo sessionBecomesSlow() throws IOException
    {
        setup(DEFAULT_SENDER_MAX_BYTES_IN_BUFFER);

        initiateConnection();

        session = acquireSession(handler, library);
        final SessionInfo sessionInfo = getSessionInfo();

        assertNotSlow(session);

        framerIdleStrategy.startStepping();

        // Get into a slow state
        while (sessionInfo.bytesInBuffer() == 0 || !handler.isSlow(session))
        {
            for (int i = 0; i < 10; i++)
            {
                session.send(testRequest);
            }

            library.poll(1);
            framerIdleStrategy.step();
        }

        assertTrue(handler.isSlow(session));
        return sessionInfo;
    }

    private void assertNotSlow(final Session session)
    {
        assertFalse(handler.isSlow(session));
    }

    private void bytesInBufferAtLeast(final SessionInfo sessionInfo, final long bytesInBuffer)
    {
        Timing.assertEventuallyTrue("Buffer doesn't have enough bytes in", () ->
        {
            assertThat(sessionInfo.bytesInBuffer(), greaterThanOrEqualTo(bytesInBuffer));
            framerIdleStrategy.step();
        });
    }

    private TestRequestEncoder newTestRequest()
    {
        final TestRequestEncoder testRequest = new TestRequestEncoder();
        testRequest.testReqID("some relatively long test req id");
        return testRequest;
    }

    private SessionInfo getSessionInfo()
    {
        final List<LibraryInfo> libraries = libraries(engine);
        assertThat(libraries, hasSize(2));
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
            return socket.write(byteBuffer) >= 0;
        }
        catch (final IOException e)
        {
            return false;
        }
    }

    @After
    public void cleanup()
    {
        close(library);
        close(engine);
        cleanupMediaDriver(mediaDriver);
        close(socket);
    }

    private void setup(final int senderMaxBytesInBuffer) throws IOException
    {
        mediaDriver = launchMediaDriver(8 * 1024 * 1024);
        delete(ACCEPTOR_LOGS);
        final EngineConfiguration config = acceptingConfig(port, "engineCounters", ACCEPTOR_ID, INITIATOR_ID)
            .framerIdleStrategy(framerIdleStrategy);
        config.senderMaxBytesInBuffer(senderMaxBytesInBuffer);
        engine = FixEngine.launch(config);
        final LibraryConfiguration libraryConfiguration = acceptingLibraryConfig(handler);
        libraryConfiguration.outboundMaxClaimAttempts(1);
        library = connect(libraryConfiguration);
    }
}
