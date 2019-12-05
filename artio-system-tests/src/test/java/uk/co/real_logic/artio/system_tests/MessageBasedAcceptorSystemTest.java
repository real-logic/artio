/*
 * Copyright 2015-2018 Real Logic Ltd, Adaptive Financial Consulting Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.co.real_logic.artio.system_tests;

import io.aeron.archive.ArchivingMediaDriver;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import uk.co.real_logic.artio.Reply;
import uk.co.real_logic.artio.Timing;
import uk.co.real_logic.artio.builder.Encoder;
import uk.co.real_logic.artio.builder.LogonEncoder;
import uk.co.real_logic.artio.builder.SessionHeaderEncoder;
import uk.co.real_logic.artio.builder.TestRequestEncoder;
import uk.co.real_logic.artio.decoder.LogonDecoder;
import uk.co.real_logic.artio.decoder.RejectDecoder;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.engine.InitialAcceptedSessionOwner;
import uk.co.real_logic.artio.library.FixLibrary;

import java.io.IOException;
import java.net.ConnectException;
import java.util.concurrent.locks.LockSupport;

import static io.aeron.CommonContext.IPC_CHANNEL;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.agrona.CloseHelper.close;
import static org.junit.Assert.*;
import static uk.co.real_logic.artio.TestFixtures.*;
import static uk.co.real_logic.artio.engine.InitialAcceptedSessionOwner.ENGINE;
import static uk.co.real_logic.artio.engine.InitialAcceptedSessionOwner.SOLE_LIBRARY;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.*;
import static uk.co.real_logic.artio.validation.PersistenceLevel.PERSISTENT_SEQUENCE_NUMBERS;
import static uk.co.real_logic.artio.validation.PersistenceLevel.TRANSIENT_SEQUENCE_NUMBERS;

public class MessageBasedAcceptorSystemTest
{
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private int port = unusedPort();

    private ArchivingMediaDriver mediaDriver;
    private FixEngine engine;

    // Trying to reproduce
    // > [8=FIX.4.4|9=0079|35=A|49=initiator|56=acceptor|34=1|52=20160825-10:25:03.931|98=0|108=30|141=Y|10=018]
    // < [8=FIX.4.4|9=0079|35=A|49=acceptor|56=initiator|34=1|52=20160825-10:24:57.920|98=0|108=30|141=N|10=013]
    // < [8=FIX.4.4|9=0070|35=2|49=acceptor|56=initiator|34=3|52=20160825-10:25:27.766|7=1|16=0|10=061]

    @Test
    public void shouldComplyWithLogonBasedSequenceNumberResetOn()
        throws IOException
    {
        shouldComplyWithLogonBasedSequenceNumberReset(true);
    }

    @Test
    public void shouldComplyWithLogonBasedSequenceNumberResetOff()
        throws IOException
    {
        shouldComplyWithLogonBasedSequenceNumberReset(false);
    }

    private void shouldComplyWithLogonBasedSequenceNumberReset(final boolean sequenceNumberReset)
        throws IOException
    {
        setup(sequenceNumberReset, true);

        logonThenLogout();

        logonThenLogout();
    }

    @Test
    public void shouldNotNotifyLibraryOfSessionUntilLoggedOn() throws IOException
    {
        setup(true, true);

        final FakeOtfAcceptor fakeOtfAcceptor = new FakeOtfAcceptor();
        final FakeHandler fakeHandler = new FakeHandler(fakeOtfAcceptor);
        try (FixLibrary library = newAcceptingLibrary(fakeHandler))
        {
            try (FixConnection connection = FixConnection.initiate(port))
            {
                library.poll(10);

                assertFalse(fakeHandler.hasSeenSession());

                logon(connection);

                fakeHandler.awaitSessionIdFor(INITIATOR_ID, ACCEPTOR_ID, () -> library.poll(2), 1000);
            }
        }
    }

    @Test
    public void shouldRejectExceptionalLogonMessage() throws IOException
    {
        setup(true, true);

        try (FixConnection connection = FixConnection.initiate(port))
        {
            sendInvalidLogon(connection);

            final RejectDecoder reject = connection.readMessage(new RejectDecoder());
            assertEquals(1, reject.refSeqNum());
            assertEquals(LogonDecoder.MESSAGE_TYPE_AS_STRING, reject.refMsgTypeAsString());

            connection.logoutAndAwaitReply();
        }
    }

    @Test
    public void shouldRejectExceptionalSessionMessage() throws IOException
    {
        setup(true, true);

        try (FixConnection connection = FixConnection.initiate(port))
        {
            logon(connection);

            sendInvalidTestRequestMessage(connection);

            final RejectDecoder reject = connection.readMessage(new RejectDecoder());
            assertEquals(2, reject.refSeqNum());

            connection.logoutAndAwaitReply();
        }
    }

    @Test
    public void shouldShutdownWithNotLoggedInSessionsOpen() throws IOException
    {
        setup(true, true);

        try (FixConnection connection = FixConnection.initiate(port))
        {
            close(engine);
        }
    }

    @Test
    public void shouldUnbindTcpPortWhenRequested() throws IOException
    {
        setup(true, true);

        try (FixConnection connection = FixConnection.initiate(port))
        {
        }

        completeUnbind();

        cannotConnect();
    }

    @Test
    public void shouldRebindTcpPortWhenRequested() throws IOException
    {
        setup(true, true);

        completeUnbind();

        completeBind();

        try (FixConnection connection = FixConnection.initiate(port))
        {
        }
    }

    @Test
    public void shouldHaveIdempotentBind() throws IOException
    {
        setup(true, true);

        completeBind();

        try (FixConnection connection = FixConnection.initiate(port))
        {
        }

        completeUnbind();
        completeBind();
        completeBind();

        try (FixConnection connection = FixConnection.initiate(port))
        {
        }
    }

    @Test
    public void shouldHaveIdempotentUnbind() throws IOException
    {
        setup(true, true);

        completeUnbind();
        completeUnbind();

        cannotConnect();
    }

    @Test
    public void shouldReturnErrorWhenBindingWithoutAddress()
    {
        setup(true, false, false);

        final Reply<?> reply = engine.bind();
        SystemTestUtil.awaitReply(reply);
        assertEquals(reply.toString(), Reply.State.ERRORED, reply.state());

        assertEquals("Missing address: EngineConfiguration.bindTo()", reply.error().getMessage());
    }

    @Test
    public void shouldNotDisconnectWhenUnbinding() throws IOException
    {
        setup(true, true);

        try (FixConnection connection = FixConnection.initiate(port))
        {
            logon(connection);
            completeUnbind();
            connection.logoutAndAwaitReply();
        }
    }

    @Test
    public void shouldAllowBindingToBeDeferred() throws IOException
    {
        setup(true, false);
        cannotConnect();
    }

    @Test
    public void shouldDisconnectConnectionWithNoLogonEngine() throws IOException
    {
        shouldDisconnectConnectionWithNoLogon(ENGINE);
    }

    @Test
    public void shouldDisconnectConnectionWithNoLogonSoleLibrary() throws IOException
    {
        shouldDisconnectConnectionWithNoLogon(SOLE_LIBRARY);
    }

    private void shouldDisconnectConnectionWithNoLogon(final InitialAcceptedSessionOwner initialAcceptedSessionOwner)
        throws IOException
    {
        setup(true, true, true, initialAcceptedSessionOwner);

        final FakeOtfAcceptor fakeOtfAcceptor = new FakeOtfAcceptor();
        final FakeHandler fakeHandler = new FakeHandler(fakeOtfAcceptor);
        try (FixLibrary library = newAcceptingLibrary(fakeHandler))
        {
            try (FixConnection connection = FixConnection.initiate(port))
            {
                Timing.assertEventuallyTrue(
                    "Never gets disconnected",
                    () ->
                    {
                        library.poll(10);

                        LockSupport.parkNanos(10_000);

                        return !connection.isConnected();
                    });
            }
        }
    }

    private void completeBind()
    {
        final Reply<?> bindReply = engine.bind();
        SystemTestUtil.awaitReply(bindReply);
        assertEquals(bindReply.toString(), Reply.State.COMPLETED, bindReply.state());
    }

    private void completeUnbind()
    {
        final Reply<?> unbindReply = engine.unbind();
        SystemTestUtil.awaitReply(unbindReply);
        assertEquals(unbindReply.toString(), Reply.State.COMPLETED, unbindReply.state());
    }

    private void cannotConnect() throws IOException
    {
        thrown.expect(ConnectException.class);
        FixConnection.initiate(port);
    }

    private void sendInvalidLogon(final FixConnection connection)
    {
        final LogonEncoder logon = new LogonEncoder()
            .resetSeqNumFlag(true)
            .encryptMethod(0)
            .heartBtInt(30)
            .maxMessageSize(9999);

        sendInvalidMessage(connection, logon);
    }

    private void sendInvalidTestRequestMessage(final FixConnection connection)
    {
        final TestRequestEncoder testRequest = new TestRequestEncoder();
        testRequest.testReqID("A");

        sendInvalidMessage(connection, testRequest);
    }

    private void sendInvalidMessage(final FixConnection connection, final Encoder encoder)
    {
        final SessionHeaderEncoder header = encoder.header();

        connection.setupHeader(header, connection.acquireMsgSeqNum(), false);

        header.sendingTime("nonsense".getBytes(US_ASCII));

        connection.send(encoder);
    }

    private void setup(final boolean sequenceNumberReset, final boolean shouldBind)
    {
        setup(sequenceNumberReset, shouldBind, true);
    }

    private void setup(
        final boolean sequenceNumberReset,
        final boolean shouldBind,
        final boolean provideBindingAddress)
    {
        setup(sequenceNumberReset, shouldBind, provideBindingAddress, InitialAcceptedSessionOwner.ENGINE);
    }

    private void setup(
        final boolean sequenceNumberReset,
        final boolean shouldBind,
        final boolean provideBindingAddress,
        final InitialAcceptedSessionOwner initialAcceptedSessionOwner)
    {
        mediaDriver = launchMediaDriver();

        delete(ACCEPTOR_LOGS);
        final EngineConfiguration config = new EngineConfiguration()
            .libraryAeronChannel(IPC_CHANNEL)
            .monitoringFile(acceptorMonitoringFile("engineCounters"))
            .logFileDir(ACCEPTOR_LOGS)
            .initialAcceptedSessionOwner(initialAcceptedSessionOwner)
            .noLogonDisconnectTimeoutInMs(500)
            .sessionPersistenceStrategy(logon ->
            sequenceNumberReset ? TRANSIENT_SEQUENCE_NUMBERS : PERSISTENT_SEQUENCE_NUMBERS)
            .bindAtStartup(shouldBind);

        if (provideBindingAddress)
        {
            config.bindTo("localhost", port);
        }

        config.printErrorMessages(false);
        engine = FixEngine.launch(config);
    }

    private void logonThenLogout() throws IOException
    {
        final FixConnection connection = FixConnection.initiate(port);

        logon(connection);

        connection.logoutAndAwaitReply();

        connection.close();
    }

    private void logon(final FixConnection connection)
    {
        connection.logon(true);

        final LogonDecoder logon = connection.readLogonReply();
        assertTrue(logon.resetSeqNumFlag());
    }

    @After
    public void tearDown()
    {
        close(engine);
        cleanupMediaDriver(mediaDriver);
    }
}
