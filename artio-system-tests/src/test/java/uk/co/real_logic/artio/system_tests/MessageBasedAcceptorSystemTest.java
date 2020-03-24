/*
 * Copyright 2015-2020 Real Logic Limited, Adaptive Financial Consulting Ltd.
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
import org.agrona.CloseHelper;
import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.Test;
import uk.co.real_logic.artio.Reply;
import uk.co.real_logic.artio.Timing;
import uk.co.real_logic.artio.builder.*;
import uk.co.real_logic.artio.decoder.LogonDecoder;
import uk.co.real_logic.artio.decoder.LogoutDecoder;
import uk.co.real_logic.artio.decoder.RejectDecoder;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.engine.InitialAcceptedSessionOwner;
import uk.co.real_logic.artio.library.FixLibrary;
import uk.co.real_logic.artio.library.LibraryConfiguration;
import uk.co.real_logic.artio.session.Session;
import uk.co.real_logic.artio.validation.MessageValidationStrategy;

import java.io.IOException;
import java.net.ConnectException;
import java.util.concurrent.locks.LockSupport;

import static io.aeron.CommonContext.IPC_CHANNEL;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.agrona.CloseHelper.close;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;
import static uk.co.real_logic.artio.SessionRejectReason.COMPID_PROBLEM;
import static uk.co.real_logic.artio.TestFixtures.*;
import static uk.co.real_logic.artio.dictionary.SessionConstants.*;
import static uk.co.real_logic.artio.engine.InitialAcceptedSessionOwner.ENGINE;
import static uk.co.real_logic.artio.engine.InitialAcceptedSessionOwner.SOLE_LIBRARY;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.*;
import static uk.co.real_logic.artio.validation.PersistenceLevel.PERSISTENT_SEQUENCE_NUMBERS;
import static uk.co.real_logic.artio.validation.PersistenceLevel.TRANSIENT_SEQUENCE_NUMBERS;

public class MessageBasedAcceptorSystemTest
{
    private int port = unusedPort();

    private ArchivingMediaDriver mediaDriver;
    private FixEngine engine;
    private FakeOtfAcceptor otfAcceptor;
    private FakeHandler handler;
    private FixLibrary library;
    private TestSystem testSystem;

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

        try (FixConnection ignore = FixConnection.initiate(port))
        {
            close(engine);
        }
    }

    @Test
    public void shouldUnbindTcpPortWhenRequested() throws IOException
    {
        setup(true, true);

        try (FixConnection ignore = FixConnection.initiate(port))
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

        try (FixConnection ignore = FixConnection.initiate(port))
        {
        }
    }

    @Test
    public void shouldHaveIdempotentBind() throws IOException
    {
        setup(true, true);

        completeBind();

        try (FixConnection ignore = FixConnection.initiate(port))
        {
        }

        completeUnbind();
        completeBind();
        completeBind();

        try (FixConnection ignore = FixConnection.initiate(port))
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

    @Test
    public void shouldSupportRapidLogonAndLogoutOperations() throws IOException
    {
        setup(false, true, true, ENGINE);

        final FakeOtfAcceptor otfAcceptor = new FakeOtfAcceptor();
        final FakeHandler handler = new FakeHandler(otfAcceptor);
        final TestSystem testSystem = new TestSystem();
        final Session session;

        try (FixLibrary library = testSystem.connect(acceptingLibraryConfig(handler)))
        {
            try (FixConnection connection = FixConnection.initiate(port))
            {
                connection.logon(false);

                handler.awaitSessionId(testSystem::poll);

                final long sessionId = handler.awaitSessionId(testSystem::poll);
                handler.clearSessionExistsInfos();
                session = acquireSession(handler, library, sessionId, testSystem);
                assertNotNull(session);

                connection.readLogonReply();

                connection.logout();
            }

            try (FixConnection connection = FixConnection.initiate(port))
            {
                // The previous connection hasn't yet been detected as it's still active.
                assertTrue(session.isActive());

                connection.msgSeqNum(3);

                connection.logon(false);

                // During this loop the logout message for the disconnected connection is sent,
                // But not received by the new connection.
                Timing.assertEventuallyTrue("Library has disconnected old session", () ->
                {
                    testSystem.poll();

                    return !handler.sessions().contains(session);
                });

                // Use sequence number 3 to ensure that we're getting a logon reply
                final LogonDecoder logonReply = connection.readLogonReply();
                assertEquals(3, logonReply.header().msgSeqNum());

                final LogoutDecoder logoutDecoder = connection.logoutAndAwaitReply();
                assertEquals(4, logoutDecoder.header().msgSeqNum());
            }
        }
    }

    @Test
    public void shouldRejectMessageWithInvalidSenderAndTargetCompIds() throws IOException
    {
        setup(true, true);
        setupLibrary();

        try (FixConnection connection = FixConnection.initiate(port))
        {
            logon(connection);

            final long sessionId = handler.awaitSessionId(testSystem::poll);
            final Session session = acquireSession(handler, library, sessionId, testSystem);

            final TestRequestEncoder testRequestEncoder = new TestRequestEncoder();
            connection.setupHeader(testRequestEncoder.header(), connection.acquireMsgSeqNum(), false);
            testRequestEncoder.header().senderCompID("Wrong").targetCompID("Values");
            testRequestEncoder.testReqID("ABC");
            connection.send(testRequestEncoder);

            Timing.assertEventuallyTrue("", () ->
            {
                testSystem.poll();

                return session.lastSentMsgSeqNum() >= 2;
            });

            final RejectDecoder rejectDecoder = connection.readReject();
            assertEquals(2, rejectDecoder.refSeqNum());
            assertEquals(TEST_REQUEST_MESSAGE_TYPE_STR, rejectDecoder.refMsgTypeAsString());
            assertEquals(COMPID_PROBLEM, rejectDecoder.sessionRejectReasonAsEnum());

            MatcherAssert.assertThat(rejectDecoder.refTagID(), either(is(SENDER_COMP_ID)).or(is(TARGET_COMP_ID)));
            assertFalse(otfAcceptor.lastReceivedMessage().isValid());
        }
    }

    @Test
    public void shouldRejectInvalidResendRequests() throws IOException
    {
        setup(true, true);

        try (FixConnection connection = FixConnection.initiate(port))
        {
            logon(connection);

            final String testReqId = "ABC";
            connection.sendTestRequest(testReqId);
            final int headerSeqNum = connection.readHeartbeat(testReqId).header().msgSeqNum();

            // Send an invalid resend request
            final ResendRequestEncoder resendRequest = new ResendRequestEncoder();
            resendRequest.beginSeqNo(headerSeqNum).endSeqNo(headerSeqNum);
            connection.setupHeader(resendRequest.header(), 1, false);
            resendRequest.header().targetCompID(" ");
            connection.send(resendRequest);

            connection.readReject();
            connection.readLogout();

            sleepToAwaitResend();

            connection.logout();
            assertFalse("Read a resent FIX message instead of a disconnect", connection.isConnected());
        }
    }

    @Test
    public void shouldSupportProxyProtocol() throws IOException
    {
        setup(true, true);

        handler = new FakeHandler(new FakeOtfAcceptor());
        final TestSystem testSystem = new TestSystem();

        try (FixLibrary library = testSystem.connect(acceptingLibraryConfig(handler)))
        {
            try (FixConnection connection = FixConnection.initiate(port))
            {
                connection.sendProxyV1Line();
                logon(connection);

                final long sessionId = handler.awaitSessionId(testSystem::poll);
                handler.clearSessionExistsInfos();
                final Session session = acquireSession(handler, library, sessionId, testSystem);
                assertNotNull(session);

                assertEquals(FixConnection.PROXY_SOURCE_IP, session.connectedHost());
                assertEquals(FixConnection.PROXY_SOURCE_PORT, session.connectedPort());
            }
        }
    }

    private void sleepToAwaitResend()
    {
        try
        {
            Thread.sleep(200);
        }
        catch (final InterruptedException e)
        {
            e.printStackTrace();
        }
    }

    private void setupLibrary()
    {
        otfAcceptor = new FakeOtfAcceptor();
        handler = new FakeHandler(otfAcceptor);
        final LibraryConfiguration configuration = acceptingLibraryConfig(handler);
        configuration.messageValidationStrategy(MessageValidationStrategy.none());
        library = connect(configuration);
        testSystem = new TestSystem(library);
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
        try
        {
            FixConnection.initiate(port);
        }
        catch (final ConnectException ignore)
        {
            return;
        }

        fail("expected ConnectException");
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
        if (testSystem == null)
        {
            close(engine);
        }
        else
        {
            testSystem.awaitBlocking(() -> CloseHelper.close(engine));
        }

        close(library);

        cleanupMediaDriver(mediaDriver);
    }
}
