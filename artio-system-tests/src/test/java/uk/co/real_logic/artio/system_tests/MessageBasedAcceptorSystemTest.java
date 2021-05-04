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

import org.junit.Test;
import uk.co.real_logic.artio.Constants;
import uk.co.real_logic.artio.Side;
import uk.co.real_logic.artio.Timing;
import uk.co.real_logic.artio.builder.*;
import uk.co.real_logic.artio.decoder.*;
import uk.co.real_logic.artio.engine.SessionInfo;
import uk.co.real_logic.artio.library.FixLibrary;
import uk.co.real_logic.artio.messages.InitialAcceptedSessionOwner;
import uk.co.real_logic.artio.session.Session;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.function.BiConsumer;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.agrona.CloseHelper.close;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static uk.co.real_logic.artio.Constants.RESEND_REQUEST_MESSAGE_AS_STR;
import static uk.co.real_logic.artio.SessionRejectReason.COMPID_PROBLEM;
import static uk.co.real_logic.artio.dictionary.SessionConstants.*;
import static uk.co.real_logic.artio.messages.InitialAcceptedSessionOwner.ENGINE;
import static uk.co.real_logic.artio.messages.InitialAcceptedSessionOwner.SOLE_LIBRARY;
import static uk.co.real_logic.artio.system_tests.AbstractGatewayToGatewaySystemTest.TEST_TIMEOUT_IN_MS;
import static uk.co.real_logic.artio.system_tests.FixConnection.BUFFER_SIZE;
import static uk.co.real_logic.artio.system_tests.MessageBasedInitiatorSystemTest.assertConnectionDisconnects;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.*;

public class MessageBasedAcceptorSystemTest extends AbstractMessageBasedAcceptorSystemTest
{
    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldComplyWithLogonBasedSequenceNumberResetOn()
        throws IOException
    {
        shouldComplyWithLogonBasedSequenceNumberReset(true);
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
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

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldNotNotifyLibraryOfSessionUntilLoggedOn() throws IOException
    {
        setup(true, true);

        final FakeOtfAcceptor fakeOtfAcceptor = new FakeOtfAcceptor();
        final FakeHandler fakeHandler = new FakeHandler(fakeOtfAcceptor);
        try (FixLibrary library = newAcceptingLibrary(fakeHandler, nanoClock))
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

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldRejectExceptionalLogonMessageAndLogout() throws IOException
    {
        setup(true, true);

        try (FixConnection connection = FixConnection.initiate(port))
        {
            sendInvalidLogon(connection);

            final RejectDecoder reject = connection.readMessage(new RejectDecoder());
            assertEquals(1, reject.refSeqNum());
            assertEquals(Constants.SENDING_TIME, reject.refTagID());
            assertEquals(LogonDecoder.MESSAGE_TYPE_AS_STRING, reject.refMsgTypeAsString());

            connection.readLogout();
        }
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldRejectExceptionalSessionMessage() throws IOException
    {
        setup(true, true);

        try (FixConnection connection = FixConnection.initiate(port))
        {
            logon(connection);

            sendInvalidTestRequestMessage(connection);

            final RejectDecoder reject = connection.readMessage(new RejectDecoder());
            assertEquals(2, reject.refSeqNum());
            assertEquals(Constants.SENDING_TIME, reject.refTagID());

            connection.logoutAndAwaitReply();
        }
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldShutdownWithNotLoggedInSessionsOpen() throws IOException
    {
        setup(true, true);

        try (FixConnection ignore = FixConnection.initiate(port))
        {
            close(engine);
        }
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldDisconnectConnectionWithNoLogonEngine() throws IOException
    {
        shouldDisconnectConnectionWithNoLogon(ENGINE);
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldDisconnectConnectionWithNoLogonSoleLibrary() throws IOException
    {
        shouldDisconnectConnectionWithNoLogon(SOLE_LIBRARY);
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldDisconnectConnectionWithNoLogoutReply() throws IOException
    {
        setup(true, true);

        setupLibrary();

        try (FixConnection connection = FixConnection.initiate(port))
        {
            connection.logon(true, 1);

            final LogonDecoder logon = connection.readLogonReply();
            assertTrue(logon.resetSeqNumFlag());

            final Session session = acquireSession();
            logoutSession(session);
            assertSessionDisconnected(testSystem, session);

            assertConnectionDisconnects(testSystem, connection);
        }
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldSupportRapidLogonAndLogoutOperations() throws IOException
    {
        setup(false, true, true);

        setupLibrary();

        final Session session;

        try (FixConnection connection = FixConnection.initiate(port))
        {
            connection.logon(false);

            handler.awaitSessionId(testSystem::poll);

            session = acquireSession();

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

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldRejectMessageWithInvalidSenderAndTargetCompIds() throws IOException
    {
        setup(true, true);
        setupLibrary();

        try (FixConnection connection = FixConnection.initiate(port))
        {
            logon(connection);

            final Session session = acquireSession();

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

            assertThat(rejectDecoder.refTagID(), either(is(SENDER_COMP_ID)).or(is(TARGET_COMP_ID)));
            assertFalse(otfAcceptor.lastReceivedMessage().isValid());
        }
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldRejectInvalidResendRequestsWrongCompId() throws IOException
    {
        setup(true, true);

        try (FixConnection connection = FixConnection.initiate(port))
        {
            logon(connection);

            final String testReqId = "ABC";
            final int headerSeqNum = connection.exchangeTestRequestHeartbeat(testReqId).header().msgSeqNum();

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

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldRejectInvalidResendRequestsHighBeginSeqNo() throws IOException
    {
        setup(true, true);
        setupLibrary();

        try (FixConnection connection = FixConnection.initiate(port))
        {
            logon(connection);

            final String testReqId = "ABC";
            connection.exchangeTestRequestHeartbeat(testReqId).header().msgSeqNum();

            session = acquireSession();
            ReportFactory.sendOneReport(testSystem, session, Side.SELL);

            testSystem.awaitBlocking(() ->
            {
                final int reportSeqNum = connection.readExecutionReport().header().msgSeqNum();

                // Send an invalid resend request
                final int invalidSeqNum = reportSeqNum + 1;
                final ResendRequestEncoder resendRequest = connection.sendResendRequest(invalidSeqNum, invalidSeqNum);

                final RejectDecoder reject = connection.readReject();
                assertEquals(RESEND_REQUEST_MESSAGE_AS_STR, reject.refMsgTypeAsString());
                assertEquals(resendRequest.header().msgSeqNum(), reject.refSeqNum());

                connection.logout();
                connection.readLogout();
                assertFalse("Read a resent FIX message instead of a disconnect", connection.isConnected());
            });
        }
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldReplyWithOnlyValidMessageSequenceWithHighEndSeqNo() throws IOException
    {
        setup(true, true);
        setupLibrary();

        try (FixConnection connection = FixConnection.initiate(port))
        {
            logon(connection);

            final String testReqId = "ABC";
            final int headerSeqNum = connection.exchangeTestRequestHeartbeat(testReqId).header().msgSeqNum();

            session = acquireSession();
            ReportFactory.sendOneReport(testSystem, session, Side.SELL);

            testSystem.awaitBlocking(() ->
            {
                final int reportSeqNum = connection.readExecutionReport().header().msgSeqNum();

                sleepToAwaitResend();

                // Send an invalid resend request
                final int invalidSeqNum = reportSeqNum + 100;
                connection.sendResendRequest(headerSeqNum, invalidSeqNum);

                final SequenceResetDecoder sequenceResetDecoder = connection.readMessage(new SequenceResetDecoder());
                assertTrue(sequenceResetDecoder.header().possDupFlag());
                assertEquals(reportSeqNum, sequenceResetDecoder.newSeqNo());
                final ExecutionReportDecoder secondExecutionReport = connection.readExecutionReport();
                assertTrue(secondExecutionReport.header().possDupFlag());
                assertEquals(reportSeqNum, secondExecutionReport.header().msgSeqNum());

                sleepToAwaitResend();

                final HeartbeatDecoder heartbeat = connection.exchangeTestRequestHeartbeat("ABC2");
                assertFalse(heartbeat.header().hasPossDupFlag());
                assertEquals(reportSeqNum + 1, heartbeat.header().msgSeqNum());

                connection.logout();
                connection.readLogout();
                assertFalse("Read a resent FIX message instead of a disconnect", connection.isConnected());
            });
        }
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldRejectInvalidLogonWithMissingTargetCompId()
    {
        setup(true, true, true, SOLE_LIBRARY);

        setupLibrary();

        testSystem.awaitBlocking(() ->
        {
            //  Create a logon message that will fail session level validation, but nothing else.
            try (FixConnection connection = new FixConnection(
                SocketChannel.open(new InetSocketAddress("localhost", port)),
                INITIATOR_ID,
                "\000"))
            {
                final LogonEncoder logon = new LogonEncoder();
                connection.setupHeader(logon.header(), connection.acquireMsgSeqNum(), false);

                logon
                    .encryptMethod(0)
                    .heartBtInt(20)
                    .username("AAAAAAAAA")
                    .password("asd");

                final MutableAsciiBuffer asciiBuffer = new MutableAsciiBuffer(new byte[BUFFER_SIZE]);
                final long result = logon.encode(asciiBuffer, 0);
                final int offset = Encoder.offset(result);
                final int length = Encoder.length(result);

                // Remove the acceptor id whilst keeping the checksum the same
                final byte[] badLogon = asciiBuffer.getAscii(offset, length)
                    .replace("\000", "")
                    .replace(INITIATOR_ID, INITIATOR_ID + "\000")
                    .getBytes(US_ASCII);

                connection.sendBytes(badLogon);
                assertFalse(connection.isConnected());
            }
            catch (final IOException e)
            {
                e.printStackTrace();
            }
        });

        final List<SessionInfo> sessions = engine.allSessions();
        assertThat("sessions = " + sessions, sessions, hasSize(0));
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldSupportLogonBasedSequenceNumberResetWithImmediateMessageSend() throws IOException
    {
        shouldSupportLogonBasedSequenceNumberReset(ENGINE, (connection, reportFactory) ->
        {
            connection.msgSeqNum(1).logon(true);

            testSystem.awaitReceivedSequenceNumber(session, 1);
            reportFactory.sendReport(testSystem, session, Side.SELL);
            assertEquals(3, session.lastSentMsgSeqNum());
            connection.readExecutionReport(3);
        });
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldSupportLogonBasedSequenceNumberResetWithLowSequenceNumberReconnect() throws IOException
    {
        shouldSupportLogonBasedSequenceNumberReset(SOLE_LIBRARY, (connection, reportFactory) ->
        {
            // Replicate a buggy client triggering a disconnect whilst the session is performing a
            // sequence number reset that results in a logon not being replied to
            connection.sendExecutionReport(1, false);
            assertSessionDisconnected(testSystem, session);
            assertEquals("MsgSeqNum too low, expecting 3 but received 1",
                connection.readLogout().textAsString());
            assertFalse(connection.isConnected());
        });

        try (FixConnection connection = FixConnection.initiate(port))
        {
            awaitedLogon(connection);
        }
    }

    private void shouldSupportLogonBasedSequenceNumberReset(
        final InitialAcceptedSessionOwner owner,
        final BiConsumer<FixConnection, ReportFactory> onNext)
        throws IOException
    {
        // Replicates a bug reported where if you send a message on a FIX session after a tryResetSequenceNumbers
        // and before the counter-party replies with their logon then it can result in an infinite logon loop.

        setup(true, true, true, owner);
        setupLibrary();

        try (FixConnection connection = FixConnection.initiate(port))
        {
            awaitedLogon(connection);

            final ReportFactory reportFactory = new ReportFactory();

            if (owner == ENGINE)
            {
                session = acquireSession();
            }
            else
            {
                session = handler.lastSession();
                assertNotNull(session);
            }
            reportFactory.sendReport(testSystem, session, Side.SELL);
            connection.readExecutionReport(2);
            connection.sendExecutionReport(2, false);
            testSystem.awaitReceivedSequenceNumber(session, 2);

            testSystem.awaitSend(session::tryResetSequenceNumbers);
            reportFactory.sendReport(testSystem, session, Side.SELL);
            assertEquals(2, session.lastSentMsgSeqNum());
            assertEquals(2, session.lastReceivedMsgSeqNum());

            assertTrue(connection.readLogon(1).resetSeqNumFlag());
            connection.readExecutionReport(2);

            // Last sequence numbers Artio->Client: 2, Client->Artio: 2, client needs to send logon,seqnum=1 after
            onNext.accept(connection, reportFactory);
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

    private void shouldDisconnectConnectionWithNoLogon(final InitialAcceptedSessionOwner initialAcceptedSessionOwner)
        throws IOException
    {
        setup(true, true, true, initialAcceptedSessionOwner);

        final FakeOtfAcceptor fakeOtfAcceptor = new FakeOtfAcceptor();
        final FakeHandler fakeHandler = new FakeHandler(fakeOtfAcceptor);
        try (FixLibrary library = newAcceptingLibrary(fakeHandler, nanoClock))
        {
            try (FixConnection connection = FixConnection.initiate(port))
            {
            }
        }
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

    private void logonThenLogout() throws IOException
    {
        final FixConnection connection = FixConnection.initiate(port);

        logon(connection);

        connection.logoutAndAwaitReply();

        connection.close();
    }
}
