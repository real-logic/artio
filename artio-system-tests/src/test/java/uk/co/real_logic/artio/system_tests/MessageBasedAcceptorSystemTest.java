/*
 * Copyright 2015-2023 Real Logic Limited, Adaptive Financial Consulting Ltd.
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

import org.agrona.concurrent.status.ReadablePosition;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.mockito.ArgumentMatchers;
import uk.co.real_logic.artio.Constants;
import uk.co.real_logic.artio.Reply;
import uk.co.real_logic.artio.Side;
import uk.co.real_logic.artio.Timing;
import uk.co.real_logic.artio.builder.*;
import uk.co.real_logic.artio.decoder.*;
import uk.co.real_logic.artio.engine.SessionInfo;
import uk.co.real_logic.artio.library.FixLibrary;
import uk.co.real_logic.artio.messages.DisconnectReason;
import uk.co.real_logic.artio.messages.InitialAcceptedSessionOwner;
import uk.co.real_logic.artio.messages.SessionReplyStatus;
import uk.co.real_logic.artio.messages.ThrottleConfigurationStatus;
import uk.co.real_logic.artio.session.Session;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.agrona.CloseHelper.close;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.verify;
import static uk.co.real_logic.artio.Constants.RESEND_REQUEST_MESSAGE_AS_STR;
import static uk.co.real_logic.artio.SessionRejectReason.COMPID_PROBLEM;
import static uk.co.real_logic.artio.dictionary.SessionConstants.*;
import static uk.co.real_logic.artio.messages.InitialAcceptedSessionOwner.ENGINE;
import static uk.co.real_logic.artio.messages.InitialAcceptedSessionOwner.SOLE_LIBRARY;
import static uk.co.real_logic.artio.messages.ThrottleConfigurationStatus.OK;
import static uk.co.real_logic.artio.system_tests.AbstractGatewayToGatewaySystemTest.LONG_TEST_TIMEOUT_IN_MS;
import static uk.co.real_logic.artio.system_tests.AbstractGatewayToGatewaySystemTest.TEST_TIMEOUT_IN_MS;
import static uk.co.real_logic.artio.system_tests.FixConnection.BUFFER_SIZE;
import static uk.co.real_logic.artio.system_tests.MessageBasedInitiatorSystemTest.assertConnectionDisconnects;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.*;

public class MessageBasedAcceptorSystemTest extends AbstractMessageBasedAcceptorSystemTest
{
    private final int timeoutDisconnectHeartBtIntInS = 1;
    private final long timeoutDisconnectHeartBtIntInMs = TimeUnit.SECONDS.toMillis(timeoutDisconnectHeartBtIntInS);

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

            final RejectDecoder reject = connection.readReject();
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

            final LogonDecoder logon = connection.readLogon();
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

            connection.readLogon();

            connection.logout();
        }

        try (FixConnection connection = FixConnection.initiate(port))
        {
            // The previous disconnection hasn't yet been detected as it's still active.
            assertTrue(session.isActive());

            connection.logon(true);

            // During this loop the logout message for the disconnected connection is sent,
            // But not received by the new connection.
            // Send a test request here to increase likelihood of triggering the race
            SystemTestUtil.sendTestRequest(testSystem, session, "badTestRequest");
            Timing.assertEventuallyTrue("Library has disconnected old session", () ->
            {
                testSystem.poll();

                return !handler.sessions().contains(session);
            });

            connection.readLogon();
            // check that it really is a logon and not the logout
            assertThat(
                connection.lastTotalBytesRead(),
                containsString("\00135=A\001"));

            connection.logoutAndAwaitReply();
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

            sleep(200);

            connection.logout();
            assertFalse("Read a resent FIX message instead of a disconnect", connection.isConnected());
        }
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldRejectInvalidResendRequestsHighBeginSeqNo() throws IOException
    {
        shouldRejectInvalidResendRequests((connection, reportSeqNum) ->
        {
            final int invalidSeqNum = reportSeqNum + 1;
            return connection.sendResendRequest(invalidSeqNum, invalidSeqNum);
        });
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldRejectInvalidResendRequestsEndSeqNoBelowBeginSeqNo() throws IOException
    {
        shouldRejectInvalidResendRequests((connection, reportSeqNum) ->
        {
            return connection.sendResendRequest(reportSeqNum, reportSeqNum - 1);
        });
    }

    private void shouldRejectInvalidResendRequests(
        final BiFunction<FixConnection, Integer, ResendRequestEncoder> resendRequester)
        throws IOException
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
                final ResendRequestEncoder resendRequest = resendRequester.apply(connection, reportSeqNum);

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
            final long reportIndex = ReportFactory.sendOneReport(testSystem, session, Side.SELL);

            final ReadablePosition libraryPosition = testSystem.awaitReply(
                engine.libraryIndexedPosition(library.libraryId())).resultIfPresent();
            testSystem.awaitPosition(libraryPosition, reportIndex);

            testSystem.awaitBlocking(() ->
            {
                final int reportSeqNum = connection.readExecutionReport().header().msgSeqNum();

                // Send an invalid resend request
                final int invalidSeqNum = reportSeqNum + 100;
                connection.sendResendRequest(headerSeqNum, invalidSeqNum);

                final SequenceResetDecoder sequenceResetDecoder = connection.readMessage(new SequenceResetDecoder());
                assertTrue(sequenceResetDecoder.header().possDupFlag());
                assertEquals(connection.lastMessageAsString(), reportSeqNum, sequenceResetDecoder.newSeqNo());
                final ExecutionReportDecoder secondExecutionReport = connection.readExecutionReport();
                assertTrue(secondExecutionReport.header().possDupFlag());
                assertEquals(reportSeqNum, secondExecutionReport.header().msgSeqNum());

                sleep(200);

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
    public void shouldRejectMessagesOverThrottle() throws IOException
    {
        setup(true, true, true, ENGINE, true);
        setupLibrary();

        try (FixConnection connection = FixConnection.initiate(port))
        {
            logon(connection);
            testSystem.poll();

            assertMessagesRejectedAboveThrottleRate(connection, THROTTLE_MSG_LIMIT, 2, 4, 1);

            testSystem.awaitBlocking(MessageBasedAcceptorSystemTest::sleepThrottleWindow);

            final HeartbeatDecoder abc = connection.exchangeTestRequestHeartbeat("ABC");
            assertEquals(10, abc.header().msgSeqNum());

            // Test that resend requests work with throttle rejection
            connection.sendResendRequest(4, 5);
            assertReadsBusinessReject(connection, 4, 6, true, THROTTLE_MSG_LIMIT);
            assertReadsBusinessReject(connection, 5, 7, true, THROTTLE_MSG_LIMIT);
            final HeartbeatDecoder def = connection.exchangeTestRequestHeartbeat("DEF");
            assertEquals(11, def.header().msgSeqNum());
            testSystem.poll();

            // Reset the throttle rate
            session = acquireSession();
            final Reply<ThrottleConfigurationStatus> reply = testSystem.awaitCompletedReply(session.throttleMessagesAt(
                TEST_THROTTLE_WINDOW_IN_MS, RESET_THROTTLE_MSG_LIMIT));
            assertEquals(reply.toString(), OK, reply.resultIfPresent());

            testSystem.awaitBlocking(() ->
            {
                sleepThrottleWindow();

                assertMessagesRejectedAboveThrottleRate(connection, RESET_THROTTLE_MSG_LIMIT, 12, 20, 0);

                sleepThrottleWindow();

                connection.logoutAndAwaitReply();
            });
        }
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldAnswerResendRequestWithHighSeqNum() throws IOException
    {
        setup(true, true);
        setupLibrary();

        try (FixConnection connection = FixConnection.initiate(port))
        {
            logon(connection);
            testSystem.poll();

            final HeartbeatDecoder abc = connection.exchangeTestRequestHeartbeat("ABC");
            assertEquals(2, abc.header().msgSeqNum());

            // shift msg seq num
            connection.msgSeqNum(connection.msgSeqNum() + 3);
            connection.sendResendRequest(1, 2);
            // answers with resend
            connection.readResendRequest(3, 0);
            // because original resend request is not resent, but rather gap filled
            connection.sendGapFill(3, connection.msgSeqNum() + 1);
            // answers the resend as well
            final SequenceResetDecoder sequenceResetDecoder = connection.readMessage(new SequenceResetDecoder());
            assertEquals(sequenceResetDecoder.header().msgSeqNum(), 1);
            assertEquals(sequenceResetDecoder.newSeqNo(), 3);
        }
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

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldGracefullyHandleExceptionsInOnSessionStart() throws IOException
    {
        setup(true, true, true, SOLE_LIBRARY);

        setupLibrary();

        handler.shouldThrowInOnSessionStart(true);

        try (FixConnection connection = FixConnection.initiate(port))
        {
            testSystem.awaitBlocking(() ->
            {
                connection.logon(false);

                final LogonDecoder logonReply = connection.readLogon();
                assertEquals(1, logonReply.header().msgSeqNum());

                connection.readLogout();
            });
        }

        assertTrue(handler.onSessionStartCalled());
        verify(errorHandler).onError(ArgumentMatchers.any(RuntimeException.class));
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldSendLogoutOnTimeoutDisconnect() throws IOException
    {
        reasonableTransmissionTimeInMs = 1;

        setup(true, true, true, ENGINE);

        try (FixConnection connection = FixConnection.initiate(port))
        {
            connection.logon(true, timeoutDisconnectHeartBtIntInS);
            final LogonDecoder logon = connection.readLogon();
            assertTrue(logon.resetSeqNumFlag());

            processTimeoutDisconnect(connection);
        }
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldSendLogoutOnTimeoutDisconnectSoleLibrary() throws IOException
    {
        reasonableTransmissionTimeInMs = 1;

        setup(true, true, true, SOLE_LIBRARY);

        setupLibrary();
        testSystem.awaitTimeoutInMs(LONG_TEST_TIMEOUT_IN_MS);

        final FakeDisconnectHandler onDisconnect = new FakeDisconnectHandler();
        handler.onDisconnectCallback(onDisconnect);

        try (FixConnection connection = FixConnection.initiate(port))
        {
            connection.logon(true, timeoutDisconnectHeartBtIntInS);
            //noinspection Convert2MethodRef
            final LogonDecoder logon = testSystem.awaitBlocking(() -> connection.readLogon());
            assertTrue(logon.resetSeqNumFlag());

            session = handler.lastSession();
            assertNotNull(session);

            final long logoutTimeInMs = testSystem.awaitBlocking(() -> processTimeoutDisconnect(connection));
            SystemTestUtil.assertSessionDisconnected(testSystem, session);

            assertPromptDisconnect(logoutTimeInMs, onDisconnect.timeInMs());
            assertEquals(DisconnectReason.FIX_HEARTBEAT_TIMEOUT, onDisconnect.reason());
        }
    }

    private long processTimeoutDisconnect(final FixConnection connection)
    {
        connection.readHeartbeat();
        final TestRequestDecoder testRequest = connection.readTestRequest();
        assertEquals("TEST", testRequest.testReqIDAsString());

        connection.readHeartbeat();
        connection.readLogout();
        final long logoutTimeInMs = System.currentTimeMillis();

        assertFalse(connection.isConnected());
        final long disconnectTimeInMs = System.currentTimeMillis();

        assertPromptDisconnect(logoutTimeInMs, disconnectTimeInMs);

        return logoutTimeInMs;
    }

    private void assertPromptDisconnect(final long logoutTimeInMs, final long disconnectTimeInMs)
    {
        assertThat("Disconnect not prompt enough", (disconnectTimeInMs - logoutTimeInMs),
            Matchers.lessThan(timeoutDisconnectHeartBtIntInMs));
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldNotAllowRequestingOfUnknownSession() throws IOException
    {
        setup(true, true);

        setupLibrary();

        // Create internal session object with a session id of -1
        try (FixConnection connection = FixConnection.initiate(port))
        {
            final SessionReplyStatus sessionReplyStatus = requestSession(library, Session.UNKNOWN, testSystem);
            assertEquals(SessionReplyStatus.UNKNOWN_SESSION, sessionReplyStatus);
        }
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldSupportResendRequestsAfterSequenceReset() throws IOException
    {
        setup(true, true);

        setupLibrary();

        final ReadablePosition libraryPosition = testSystem.libraryPosition(engine, library);

        try (FixConnection connection = FixConnection.initiate(port))
        {
            // Given setup session with 1 sent execution report
            logon(connection);

            session = acquireSession();
            ReportFactory.sendOneReport(testSystem, session, Side.SELL);

            final ExecutionReportDecoder executionReport = connection.readExecutionReport();
            assertSell(executionReport);
            final int msgSeqNum = executionReport.header().msgSeqNum();

            // When you perform the sequence reset
            // NB: this is a high sequence number, so a normal FIX session would reject this message but it can be
            // useful to send it on an offline session
            final int highSeqNum = 100;
            assertThat(highSeqNum, greaterThan(msgSeqNum));
            final long position = testSystem.awaitSend(() -> session.trySendSequenceReset(highSeqNum, highSeqNum));

            testSystem.awaitBlocking(() ->
            {
                final SequenceResetDecoder sequenceReset = connection.readSequenceReset();
                assertFalse(sequenceReset.toString(), sequenceReset.hasGapFillFlag());
                assertEquals(highSeqNum, sequenceReset.newSeqNo());
            });

            testSystem.awaitPosition(libraryPosition, position);

            testSystem.awaitBlocking(() ->
            {
                // Then you get the resend
                connection.msgSeqNum(highSeqNum).sendResendRequest(1, 0);
                connection.readSequenceResetGapFill(msgSeqNum);
                final ExecutionReportDecoder executionReportResent = connection.readExecutionReport(msgSeqNum);
                assertSell(executionReportResent);
                connection.readSequenceResetGapFill(highSeqNum);
            });
        }
    }

    private void assertSell(final ExecutionReportDecoder executionReport)
    {
        assertEquals(executionReport.toString(), Side.SELL, executionReport.sideAsEnum());
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

    public static void sleepThrottleWindow()
    {
        sleep(TEST_THROTTLE_WINDOW_IN_MS);
    }

    private void assertMessagesRejectedAboveThrottleRate(
        final FixConnection connection,
        final int limitOfMessages,
        final int seqNumOffset,
        final int refSeqNumOffset,
        final int messagesWithinThrottleWindow)
    {
        final int reportCount = 10;
        for (int i = 0; i < reportCount; i++)
        {
            connection.sendExecutionReport(connection.acquireMsgSeqNum(), false);

            sleep(1);
        }

        // messagesWithinThrottleWindow - could be login message at the start
        for (int i = 0; i < (reportCount - limitOfMessages) + messagesWithinThrottleWindow; i++)
        {
            assertReadsBusinessReject(
                connection, i + seqNumOffset, i + refSeqNumOffset, false, limitOfMessages);
        }
    }

    private void assertReadsBusinessReject(
        final FixConnection connection,
        final int seqNum,
        final int refSeqNum,
        final boolean possDup,
        final int limitOfMessages)
    {
        final BusinessMessageRejectDecoder reject = connection.readBusinessReject();
        final HeaderDecoder header = reject.header();
        assertEquals(seqNum, header.msgSeqNum());
        assertEquals(ACCEPTOR_ID, header.senderCompIDAsString());
        assertEquals(INITIATOR_ID, header.targetCompIDAsString());
        assertEquals(possDup, header.hasPossDupFlag() && header.possDupFlag());

        assertEquals("wrong businessRejectReason", 99, reject.businessRejectReason());
        assertEquals("8", reject.refMsgTypeAsString());
        assertEquals("wrong refSeqNum", refSeqNum, reject.refSeqNum());
        assertEquals("Throttle limit exceeded (" + limitOfMessages + " in 300ms)",
            reject.textAsString());
    }

    private static void sleep(final int timeInMs)
    {
        try
        {
            Thread.sleep(timeInMs);
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
