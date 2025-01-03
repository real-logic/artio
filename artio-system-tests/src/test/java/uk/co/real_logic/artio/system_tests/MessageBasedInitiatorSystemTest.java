/*
 * Copyright 2015-2025 Real Logic Limited, Adaptive Financial Consulting Ltd.
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

import io.aeron.Aeron;
import io.aeron.archive.ArchivingMediaDriver;
import org.agrona.ErrorHandler;
import org.agrona.collections.IntHashSet;
import org.agrona.concurrent.EpochNanoClock;
import org.agrona.concurrent.OffsetEpochNanoClock;
import org.agrona.concurrent.status.CountersReader;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;
import uk.co.real_logic.artio.*;
import uk.co.real_logic.artio.builder.ExecutionReportEncoder;
import uk.co.real_logic.artio.builder.HeaderEncoder;
import uk.co.real_logic.artio.decoder.ExecutionReportDecoder;
import uk.co.real_logic.artio.decoder.NewOrderSingleDecoder;
import uk.co.real_logic.artio.dictionary.generation.Exceptions;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.fields.RejectReason;
import uk.co.real_logic.artio.library.FixLibrary;
import uk.co.real_logic.artio.library.SessionConfiguration;
import uk.co.real_logic.artio.messages.InitialAcceptedSessionOwner;
import uk.co.real_logic.artio.messages.SessionState;
import uk.co.real_logic.artio.session.Session;
import uk.co.real_logic.artio.session.SessionWriter;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static uk.co.real_logic.artio.Constants.*;
import static uk.co.real_logic.artio.FixCounters.FixCountersId.RECV_MSG_SEQ_NO_TYPE_ID;
import static uk.co.real_logic.artio.FixCounters.lookupCounterIds;
import static uk.co.real_logic.artio.Reply.State.COMPLETED;
import static uk.co.real_logic.artio.Reply.State.ERRORED;
import static uk.co.real_logic.artio.TestFixtures.*;
import static uk.co.real_logic.artio.Timing.assertEventuallyTrue;
import static uk.co.real_logic.artio.messages.SessionState.ACTIVE;
import static uk.co.real_logic.artio.system_tests.AbstractGatewayToGatewaySystemTest.TEST_TIMEOUT_IN_MS;
import static uk.co.real_logic.artio.system_tests.AbstractGatewayToGatewaySystemTest.createFollowerSession;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.PASSWORD;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.USERNAME;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.*;

// For reproducing error scenarios when initiating a connection
public class MessageBasedInitiatorSystemTest
{
    @Rule
    public Timeout timeout = Timeout.seconds(20);

    private static final int LOGON_SEQ_NUM = 2;

    private final ErrorHandler errorHandler = mock(ErrorHandler.class);
    private final EpochNanoClock nanoClock = new OffsetEpochNanoClock();
    private final FakeOtfAcceptor otfAcceptor = new FakeOtfAcceptor();
    private final FakeHandler handler = new FakeHandler(otfAcceptor);
    private final int fixPort = unusedPort();
    private final int libraryAeronPort = unusedPort();

    private ArchivingMediaDriver mediaDriver;
    private FixEngine engine;
    private FixLibrary library;
    private TestSystem testSystem;

    private Reply<Session> sessionReply;

    @Before
    public void setUp()
    {
        mediaDriver = launchMediaDriver();
        final EngineConfiguration initiatingConfig = initiatingConfig(libraryAeronPort, nanoClock);
        initiatingConfig.deleteLogFileDirOnStart(true);
        initiatingConfig.initialAcceptedSessionOwner(InitialAcceptedSessionOwner.SOLE_LIBRARY);
        initiatingConfig.errorHandlerFactory(errorBuffer -> errorHandler);
        engine = FixEngine.launch(initiatingConfig);
        testSystem = new TestSystem();
        library = testSystem.connect(initiatingLibraryConfig(libraryAeronPort, handler, nanoClock));
    }

    @Test
    public void shouldRequestResendForWrongSequenceNumber() throws IOException
    {
        try (FixConnection connection = acceptConnection())
        {
            sendLogonToAcceptor(connection);

            connection.msgSeqNum(LOGON_SEQ_NUM).logon(false);

            final Session session = lookupSession();
            assertTrue(session.awaitingResend());
        }
    }

    @Test
    public void shouldCompleteInitiateWhenResetSeqNumFlagSet() throws IOException
    {
        try (FixConnection connection = acceptConnection())
        {
            sendLogonToAcceptor(connection);

            connection.logon(true);

            final Session session = lookupSession();
            assertEquals(1, session.lastReceivedMsgSeqNum());
        }
    }

    private Session lookupSession()
    {
        final Reply<Session> reply = testSystem.awaitReply(this.sessionReply);
        assertEquals(COMPLETED, reply.state());

        final Session session = reply.resultIfPresent();
        assertEquals(ACTIVE, session.state());

        return session;
    }

    @Test
    public void shouldValidateMissingEnumValue() throws IOException
    {
        try (FixConnection connection = acceptConnection())
        {
            sendLogonToAcceptor(connection);
            connection.logon(false);

            final Session session = lookupSession();
            assertEquals(1, session.lastReceivedMsgSeqNum());

            handler.copyMessages(true);
            sendInvalidExecutionReport(connection);

            testSystem.awaitMessageOf(otfAcceptor, EXECUTION_REPORT_MESSAGE_AS_STR);
            assertEquals(2, session.lastReceivedMsgSeqNum());

            final MutableAsciiBuffer lastMessage = handler.lastMessage();
            final int length = handler.lastMessageLength();

            final ExecutionReportDecoder executionReportDecoder = new ExecutionReportDecoder();
            executionReportDecoder.decode(lastMessage, 0, length);

            assertFalse(executionReportDecoder.validate());
            assertEquals(Constants.EXEC_TYPE, executionReportDecoder.invalidTagId());
            assertEquals(RejectReason.VALUE_IS_INCORRECT, RejectReason.decode(executionReportDecoder.rejectReason()));
        }
    }

    private void sendInvalidExecutionReport(final FixConnection connection)
    {
        final ExecutionReportEncoder executionReportEncoder = new ExecutionReportEncoder();
        final HeaderEncoder header = executionReportEncoder.header();

        connection.setupHeader(header, 2, false);

        executionReportEncoder
            .orderID("order")
            .execID("exec")
            .execType('5') // Invalid exec type
            .ordStatus(OrdStatus.FILLED)
            .side(Side.BUY);

        executionReportEncoder.instrument().symbol("IBM");

        connection.send(executionReportEncoder);
    }

    @Test
    public void shouldCatchupReplaySequences() throws IOException
    {
        final String testReqID = "thisIsATest";

        try (FixConnection connection = acceptConnection())
        {
            sendLogonToAcceptor(connection);

            connection.msgSeqNum(4).logon(false);

            final Session session = lookupSession();
            assertTrue(session.awaitingResend());

            // Receive resend request for missing messages.
            connection.readResendRequest(1, 0);

            // Intermingle replay of
            sendExecutionReport(connection, 1, true);
            sendExecutionReport(connection, 2, true);
            sendExecutionReport(connection, 3, true);
            connection.sendGapFill(4, 5);

            connection.msgSeqNum(5).sendTestRequest(testReqID);

            Timing.assertEventuallyTrue("Session has not caught up", () ->
            {
                testSystem.poll();

                return !session.awaitingResend() && session.lastReceivedMsgSeqNum() >= 5;
            });

            connection.readHeartbeat(testReqID);
        }
    }

    @Test
    public void shouldAcceptExtendedFillGap() throws IOException
    {
        final String testReqID = "thisIsATest";

        try (FixConnection connection = acceptConnection())
        {
            sendLogonToAcceptor(connection);

            connection.msgSeqNum(4).logon(false);

            final Session session = lookupSession();
            assertTrue(session.awaitingResend());

            // Receive resend request for missing messages.
            connection.readResendRequest(1, 0);

            // Fill the gap
            connection.sendGapFill(1, 6);
            connection.msgSeqNum(6).sendTestRequest(testReqID);

            Timing.assertEventuallyTrue("Session has caught up", () ->
            {
                testSystem.poll();

                return !session.awaitingResend() && session.lastReceivedMsgSeqNum() == 6;
            });

            connection.readHeartbeat(testReqID);
        }
    }

    @Test
    public void shouldBeNotifiedOnDisconnect() throws IOException
    {
        try (FixConnection connection = acceptConnection())
        {
            sendLogonToAcceptor(connection);

            assertFalse(handler.hasDisconnected());

            final Session session = handler.lastSession();
            testSystem.awaitSend(session::logoutAndDisconnect);

            assertConnectionDisconnects(testSystem, connection);

            assertEventuallyTrue("SessionHandler.onDisconnect has not been called", () ->
            {
                testSystem.poll();
                return handler.hasDisconnected();
            });
        }
    }

    @Test
    public void shouldNotErrorWhenNoLogonClosedInSoleLibraryReconnectScenario() throws IOException
    {
        shouldBeNotifiedOnDisconnect();

        try (FixConnection connection = acceptConnection())
        {
            connection.close();

            testSystem.awaitReply(sessionReply);
            assertEquals(sessionReply.toString(), sessionReply.state(), ERRORED);
        }

        verifyNoInteractions(errorHandler);
    }

    @Test
    public void shouldNotErrorWhenInSoleLibraryReconnectScenario() throws IOException
    {
        shouldBeNotifiedOnDisconnect();

        try (FixConnection connection = acceptConnection())
        {
            testSystem.awaitBlocking(() ->
            {
                connection.readLogon();
                connection.logon(false);
            });

            testSystem.awaitCompletedReply(sessionReply);
        }
        assertSessionDisconnected(testSystem, sessionReply.resultIfPresent());

        verifyNoInteractions(errorHandler);
    }

    @Test
    public void shouldProcessResendRequestMarkingInvalidMessagesAsSo() throws IOException
    {
        sendTwoOrdersReceiveOneReportAndDisconnect(false, false);

        // The gateway thinks it's sent the second execution report and wants to send a third.
        try (FixConnection connection = acceptPersistentConnection(false, false))
        {
            testSystem.awaitBlocking(() ->
            {
                connection.readLogon(4);
                connection.msgSeqNum(4).logon(false);
                connection.sendExecutionReport(5, false);
                connection.readResendRequest(3, 0);
                connection.sendExecutionReport(3, true);
                connection.sendGapFill(4, 5);
                connection.sendExecutionReport(5, true);
                connection.sendExecutionReport(6, false);
            });

            // Assert that we've received the resent messages in order - without the first execution report
            // which should be ignored
            assertResendMessagesInOrder();
        }
    }

    @Test
    public void shouldProcessResendRequestMarkingInvalidMessagesAsSoWithClosedResendInterval() throws IOException
    {
        sendTwoOrdersReceiveOneReportAndDisconnect(true, false);

        // The gateway thinks it's sent the second execution report and wants to send a third.
        try (FixConnection connection = acceptPersistentConnection(true, false))
        {
            testSystem.awaitBlocking(() ->
            {
                connection.readLogon(4);
                connection.msgSeqNum(4).logon(false);
                connection.sendExecutionReport(5, false);
                connection.readResendRequest(3, 4);
                connection.sendExecutionReport(3, true);
                connection.sendGapFill(4, 5);
                connection.readResendRequest(5, 5);
                connection.sendExecutionReport(5, true);
                connection.sendExecutionReport(6, false);
            });
            assertResendMessagesInOrder();
        }
    }

    @Test
    public void shouldProcessResendRequestMarkingInvalidMessagesAsSoWithRepeatResendRequests() throws IOException
    {
        sendTwoOrdersReceiveOneReportAndDisconnect(false, true);

        // The gateway thinks it's sent the second execution report and wants to send a third.
        try (FixConnection connection = acceptPersistentConnection(false, true))
        {
            testSystem.awaitBlocking(() ->
            {
                connection.readLogon(4);
                connection.msgSeqNum(4).logon(false);
                connection.readResendRequest(3, 0);
                connection.sendExecutionReport(5, false);
                connection.readResendRequest(5, 0);
                connection.sendExecutionReport(3, true);
                connection.sendGapFill(4, 5);
                connection.sendExecutionReport(5, true);
                connection.sendExecutionReport(6, false);
            });
            assertResendMessagesInOrder();
        }
    }

    @Test
    public void shouldProcessResendRequestMarkingInvalidMessagesAsSoWithRepeatResendRequestsAndClosedResendInterval()
        throws IOException
    {
        sendTwoOrdersReceiveOneReportAndDisconnect(true, true);

        // The gateway thinks it's sent the second execution report and wants to send a third.
        try (FixConnection connection = acceptPersistentConnection(true, true))
        {
            testSystem.awaitBlocking(() ->
            {
                connection.readLogon(4);
                connection.msgSeqNum(4).logon(false);
                connection.readResendRequest(3, 4);
                connection.sendExecutionReport(5, false);
                connection.readResendRequest(5, 5);
                connection.sendExecutionReport(3, true);
                connection.sendGapFill(4, 5);
                connection.sendExecutionReport(5, true);
                connection.sendExecutionReport(6, false);
            });
            assertResendMessagesInOrder();
        }
    }

    @Test
    public void shouldIgnoreMessagesAfterALowSequenceNumberLogout() throws IOException
    {
        final Session session;

        // Boost the received sequence number to 4
        try (FixConnection connection = acceptPersistentConnection(false, false))
        {
            sendLogonToAcceptor(connection);
            connection.logon(false);

            session = completeLogon();
            OrderFactory.sendOrder(session);

            testSystem.awaitBlocking(() ->
            {
                connection.readOrder();
                connection.sendExecutionReport(2, false);
                connection.sendExecutionReport(3, false);

                connection.msgSeqNum(4).logoutAndAwaitReply();
            });
            assertEquals(4, session.lastReceivedMsgSeqNum());
            assertEquals(3, session.lastSentMsgSeqNum());
        }

        testSystem.await("Failed to disconnect", () -> session.state() == SessionState.DISCONNECTED);

        // Low sequence number logon followed by a resend request
        try (FixConnection connection = acceptPersistentConnection(false, false))
        {
            testSystem.awaitBlocking(() ->
            {
                connection.readLogon();
                connection.logon(false);
                connection.sendResendRequest(2, 2);
                assertEquals(
                    "MsgSeqNum too low, expecting 5 but received 1",
                    connection.readLogout().textAsString());
                assertFalse(connection.isConnected());
            });

            // logon=4,logout=5, no more messages sent after logout
            assertEquals(5, session.lastSentMsgSeqNum());
        }
    }

    @Test
    public void shouldInitiateConnectionAfterRequestSession() throws IOException
    {
        final SessionWriter sessionWriter = createFollowerSession(
            TEST_TIMEOUT_IN_MS, testSystem, library, ACCEPTOR_ID, INITIATOR_ID);
        final long sessionId = sessionWriter.id();

        final Session session = acquireSession(handler, library, sessionId, testSystem);
        assertNotNull(session);

        try (FixConnection connection = acceptConnection())
        {
            sendLogonToAcceptor(connection);

            connection.logon(false);

            final Session session1 = lookupSession();
            assertSame(session, session1);
            assertEquals(sessionId, session1.id());
        }
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldNotLeakCountersIfDisconnectedBeforeLogonCompleteInSoleLibraryMode() throws IOException
    {
        // Replicates a client issue, note that we only close the counters when we're
        initiatorFailsToConnect();

        initiatorFailsToConnect();

        initiatorFailsToConnect();

        try (Aeron aeron = Aeron.connect(engine.configuration().aeronContextClone()))
        {
            final CountersReader countersReader = aeron.countersReader();
            final IntHashSet ids = lookupCounterIds(RECV_MSG_SEQ_NO_TYPE_ID, countersReader, label -> true);
            assertThat(ids.toString(), ids, hasSize(1));
        }
    }

    private void initiatorFailsToConnect() throws IOException
    {
        try (FixConnection connection = acceptConnection())
        {
            sendLogonToAcceptor(connection);
        }

        final Reply<Session> reply = testSystem.awaitReply(this.sessionReply);
        assertEquals(ERRORED, reply.state());
        assertThat(reply.error().toString(), containsString("UNABLE_TO_LOGON"));
    }

    private Session completeLogon()
    {
        return testSystem.awaitCompletedReply(sessionReply).resultIfPresent();
    }

    private void assertResendMessagesInOrder()
    {
        // Assert that we've received the resent messages in order - without the first execution report
        // which should be ignored
        final List<FixMessage> messages = testSystem.awaitMessageCount(otfAcceptor, 6);

        final FixMessage logon = messages.get(0);
        assertEquals(logon.toString(), 4, logon.messageSequenceNumber());
        assertEquals(logon.toString(), LOGON_MESSAGE_AS_STR, logon.msgType());
        assertFalse(logon.toString(), logon.isValid());

        final FixMessage invalidReport5 = messages.get(1);
        assertEquals(invalidReport5.toString(), 5, invalidReport5.messageSequenceNumber());
        assertEquals(invalidReport5.toString(), EXECUTION_REPORT_MESSAGE_AS_STR, invalidReport5.msgType());
        assertNull(invalidReport5.toString(), invalidReport5.possDup());
        assertFalse(invalidReport5.toString(), invalidReport5.isValid());

        final FixMessage report3 = messages.get(2);
        assertEquals(report3.toString(), 3, report3.messageSequenceNumber());
        assertEquals(report3.toString(), EXECUTION_REPORT_MESSAGE_AS_STR, report3.msgType());
        assertEquals(report3.toString(), "Y", report3.possDup());

        final FixMessage gapFill = messages.get(3);
        assertEquals(gapFill.toString(), 4, gapFill.messageSequenceNumber());
        assertEquals(gapFill.toString(), SEQUENCE_RESET_MESSAGE_AS_STR, gapFill.msgType());
        assertEquals(gapFill.toString(), "Y", gapFill.gapFill());
        assertEquals(gapFill.toString(), "Y", gapFill.possDup());

        final FixMessage report5 = messages.get(4);
        assertEquals(report5.toString(), 5, report5.messageSequenceNumber());
        assertEquals(report5.toString(), EXECUTION_REPORT_MESSAGE_AS_STR, report5.msgType());
        assertEquals(report5.toString(), "Y", report5.possDup());

        final FixMessage report6 = messages.get(5);
        assertEquals(report6.toString(), 6, report6.messageSequenceNumber());
        assertEquals(report6.toString(), EXECUTION_REPORT_MESSAGE_AS_STR, report6.msgType());
        assertNull(report6.toString(), report6.possDup());
    }

    private void sendTwoOrdersReceiveOneReportAndDisconnect(
        final boolean closedResendInterval, final boolean sendRedundantResendRequests) throws IOException
    {
        final Session session;
        try (FixConnection connection = acceptPersistentConnection(closedResendInterval, sendRedundantResendRequests))
        {
            testSystem.awaitBlocking(() ->
            {
                connection.readLogon(1);
                connection.logon(false);
            });
            testSystem.awaitCompletedReply(sessionReply);
            session = sessionReply.resultIfPresent();
            OrderFactory.sendOrder(session);
            connection.readOrder();

            OrderFactory.sendOrder(session);
            final NewOrderSingleDecoder receivedOrder = connection.readOrder();
            assertEquals(3, receivedOrder.header().msgSeqNum());

            connection.sendExecutionReport(2, false);
            testSystem.awaitMessageOf(otfAcceptor, EXECUTION_REPORT_MESSAGE_AS_STR);
            otfAcceptor.messages().clear();
        }
        SystemTestUtil.assertSessionDisconnected(testSystem, session);
    }

    public static void assertConnectionDisconnects(final TestSystem testSystem, final FixConnection connection)
    {
        assertEventuallyTrue("Socket is not disconnected", () ->
        {
            testSystem.poll();
            return !connection.isConnected();
        });
    }

    void sendExecutionReport(final FixConnection connection, final int msgSeqNum, final boolean possDupFlag)
    {
        connection.sendExecutionReport(msgSeqNum, possDupFlag);

        testSystem.poll();
    }

    private void sendLogonToAcceptor(final FixConnection connection)
    {
        //noinspection Convert2MethodRef
        testSystem.awaitBlocking(() -> connection.readLogon());
    }

    @After
    public void tearDown()
    {
        Exceptions.closeAll(library, engine);
        cleanupMediaDriver(mediaDriver);
    }

    private FixConnection acceptConnection() throws IOException
    {
        return FixConnection.accept(fixPort, () ->
            sessionReply = SystemTestUtil.initiate(library, fixPort, INITIATOR_ID, ACCEPTOR_ID));
    }

    private FixConnection acceptPersistentConnection(
        final boolean closedResendInterval, final boolean sendRedundantResendRequests) throws IOException
    {
        final SessionConfiguration config = SessionConfiguration.builder()
            .address("localhost", fixPort)
            .credentials(USERNAME, PASSWORD)
            .senderCompId(INITIATOR_ID)
            .targetCompId(ACCEPTOR_ID)
            .timeoutInMs(TEST_REPLY_TIMEOUT_IN_MS)
            .sequenceNumbersPersistent(true)
            .closedResendInterval(closedResendInterval)
            .sendRedundantResendRequests(sendRedundantResendRequests)
            .build();

        return FixConnection.accept(fixPort, () -> sessionReply = library.initiate(config));
    }
}
