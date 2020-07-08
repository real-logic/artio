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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.artio.Constants;
import uk.co.real_logic.artio.OrdStatus;
import uk.co.real_logic.artio.Reply;
import uk.co.real_logic.artio.Side;
import uk.co.real_logic.artio.Timing;
import uk.co.real_logic.artio.builder.ExecutionReportEncoder;
import uk.co.real_logic.artio.builder.HeaderEncoder;
import uk.co.real_logic.artio.decoder.ExecutionReportDecoder;
import uk.co.real_logic.artio.decoder.ResendRequestDecoder;
import uk.co.real_logic.artio.dictionary.generation.Exceptions;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.fields.RejectReason;
import uk.co.real_logic.artio.library.FixLibrary;
import uk.co.real_logic.artio.session.Session;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static uk.co.real_logic.artio.Constants.EXECUTION_REPORT_MESSAGE_AS_STR;
import static uk.co.real_logic.artio.Reply.State.COMPLETED;
import static uk.co.real_logic.artio.TestFixtures.cleanupMediaDriver;
import static uk.co.real_logic.artio.TestFixtures.launchMediaDriver;
import static uk.co.real_logic.artio.TestFixtures.unusedPort;
import static uk.co.real_logic.artio.Timing.assertEventuallyTrue;
import static uk.co.real_logic.artio.messages.SessionState.ACTIVE;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.ACCEPTOR_ID;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.INITIATOR_ID;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.LIBRARY_LIMIT;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.initiatingLibraryConfig;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.launchInitiatingEngine;

// For reproducing error scenarios when initiating a connection
public class MessageBasedInitiatorSystemTest
{
    private static final int LOGON_SEQ_NUM = 2;

    private final FakeOtfAcceptor otfAcceptor = new FakeOtfAcceptor();
    private final FakeHandler handler = new FakeHandler(otfAcceptor);
    private final int fixPort = unusedPort();
    private final int libraryAeronPort = unusedPort();

    private ArchivingMediaDriver mediaDriver;
    private FixEngine engine;
    private FixLibrary library;
    private TestSystem testSystem;
    private int polled;

    private Reply<Session> sessionReply;

    @Before
    public void setUp()
    {
        mediaDriver = launchMediaDriver();
        engine = launchInitiatingEngine(libraryAeronPort);
        testSystem = new TestSystem();
        library = testSystem.connect(initiatingLibraryConfig(libraryAeronPort, handler));
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
            final ResendRequestDecoder resendRequestDecoder = connection.readMessage(new ResendRequestDecoder());
            assertEquals(1, resendRequestDecoder.beginSeqNo());
            assertEquals(0, resendRequestDecoder.endSeqNo());

            // Intermingle replay of
            sendExecutionReport(connection, 1, true);
            sendExecutionReport(connection, 2, true);
            sendExecutionReport(connection, 3, true);

            connection.msgSeqNum(5).sendTestRequest(testReqID);

            Timing.assertEventuallyTrue("Session has caught up", () ->
            {
                testSystem.poll();

                return !session.awaitingResend() && session.lastReceivedMsgSeqNum() == 5;
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
            final ResendRequestDecoder resendRequestDecoder = connection.readMessage(new ResendRequestDecoder());
            assertEquals(1, resendRequestDecoder.beginSeqNo());
            assertEquals(0, resendRequestDecoder.endSeqNo());

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
            assertThat(session.logoutAndDisconnect(), greaterThan(0L));

            assertConnectionDisconnects(testSystem, connection);

            assertEventuallyTrue("SessionHandler.onDisconnect has not been called", () ->
            {
                testSystem.poll();
                return handler.hasDisconnected();
            });
        }
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
        assertEventuallyTrue(
            "Never sent logon", () ->
            {
                polled += library.poll(LIBRARY_LIMIT);
                return polled > 2;
            });

        connection.readLogonReply();
    }

    private FixConnection acceptConnection() throws IOException
    {
        return FixConnection.accept(fixPort, () ->
            sessionReply = SystemTestUtil.initiate(library, fixPort, INITIATOR_ID, ACCEPTOR_ID));
    }

    @After
    public void tearDown()
    {
        Exceptions.closeAll(library, engine);
        cleanupMediaDriver(mediaDriver);
    }
}
