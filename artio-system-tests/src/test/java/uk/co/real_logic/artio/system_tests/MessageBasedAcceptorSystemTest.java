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

import org.hamcrest.MatcherAssert;
import org.junit.Test;
import uk.co.real_logic.artio.Constants;
import uk.co.real_logic.artio.Timing;
import uk.co.real_logic.artio.builder.*;
import uk.co.real_logic.artio.decoder.LogonDecoder;
import uk.co.real_logic.artio.decoder.LogoutDecoder;
import uk.co.real_logic.artio.decoder.RejectDecoder;
import uk.co.real_logic.artio.library.FixLibrary;
import uk.co.real_logic.artio.library.LibraryConfiguration;
import uk.co.real_logic.artio.messages.InitialAcceptedSessionOwner;
import uk.co.real_logic.artio.session.Session;
import uk.co.real_logic.artio.validation.MessageValidationStrategy;

import java.io.IOException;
import java.util.function.Consumer;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static org.agrona.CloseHelper.close;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;
import static uk.co.real_logic.artio.SessionRejectReason.COMPID_PROBLEM;
import static uk.co.real_logic.artio.dictionary.SessionConstants.*;
import static uk.co.real_logic.artio.messages.InitialAcceptedSessionOwner.ENGINE;
import static uk.co.real_logic.artio.messages.InitialAcceptedSessionOwner.SOLE_LIBRARY;
import static uk.co.real_logic.artio.system_tests.FixConnection.*;
import static uk.co.real_logic.artio.system_tests.MessageBasedInitiatorSystemTest.assertConnectionDisconnects;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.*;

public class MessageBasedAcceptorSystemTest extends AbstractMessageBasedAcceptorSystemTest
{
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
            assertEquals(Constants.SENDING_TIME, reject.refTagID());

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
    public void shouldDisconnectConnectionWithNoLogoutReply() throws IOException
    {
        setup(true, true);

        setupLibrary();

        try (FixConnection connection = FixConnection.initiate(port))
        {
            logon(connection);

            final Session session = acquireSession();
            logoutSession(session);
            assertSessionDisconnected(testSystem, session);

            assertConnectionDisconnects(testSystem, connection);
        }
    }

    @Test
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

    @Test
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
    public void shouldSupportProxyV1Protocol() throws IOException
    {
        shouldSupportProxyProtocol(FixConnection::sendProxyV1Line, PROXY_SOURCE_IP, PROXY_SOURCE_PORT);
    }

    @Test
    public void shouldSupportProxyV1ProtocolLargest() throws IOException
    {
        shouldSupportProxyProtocol(
            FixConnection::sendProxyV1LargestLine, LARGEST_PROXY_SOURCE_IP, LARGEST_PROXY_SOURCE_PORT);
    }

    @Test
    public void shouldSupportProxyV2ProtocolTcpV4() throws IOException
    {
        shouldSupportProxyProtocol(FixConnection::sendProxyV2LineTcpV4, PROXY_SOURCE_IP, PROXY_V2_SOURCE_PORT);
    }

    @Test
    public void shouldSupportProxyV2ProtocolTcpV6() throws IOException
    {
        shouldSupportProxyProtocol(
            FixConnection::sendProxyV2LineTcpV6, PROXY_V2_IPV6_SOURCE_IP, PROXY_V2_IPV6_SOURCE_PORT);
    }

    @Test
    public void shouldSupportProxyV2ProtocolTcpV6Localhost() throws IOException
    {
        shouldSupportProxyProtocol(
            FixConnection::sendProxyV2LineTcpV6Localhost, "::1", PROXY_V2_IPV6_SOURCE_PORT);
    }

    private void shouldSupportProxyProtocol(
        final Consumer<FixConnection> sendLine, final String proxySourceIp, final int proxySourcePort)
        throws IOException
    {
        setup(true, true);

        setupLibrary();

        try (FixConnection connection = FixConnection.initiate(port))
        {
            sendLine.accept(connection);
            logon(connection);

            final Session session = acquireSession();

            assertEquals(proxySourceIp, session.connectedHost());
            assertEquals(proxySourcePort, session.connectedPort());
        }
    }

    private Session acquireSession()
    {
        final long sessionId = handler.awaitSessionId(testSystem::poll);
        handler.clearSessionExistsInfos();
        final Session session = SystemTestUtil.acquireSession(handler, library, sessionId, testSystem);
        assertNotNull(session);
        return session;
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
