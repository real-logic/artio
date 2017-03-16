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

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import uk.co.real_logic.fix_gateway.Reply;
import uk.co.real_logic.fix_gateway.engine.FixEngine;
import uk.co.real_logic.fix_gateway.engine.SessionInfo;
import uk.co.real_logic.fix_gateway.engine.framer.LibraryInfo;
import uk.co.real_logic.fix_gateway.library.FixLibrary;
import uk.co.real_logic.fix_gateway.library.LibraryConfiguration;
import uk.co.real_logic.fix_gateway.messages.SessionReplyStatus;
import uk.co.real_logic.fix_gateway.messages.SessionState;
import uk.co.real_logic.fix_gateway.session.Session;

import java.util.Arrays;
import java.util.List;
import java.util.function.IntSupplier;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static uk.co.real_logic.fix_gateway.FixMatchers.*;
import static uk.co.real_logic.fix_gateway.TestFixtures.launchMediaDriver;
import static uk.co.real_logic.fix_gateway.Timing.DEFAULT_TIMEOUT_IN_MS;
import static uk.co.real_logic.fix_gateway.Timing.assertEventuallyTrue;
import static uk.co.real_logic.fix_gateway.decoder.Constants.MSG_SEQ_NUM;
import static uk.co.real_logic.fix_gateway.decoder.Constants.TEST_REQUEST_AS_STR;
import static uk.co.real_logic.fix_gateway.engine.FixEngine.ENGINE_LIBRARY_ID;
import static uk.co.real_logic.fix_gateway.library.FixLibrary.NO_MESSAGE_REPLAY;
import static uk.co.real_logic.fix_gateway.messages.SessionReplyStatus.OK;
import static uk.co.real_logic.fix_gateway.messages.SessionReplyStatus.SEQUENCE_NUMBER_TOO_HIGH;
import static uk.co.real_logic.fix_gateway.messages.SessionState.DISABLED;
import static uk.co.real_logic.fix_gateway.system_tests.FixMessage.hasMessageSequenceNumber;
import static uk.co.real_logic.fix_gateway.system_tests.SystemTestUtil.*;

public class GatewayToGatewaySystemTest extends AbstractGatewayToGatewaySystemTest
{
    private FakeConnectHandler fakeConnectHandler = new FakeConnectHandler();

    @Before
    public void launch()
    {
        delete(ACCEPTOR_LOGS);

        mediaDriver = launchMediaDriver();

        launchAcceptingEngine();
        initiatingEngine = launchInitiatingEngine(libraryAeronPort);

        final LibraryConfiguration acceptingLibraryConfig = acceptingLibraryConfig(acceptingHandler);
        acceptingLibraryConfig.libraryConnectHandler(fakeConnectHandler);
        acceptingLibrary = connect(acceptingLibraryConfig);
        initiatingLibrary = newInitiatingLibrary(libraryAeronPort, initiatingHandler);
        testSystem = new TestSystem(acceptingLibrary, initiatingLibrary);

        connectSessions();
    }

    @Test
    public void messagesCanBeSentFromInitiatorToAcceptor()
    {
        messagesCanBeExchanged();

        assertInitiatingSequenceIndexIs(0);
    }

    @Test
    public void messagesCanBeSentFromInitiatorToAcceptingLibrary()
    {
        acquireAcceptingSession();

        messagesCanBeExchanged();

        assertSequenceIndicesAre(0);
    }

    @Test
    public void gatewayProcessesResendRequests()
    {
        acquireAcceptingSession();

        messagesCanBeSentFromInitiatorToAcceptor();

        final int sequenceNumber = sendResendRequest();

        assertMessageResent(sequenceNumber);

        assertSequenceIndicesAre(0);
    }

    @Test
    public void messagesCanBeSentFromAcceptorToInitiator()
    {
        acquireAcceptingSession();

        messagesCanBeExchanged(acceptingSession, acceptingOtfAcceptor);

        assertSequenceIndicesAre(0);
    }

    @Test
    public void initiatorSessionCanBeDisconnected()
    {
        acquireAcceptingSession();

        initiatingSession.startLogout();

        assertSessionsDisconnected();

        assertSequenceIndicesAre(0);
    }

    @Test
    public void acceptorSessionCanBeDisconnected()
    {
        acquireAcceptingSession();

        logoutAcceptingSession();

        assertSessionsDisconnected();

        assertSequenceIndicesAre(0);
    }

    @Test
    public void sessionsCanReconnect()
    {
        acquireAcceptingSession();

        acceptingSession.startLogout();
        assertSessionsDisconnected();

        assertAllMessagesHaveSequenceIndex(0);
        clearMessages();

        wireSessions();

        messagesCanBeExchanged();

        assertSequenceIndicesAre(1);
    }

    @Test
    public void sessionsListedInAdminApi()
    {
        final List<LibraryInfo> libraries = libraries(initiatingEngine);
        assertThat(libraries, hasSize(2));

        final LibraryInfo library = libraries.get(0);
        assertEquals(initiatingLibrary.libraryId(), library.libraryId());

        final List<SessionInfo> sessions = library.sessions();
        assertThat(sessions, hasSize(1));

        final SessionInfo sessionInfo = sessions.get(0);
        assertThat(sessionInfo.address(), containsString("localhost"));
        assertThat(sessionInfo.address(), containsString(String.valueOf(port)));
        assertEquals(initiatingSession.connectionId(), sessionInfo.connectionId());

        assertEquals(initiatingSession.connectedPort(), port);
        assertEquals(initiatingSession.connectedHost(), "localhost");

        final LibraryInfo gatewayLibraryInfo = libraries.get(1);
        assertEquals(ENGINE_LIBRARY_ID, gatewayLibraryInfo.libraryId());
        assertThat(gatewayLibraryInfo.sessions(), hasSize(0));
    }

    @Test
    public void multipleLibrariesCanExchangeMessages()
    {
        final int initiator1MessageCount = initiatingOtfAcceptor.messages().size();

        final FakeOtfAcceptor initiatingOtfAcceptor2 = new FakeOtfAcceptor();
        final FakeHandler initiatingSessionHandler2 = new FakeHandler(initiatingOtfAcceptor2);
        try (FixLibrary library2 = testSystem.add(newInitiatingLibrary(libraryAeronPort, initiatingSessionHandler2)))
        {
            acceptingHandler.clearSessions();
            final Reply<Session> reply = initiate(library2, port, INITIATOR_ID2, ACCEPTOR_ID);
            awaitLibraryReply(testSystem, reply);

            final Session session2 = reply.resultIfPresent();

            assertConnected(session2);
            sessionLogsOn(testSystem, session2, DEFAULT_TIMEOUT_IN_MS);

            final long sessionId = acceptingHandler.awaitSessionIdFor(
                INITIATOR_ID2,
                ACCEPTOR_ID,
                testSystem::poll,
                1000);

            final Session acceptingSession2 = acquireSession(acceptingHandler, acceptingLibrary, sessionId);

            assertTestRequestSentAndReceived(acceptingSession2, testSystem, initiatingOtfAcceptor2);

            assertThat(session2, hasSequenceIndex(0));

            assertOriginalLibraryDoesNotReceiveMessages(initiator1MessageCount);
        }

        assertInitiatingSequenceIndexIs(0);
    }

    @Test
    public void sequenceNumbersShouldResetOverDisconnects()
    {
        acquireAcceptingSession();

        messagesCanBeExchanged();
        assertSequenceFromInitToAcceptAt(2, 2);

        initiatingSession.startLogout();

        assertSequenceIndicesAre(0);
        clearMessages();
        assertSessionsDisconnected();

        wireSessions();

        assertSequenceFromInitToAcceptAt(1, 1);

        assertTestRequestSentAndReceived(initiatingSession, testSystem, acceptingOtfAcceptor);

        assertSequenceIndicesAre(1);
    }

    @Test
    public void acceptorsShouldHandleInitiatorDisconnectsGracefully()
    {
        acquireAcceptingSession();

        assertFalse("Premature Acceptor Disconnect", acceptingHandler.hasDisconnected());

        initiatingEngine.close();

        assertEventuallyTrue("Acceptor Disconnected",
            () ->
            {
                testSystem.poll();
                return acceptingHandler.hasDisconnected();
            });

        assertSequenceIndicesAre(0);
    }

    @Test
    public void librariesShouldBeAbleToReleaseInitiatedSessionToEngine()
    {
        acquireAcceptingSession();

        releaseSessionToEngine(initiatingSession, initiatingLibrary, initiatingEngine);
    }

    @Test
    public void librariesShouldBeAbleToReleaseAcceptedSessionToEngine()
    {
        acquireAcceptingSession();

        releaseSessionToEngine(acceptingSession, acceptingLibrary, acceptingEngine);
    }

    @Test
    public void librariesShouldBeAbleToAcquireReleasedInitiatedSessions()
    {
        acquireAcceptingSession();

        final long sessionId = initiatingSession.id();

        releaseToGateway(initiatingLibrary, initiatingSession);

        libraryNotifiedThatGatewayOwnsSession(initiatingHandler, sessionId);

        reacquireSession(
            initiatingSession, initiatingLibrary, initiatingEngine,  initiatingOtfAcceptor,
            sessionId, NO_MESSAGE_REPLAY, NO_MESSAGE_REPLAY, OK);

        assertSequenceIndicesAre(0);
    }

    @Test
    public void librariesShouldBeAbleToAcquireReleasedAcceptedSessions()
    {
        acquireAcceptingSession();

        final long sessionId = acceptingSession.id();
        acceptingHandler.clearSessions();

        releaseToGateway(acceptingLibrary, acceptingSession);

        libraryNotifiedThatGatewayOwnsSession(acceptingHandler, sessionId);

        reacquireSession(
            acceptingSession, acceptingLibrary, acceptingEngine, acceptingOtfAcceptor,
            sessionId, NO_MESSAGE_REPLAY, NO_MESSAGE_REPLAY, OK);

        assertSequenceIndicesAre(0);
    }

    @Test
    public void shouldReceiveCatchupReplayAfterReconnect()
    {
        shouldReceiveCatchupReplay(() -> acceptingSession.sequenceIndex(), OK);
    }

    @Test
    public void shouldReceiveCatchupReplayForSequenceNumberTooHigh()
    {
        shouldReceiveCatchupReplay(() -> 100, SEQUENCE_NUMBER_TOO_HIGH);
    }

    private void shouldReceiveCatchupReplay(
        final IntSupplier sequenceIndexSupplier,
        final SessionReplyStatus expectedStatus)
    {
        acquireAcceptingSession();

        disconnectSessions();

        assertThat(initiatingLibrary.sessions(), hasSize(0));
        assertThat(acceptingLibrary.sessions(), hasSize(0));

        final long sessionId = acceptingSession.id();
        final int lastReceivedMsgSeqNum = acceptingSession.lastReceivedMsgSeqNum();
        final int sequenceIndex = sequenceIndexSupplier.getAsInt();

        assertSequenceIndicesAre(0);
        clearMessages();

        connectSessions();

        reacquireSession(
            acceptingSession, acceptingLibrary, acceptingEngine, acceptingOtfAcceptor,
            sessionId, lastReceivedMsgSeqNum, sequenceIndex,
            expectedStatus);

        acceptingSession = acceptingHandler.lastSession();
        acceptingHandler.resetSession();

        // Send messages both ways to ensure that the session is setup
        messagesCanBeExchanged(acceptingSession, acceptingOtfAcceptor);
        messagesCanBeExchanged(initiatingSession, initiatingOtfAcceptor);

        assertSequenceIndicesAre(1);
    }

    @Test
    public void enginesShouldManageAcceptingSession()
    {
        acquireAcceptingSession();

        engineShouldManageSession(
            acceptingSession, acceptingLibrary, acceptingOtfAcceptor,
            initiatingSession, initiatingOtfAcceptor);
    }

    @Test
    public void enginesShouldManageInitiatingSession()
    {
        acquireAcceptingSession();

        engineShouldManageSession(
            initiatingSession, initiatingLibrary, initiatingOtfAcceptor,
            acceptingSession, acceptingOtfAcceptor);
    }

    @Test
    public void librariesShouldNotBeAbleToAcquireSessionsThatDontExist()
    {
        final SessionReplyStatus status = requestSession(initiatingLibrary, 42, NO_MESSAGE_REPLAY, NO_MESSAGE_REPLAY);

        assertEquals(SessionReplyStatus.UNKNOWN_SESSION, status);
    }

    @Test
    public void librariesShouldBeNotifiedOfGatewayManagedSessionsOnConnect()
    {
        try (LibraryDriver library2 = new LibraryDriver())
        {
            assertEquals(1, library2.awaitSessionId());
        }
    }

    @Test
    public void engineAndLibraryPairsShouldBeRestartable()
    {
        messagesCanBeExchanged();

        testSystem.close(acceptingLibrary);
        acceptingEngine.close();
        assertSequenceIndicesAre(0);
        clearMessages();

        launchAcceptingEngine();
        acceptingLibrary = testSystem.add(newAcceptingLibrary(acceptingHandler));

        wireSessions();
        messagesCanBeExchanged();

        assertSequenceIndicesAre(1);
    }

    @Test
    public void enginesShouldBeRestartable()
    {
        messagesCanBeExchanged();

        acceptingEngine.close();

        assertAllMessagesHaveSequenceIndex(0);
        clearMessages();

        assertEventuallyTrue(
            "Library failed to disconnect",
            () ->
            {
                poll(acceptingLibrary, initiatingLibrary);
                return !acceptingLibrary.isConnected();
            });

        launchAcceptingEngine();

        testSystem.close(acceptingLibrary);

        acceptingLibrary = testSystem.add(newAcceptingLibrary(acceptingHandler));

        wireSessions();

        messagesCanBeExchanged();

        assertSequenceIndicesAre(1);
    }

    @Test
    public void engineShouldAcquireTimedOutLibrariesSessions()
    {
        acquireAcceptingSession();

        acceptingEngineHasSessionAndLibraryIsNotified();
    }

    @Test
    public void engineShouldAcquireClosedLibrariesSessions()
    {
        acquireAcceptingSession();
        acceptingLibrary.close();

        assertEquals(DISABLED, acceptingSession.state());

        acceptingEngineHasSessionAndLibraryIsNotified();
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldNotAllowClosingMidPoll()
    {
        fakeConnectHandler.shouldCloseOnDisconnect();

        acceptingEngine.close();

        awaitIsConnected(false, acceptingLibrary);
    }

    @Test
    public void shouldReconnectToBouncedGatewayViaIpc()
    {
        acceptingEngine.close();

        awaitIsConnected(false, acceptingLibrary);

        launchAcceptingEngine();

        awaitIsConnected(true, acceptingLibrary);
    }

    @Test
    public void shouldReconnectToBouncedGatewayViaUdp()
    {
        initiatingEngine.close();

        awaitIsConnected(false, initiatingLibrary);

        initiatingEngine = launchInitiatingEngine(libraryAeronPort);

        awaitIsConnected(true, initiatingLibrary);
    }

    @Test
    public void shouldReconnectToBouncedGatewayWithoutTimeout()
    {
        acquireAcceptingSession();

        messagesCanBeExchanged();

        assertTrue("Session not active", acceptingSession.isActive());

        acceptingEngine.close();

        launchAcceptingEngine();

        // Hasn't initially detected the library that was previously connected.
        assertThat(SystemTestUtil.libraries(acceptingEngine), hasSize(1));

        assertEventuallyTrue(
            "Session never disconnects",
            () ->
            {
                testSystem.poll();
                return !acceptingSession.isActive();
            });

        SystemTestUtil.assertEventuallyHasLibraries(
            testSystem,
            acceptingEngine,
            matchesLibrary(acceptingLibrary.libraryId()),
            matchesLibrary(ENGINE_LIBRARY_ID));

        connectSessions();

        messagesCanBeExchanged();
    }

    @Ignore
    @Test
    public void shouldExchangeLargeMessages()
    {
        acquireAcceptingSession();

        final char[] testReqIDChars = new char[SESSION_BUFFER_SIZE_IN_BYTES - 100];
        Arrays.fill(testReqIDChars, 'A');
        final String testReqID = new String(testReqIDChars);

        sendTestRequest(acceptingSession, testReqID);

        assertReceivedSingleHeartbeat(testSystem, initiatingOtfAcceptor, testReqID);
    }

    private void awaitIsConnected(final boolean connected, final FixLibrary library)
    {
        assertEventuallyTrue(
            "isConnect never became: " + connected,
            () ->
            {
                testSystem.poll();
                return library.isConnected() == connected;
            });
    }

    private void releaseSessionToEngine(
        final Session session,
        final FixLibrary library,
        final FixEngine engine)
    {
        final long connectionId = session.connectionId();

        final SessionReplyStatus status = releaseToGateway(library, session);

        assertEquals(OK, status);
        assertEquals(SessionState.DISABLED, session.state());
        assertThat(library.sessions(), hasSize(0));

        final List<SessionInfo> sessions = gatewayLibraryInfo(engine).sessions();
        assertThat(sessions, contains(hasConnectionId(connectionId)));
    }

    private void reacquireSession(
        final Session session,
        final FixLibrary library,
        final FixEngine engine,
        final FakeOtfAcceptor otfAcceptor,
        final long sessionId,
        final int lastReceivedMsgSeqNum,
        final int sequenceIndex,
        final SessionReplyStatus expectedStatus)
    {
        final SessionReplyStatus status = requestSession(library, sessionId, lastReceivedMsgSeqNum, sequenceIndex);
        assertEquals(expectedStatus, status);

        assertThat(gatewayLibraryInfo(engine).sessions(), hasSize(0));

        engineIsManagingSession(engine, session.id());

        assertEventuallyTrue(
            "library manages session",
            () ->
            {
                testSystem.poll();
                assertContainsOnlySession(session, library);
            });

        // Has received catch up messages
        if (lastReceivedMsgSeqNum != NO_MESSAGE_REPLAY && status == OK)
        {
            final FixMessage firstReplayedMessage = otfAcceptor.messages().get(0);
            assertThat(firstReplayedMessage, hasMessageSequenceNumber(1));
            assertEquals(1, firstReplayedMessage.sequenceIndex());
            assertEquals("Y", firstReplayedMessage.getPossDup());
        }
    }

    private void assertContainsOnlySession(final Session session, final FixLibrary library)
    {
        final List<Session> sessions = library.sessions();
        assertThat(sessions, hasSize(1));

        final Session newSession = sessions.get(0);
        assertTrue(newSession.isConnected());
        assertEquals(session.id(), newSession.id());
        assertEquals(session.username(), newSession.username());
        assertEquals(session.password(), newSession.password());
    }

    private void engineIsManagingSession(final FixEngine engine, final long sessionId)
    {
        final List<LibraryInfo> libraries = libraries(engine);
        assertThat(libraries.get(0).sessions(), contains(hasSessionId(sessionId)));
    }

    private void engineShouldManageSession(
        final Session session,
        final FixLibrary library,
        final FakeOtfAcceptor otfAcceptor,
        final Session otherSession,
        final FakeOtfAcceptor otherAcceptor)
    {
        final long sessionId = session.id();
        final int lastReceivedMsgSeqNum = session.lastReceivedMsgSeqNum();
        final int sequenceIndex = session.sequenceIndex();
        final List<FixMessage> messages = otfAcceptor.messages();

        releaseToGateway(library, session);

        messagesCanBeExchanged(otherSession, otherAcceptor);

        final SessionReplyStatus status = requestSession(library, sessionId, lastReceivedMsgSeqNum, sequenceIndex);
        assertEquals(OK, status);

        final List<Session> sessions = library.sessions();
        assertThat(sessions, hasSize(1));

        final Session newSession = sessions.get(0);
        assertNotSame(session, newSession);

        messagesCanBeExchanged(otherSession, otherAcceptor);

        // Callbacks for the missing messages whilst the gateway managed them
        final String expectedSeqNum = String.valueOf(lastReceivedMsgSeqNum + 1);
        assertEquals(messages.toString(), 1, messages
            .stream()
            .filter(msg -> msg.getMsgType().equals(TEST_REQUEST_AS_STR) && msg.get(MSG_SEQ_NUM).equals(expectedSeqNum))
            .count());
    }

    private void disconnectSessions()
    {
        logoutAcceptingSession();

        assertSessionsDisconnected();

        acceptingSession.close();
        initiatingSession.close();
    }

    private void logoutAcceptingSession()
    {
        assertThat(acceptingSession.startLogout(), greaterThan(0L));
    }

    private void libraryNotifiedThatGatewayOwnsSession(final FakeHandler handler, final long expectedSessionId)
    {
        final long sessionId = handler.awaitSessionId(() -> testSystem.poll());

        assertEquals(sessionId, expectedSessionId);
    }
}
