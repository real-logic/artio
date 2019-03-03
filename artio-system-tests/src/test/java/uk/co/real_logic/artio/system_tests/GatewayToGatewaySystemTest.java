/*
 * Copyright 2015-2018 Real Logic Ltd, Adaptive Financial Consulting Ltd.
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
package uk.co.real_logic.artio.system_tests;

import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.artio.*;
import uk.co.real_logic.artio.builder.ExampleMessageEncoder;
import uk.co.real_logic.artio.builder.ExecutionReportEncoder;
import uk.co.real_logic.artio.builder.ResendRequestEncoder;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.engine.SessionInfo;
import uk.co.real_logic.artio.engine.framer.LibraryInfo;
import uk.co.real_logic.artio.library.FixLibrary;
import uk.co.real_logic.artio.library.LibraryConfiguration;
import uk.co.real_logic.artio.messages.SessionReplyStatus;
import uk.co.real_logic.artio.session.CompositeKey;
import uk.co.real_logic.artio.session.Session;

import java.util.List;
import java.util.function.IntSupplier;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static uk.co.real_logic.artio.Constants.*;
import static uk.co.real_logic.artio.FixMatchers.*;
import static uk.co.real_logic.artio.TestFixtures.largeTestReqId;
import static uk.co.real_logic.artio.TestFixtures.launchMediaDriver;
import static uk.co.real_logic.artio.Timing.assertEventuallyTrue;
import static uk.co.real_logic.artio.engine.FixEngine.ENGINE_LIBRARY_ID;
import static uk.co.real_logic.artio.library.FixLibrary.NO_MESSAGE_REPLAY;
import static uk.co.real_logic.artio.messages.SessionReplyStatus.OK;
import static uk.co.real_logic.artio.messages.SessionReplyStatus.SEQUENCE_NUMBER_TOO_HIGH;
import static uk.co.real_logic.artio.messages.SessionState.DISABLED;
import static uk.co.real_logic.artio.system_tests.FixMessage.hasMessageSequenceNumber;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.*;

public class GatewayToGatewaySystemTest extends AbstractGatewayToGatewaySystemTest
{
    private final FakeConnectHandler fakeConnectHandler = new FakeConnectHandler();

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
        final String testReqID = "AAA";

        gatewayProcessesResendRequests(testReqID);
    }

    @Test
    public void gatewayProcessesResendRequestsOfFragmentedMessages()
    {
        final String testReqID = largeTestReqId();

        gatewayProcessesResendRequests(testReqID);
    }

    private void gatewayProcessesResendRequests(final String testReqID)
    {
        acquireAcceptingSession();

        final FixMessage message = exchangeExampleMessageFromInitiatorToAcceptor(testReqID);

        final int sequenceNumber = acceptorSendsResendRequest(message.messageSequenceNumber());

        final FixMessage resentMessage = assertMessageResent(sequenceNumber, EXAMPLE_MESSAGE_MESSAGE_AS_STR, false);
        assertEquals(testReqID, resentMessage.testReqId());

        assertSequenceIndicesAre(0);
    }

    private FixMessage exchangeExampleMessageFromInitiatorToAcceptor(final String testReqID)
    {
        final ExampleMessageEncoder exampleMessage = new ExampleMessageEncoder();
        exampleMessage.testReqID(testReqID);
        final long position = initiatingSession.send(exampleMessage);
        assertThat(position, greaterThan(0L));

        return testSystem.awaitMessageOf(acceptingOtfAcceptor, EXAMPLE_MESSAGE_MESSAGE_AS_STR);
    }

    @Test
    public void gatewayProcessesResendRequestsOfAdminMessages()
    {
        acquireAcceptingSession();

        messagesCanBeSentFromInitiatorToAcceptor();

        final int sequenceNumber = acceptorSendsResendRequest();

        assertMessageResent(sequenceNumber, SEQUENCE_RESET_MESSAGE_AS_STR, true);

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
        super.sessionsCanReconnect();
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
            final Reply<Session> reply = testSystem.awaitReply(initiate(library2, port, INITIATOR_ID2, ACCEPTOR_ID));

            final Session session2 = reply.resultIfPresent();

            assertConnected(session2);

            final long sessionId = acceptingHandler.awaitSessionIdFor(
                INITIATOR_ID2,
                ACCEPTOR_ID,
                testSystem::poll,
                1000);

            final Session acceptingSession2 = acquireSession(acceptingHandler, acceptingLibrary, sessionId, testSystem);

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
        assertSessionsDisconnected();
        clearMessages();

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

        releaseToGateway(initiatingLibrary, initiatingSession, testSystem);

        libraryNotifiedThatGatewayOwnsSession(initiatingHandler, sessionId);

        reacquireSession(
            initiatingSession, initiatingLibrary, initiatingEngine,
            sessionId, NO_MESSAGE_REPLAY, NO_MESSAGE_REPLAY, OK);

        assertSequenceIndicesAre(0);
    }

    @Test
    public void librariesShouldBeAbleToAcquireReleasedAcceptedSessions()
    {
        acquireAcceptingSession();

        final long sessionId = acceptingSession.id();
        acceptingHandler.clearSessions();

        releaseToGateway(acceptingLibrary, acceptingSession, testSystem);

        libraryNotifiedThatGatewayOwnsSession(acceptingHandler, sessionId);

        reacquireSession(
            acceptingSession, acceptingLibrary, acceptingEngine,
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

        final int lastReceivedMsgSeqNum = acceptingSession.lastReceivedMsgSeqNum();
        final String testReqID = largeTestReqId();
        exchangeExampleMessageFromInitiatorToAcceptor(testReqID);

        disconnectSessions();

        assertThat(initiatingLibrary.sessions(), hasSize(0));
        assertThat(acceptingLibrary.sessions(), hasSize(0));

        final long sessionId = acceptingSession.id();
        final int sequenceIndex = sequenceIndexSupplier.getAsInt();

        assertSequenceIndicesAre(0);
        clearMessages();

        connectSessions();

        reacquireSession(
            acceptingSession, acceptingLibrary, acceptingEngine,
            sessionId, lastReceivedMsgSeqNum, sequenceIndex,
            expectedStatus);

        if (expectedStatus == OK)
        {
            final FixMessage replayedExampleMessage = acceptingOtfAcceptor.messages().get(1);
            assertEquals(Constants.EXAMPLE_MESSAGE_MESSAGE_AS_STR, replayedExampleMessage.msgType());
            assertThat(replayedExampleMessage, hasMessageSequenceNumber(2));
            assertEquals(0, replayedExampleMessage.sequenceIndex());
            assertEquals("Y", replayedExampleMessage.possDup());
            assertEquals(testReqID, replayedExampleMessage.testReqId());
        }

        acceptingSession = acceptingHandler.lastSession();
        acceptingHandler.resetSession();

        // Send messages both ways to ensure that the session is setup
        messagesCanBeExchanged(acceptingSession, acceptingOtfAcceptor);
        messagesCanBeExchanged(initiatingSession, initiatingOtfAcceptor);

        assertAcceptingSessionHasSequenceIndex(1);
        assertInitiatingSequenceIndexIs(1);
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
        final SessionReplyStatus status = requestSession(
            initiatingLibrary, 42, NO_MESSAGE_REPLAY, NO_MESSAGE_REPLAY, testSystem);

        assertEquals(SessionReplyStatus.UNKNOWN_SESSION, status);
    }

    @Test
    public void librariesShouldBeNotifiedOfGatewayManagedSessionsOnConnect()
    {
        try (LibraryDriver library2 = LibraryDriver.accepting(testSystem))
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
                testSystem.poll();

                return !acceptingLibrary.isConnected();
            });

        testSystem.close(acceptingLibrary);

        launchAcceptingEngine();

        acceptingLibrary = testSystem.add(newAcceptingLibrary(acceptingHandler));

        assertTrue("acceptingLibrary has failed to connect", acceptingLibrary.isConnected());
        assertTrue("initiatingLibrary is no longer connected", initiatingLibrary.isConnected());

        wireSessions();

        messagesCanBeExchanged();

        assertSequenceIndicesAre(1);
    }

    @Test
    public void engineShouldAcquireTimedOutAcceptingSessions()
    {
        acquireAcceptingSession();

        testSystem.remove(acceptingLibrary);

        acceptingEngineHasSessionAndLibraryIsNotified();
    }

    @Test
    public void engineShouldAcquireTimedOutInitiatingSessions()
    {
        testSystem.remove(initiatingLibrary);

        initiatingEngineHasSessionAndLibraryIsNotified();
    }

    @Test
    public void engineShouldAcquireAcceptingSessionsFromClosedLibrary()
    {
        acquireAcceptingSession();
        acceptingLibrary.close();

        assertEquals(DISABLED, acceptingSession.state());

        acceptingEngineHasSessionAndLibraryIsNotified();
    }

    @Test
    public void engineShouldAcquireInitiatingSessionsFromClosedLibrary()
    {
        initiatingLibrary.close();

        assertEquals(DISABLED, initiatingSession.state());

        initiatingEngineHasSessionAndLibraryIsNotified();
    }

    @Test
    public void libraryShouldSeeReleasedAcceptingSession()
    {
        acquireAcceptingSession();

        releaseSessionToEngine(acceptingSession, acceptingLibrary, acceptingEngine);

        try (LibraryDriver library2 = LibraryDriver.accepting(testSystem))
        {
            final CompleteSessionId sessionId = library2.awaitCompleteSessionId();
            assertSameSession(sessionId, acceptingSession);
        }
    }

    @Test
    public void libraryShouldSeeReleasedInitiatingSession()
    {
        releaseSessionToEngine(initiatingSession, initiatingLibrary, initiatingEngine);

        try (LibraryDriver library2 = LibraryDriver.initiating(libraryAeronPort, testSystem))
        {
            final CompleteSessionId sessionId = library2.awaitCompleteSessionId();
            assertSameSession(sessionId, initiatingSession);

            try (LibraryDriver library3 = LibraryDriver.initiating(libraryAeronPort, testSystem))
            {
                final CompleteSessionId sessionId3 = library3.awaitCompleteSessionId();
                assertSameSession(sessionId3, initiatingSession);

                try (LibraryDriver library4 = LibraryDriver.initiating(libraryAeronPort, testSystem))
                {
                    final CompleteSessionId sessionId4 = library4.awaitCompleteSessionId();
                    assertSameSession(sessionId4, initiatingSession);
                }
            }
        }
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

    @Test
    public void shouldExchangeLargeMessages()
    {
        acquireAcceptingSession();

        final String testReqID = largeTestReqId();

        sendTestRequest(acceptingSession, testReqID);

        assertReceivedSingleHeartbeat(testSystem, acceptingOtfAcceptor, testReqID);
    }

    @Test
    public void shouldLookupSessionIdsOfSessions()
    {
        final long sessionId = lookupSessionId(INITIATOR_ID, ACCEPTOR_ID, initiatingEngine).resultIfPresent();

        assertEquals(initiatingSession.id(), sessionId);
    }

    @Test
    public void shouldNotLookupSessionIdsOfUnknownSessions()
    {
        final Reply<Long> sessionIdReply = lookupSessionId("foo", "bar", initiatingEngine);

        assertNull(sessionIdReply.resultIfPresent());
        assertThat(sessionIdReply.error(), instanceOf(IllegalArgumentException.class));
    }

    @Test
    public void shouldResetSequenceNumbersOfEngineManagedSessions()
    {
        messagesCanBeExchanged();

        assertInitSeqNum(2, 2, 0);

        final long sessionId = lookupSessionId(ACCEPTOR_ID, INITIATOR_ID, acceptingEngine).resultIfPresent();

        final Reply<?> resetSequenceNumber = resetSequenceNumber(sessionId);
        replyCompleted(resetSequenceNumber);

        assertInitSeqNum(1, 1, 1);
    }

    @Test
    public void shouldNotResetSequenceNumbersOfMissingSession()
    {
        messagesCanBeExchanged();

        assertInitSeqNum(2, 2, 0);

        final Reply<?> resetSequenceNumber = resetSequenceNumber((long)400);
        assertTrue("Should have errored: " + resetSequenceNumber, resetSequenceNumber.hasErrored());
        final String message = resetSequenceNumber.error().getMessage();
        assertTrue(message, message.contains("Unknown sessionId: 400"));

        assertInitSeqNum(2, 2, 0);
    }

    @Test
    public void shouldResetSequenceNumbersOfLibraryManagedSessions()
    {
        messagesCanBeExchanged();

        acquireAcceptingSession();

        testSystem.awaitReceivedSequenceNumber(acceptingSession, 2);

        assertInitSeqNum(2, 2, 0);
        assertAccSeqNum(2, 2, 0);

        final long sessionId = lookupSessionId(ACCEPTOR_ID, INITIATOR_ID, acceptingEngine).resultIfPresent();

        final Reply<?> resetSequenceNumber = resetSequenceNumber(sessionId);
        replyCompleted(resetSequenceNumber);

        testSystem.awaitReceivedSequenceNumber(acceptingSession, 1);

        assertInitSeqNum(1, 1, 1);
        assertAccSeqNum(1, 1, 1);
    }

    private Reply<?> resetSequenceNumber(final long sessionId)
    {
        return testSystem.awaitReply(acceptingEngine.resetSequenceNumber(sessionId));
    }

    private void replyCompleted(final Reply<?> resetSequenceNumber)
    {
        assertTrue("Should be complete: " + resetSequenceNumber, resetSequenceNumber.hasCompleted());
    }

    @Test
    public void shouldCombineGapFilledReplays()
    {
        messagesCanBeExchanged();

        messagesCanBeExchanged();

        final ResendRequestEncoder resendRequest = new ResendRequestEncoder();
        resendRequest.beginSeqNo(1).endSeqNo(0);

        initiatingOtfAcceptor.messages().clear();

        testSystem.send(initiatingSession, resendRequest);

        final FixMessage message = testSystem.awaitMessageOf(initiatingOtfAcceptor, SEQUENCE_RESET_MESSAGE_AS_STR);

        // Logon + two heartbeats gets to 3, next is 4.
        assertEquals("4", message.get(Constants.NEW_SEQ_NO));

        clearMessages();

        messagesCanBeExchanged();
    }

    @Test
    public void shouldReplayAMixOfEngineAndLibraryMessages()
    {
        // Engine messages
        messagesCanBeExchanged();
        messagesCanBeExchanged();

        // Library messages
        acquireAcceptingSession();
        exchangeExecutionReport();

        final ResendRequestEncoder resendRequest = new ResendRequestEncoder();
        resendRequest.beginSeqNo(1).endSeqNo(0);

        initiatingOtfAcceptor.messages().clear();

        testSystem.send(initiatingSession, resendRequest);

        final FixMessage gapFill = testSystem.awaitMessageOf(initiatingOtfAcceptor, SEQUENCE_RESET_MESSAGE_AS_STR);
        assertEquals(1, gapFill.messageSequenceNumber());
        assertEquals(4, Integer.parseInt(gapFill.get(NEW_SEQ_NO)));

        final FixMessage execReport = testSystem.awaitMessageOf(initiatingOtfAcceptor, EXECUTION_REPORT_MESSAGE_AS_STR);
        assertEquals(4, execReport.messageSequenceNumber());

        clearMessages();

        messagesCanBeExchanged();
    }

    private void exchangeExecutionReport()
    {
        final ExecutionReportEncoder executionReport = new ExecutionReportEncoder();
        executionReport
            .orderID("order")
            .execID("exec")
            .execType(ExecType.FILL)
            .ordStatus(OrdStatus.FILLED)
            .side(Side.BUY);
        executionReport.instrument().symbol("IBM");
        assertThat(acceptingSession.send(executionReport), greaterThan(0L));

        testSystem.awaitMessageOf(initiatingOtfAcceptor, EXECUTION_REPORT_MESSAGE_AS_STR);
    }

    private void assertInitSeqNum(
        final int lastReceivedMsgSeqNum, final int lastSentMsgSeqNum, final int sequenceIndex)
    {
        assertSeqNum(lastReceivedMsgSeqNum, lastSentMsgSeqNum, sequenceIndex, initiatingSession);
    }

    private void assertAccSeqNum(
        final int lastReceivedMsgSeqNum, final int lastSentMsgSeqNum, final int sequenceIndex)
    {
        assertSeqNum(lastReceivedMsgSeqNum, lastSentMsgSeqNum, sequenceIndex, acceptingSession);
    }

    private void assertSeqNum(
        final int lastReceivedMsgSeqNum, final int lastSentMsgSeqNum, final int sequenceIndex, final Session session)
    {
        assertEquals(lastReceivedMsgSeqNum, session.lastReceivedMsgSeqNum());
        assertEquals(lastSentMsgSeqNum, session.lastSentMsgSeqNum());
        assertEquals(sequenceIndex, session.sequenceIndex());
    }

    private void awaitIsConnected(final boolean isConnected, final FixLibrary library)
    {
        assertEventuallyTrue(
            "isConnected never became: " + isConnected,
            () ->
            {
                testSystem.poll();
                return library.isConnected() == isConnected;
            });
    }

    private void reacquireSession(
        final Session session,
        final FixLibrary library,
        final FixEngine engine,
        final long sessionId,
        final int lastReceivedMsgSeqNum,
        final int sequenceIndex,
        final SessionReplyStatus expectedStatus)
    {
        final SessionReplyStatus status = requestSession(
            library, sessionId, lastReceivedMsgSeqNum, sequenceIndex, testSystem);
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
    }

    private void assertContainsOnlySession(final Session session, final FixLibrary library)
    {
        final List<Session> sessions = library.sessions();
        assertThat(sessions, hasSize(1));

        final Session newSession = sessions.get(0);
        assertTrue(newSession.isConnected());
        assertEquals(session.id(), newSession.id());
    }

    private void engineIsManagingSession(final FixEngine engine, final long sessionId)
    {
        final List<LibraryInfo> libraries = libraries(engine);
        assertThat(libraries.get(0).sessions(), contains(hasSessionId(sessionId)));
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

    private Reply<Long> lookupSessionId(final String localCompId, final String remoteCompId, final FixEngine engine)
    {
        return testSystem.awaitReply(engine.lookupSessionId(
            localCompId, remoteCompId, null, null, null, null));
    }

    private void acceptingEngineHasSessionAndLibraryIsNotified()
    {
        engineHasSessionAndLibraryIsNotified(LibraryDriver.accepting(testSystem), acceptingEngine, acceptingSession);
    }

    private void initiatingEngineHasSessionAndLibraryIsNotified()
    {
        engineHasSessionAndLibraryIsNotified(
            LibraryDriver.initiating(libraryAeronPort, testSystem), initiatingEngine, initiatingSession);
    }

    private void engineHasSessionAndLibraryIsNotified(
        final LibraryDriver libraryDriver, final FixEngine engine, final Session session)
    {
        try (LibraryDriver library2 = libraryDriver)
        {
            library2.becomeOnlyLibraryConnectedTo(engine);

            final LibraryInfo engineLibraryInfo = engineLibrary(libraries(engine));

            assertEquals(ENGINE_LIBRARY_ID, engineLibraryInfo.libraryId());
            assertThat(engineLibraryInfo.sessions(), contains(hasConnectionId(session.connectionId())));

            final CompleteSessionId sessionId = library2.awaitCompleteSessionId();
            assertSameSession(sessionId, session);
        }
    }

    private void assertSameSession(final CompleteSessionId sessionId, final Session session)
    {
        final CompositeKey compositeKey = session.compositeKey();

        assertEquals(sessionId.surrogateId(), session.id());
        assertEquals(compositeKey.localCompId(), sessionId.localCompId());
        assertEquals(compositeKey.remoteCompId(), sessionId.remoteCompId());
    }
}
