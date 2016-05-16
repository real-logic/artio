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
import org.junit.Test;
import uk.co.real_logic.fix_gateway.engine.EngineConfiguration;
import uk.co.real_logic.fix_gateway.engine.FixEngine;
import uk.co.real_logic.fix_gateway.engine.SessionInfo;
import uk.co.real_logic.fix_gateway.engine.framer.LibraryInfo;
import uk.co.real_logic.fix_gateway.library.FixLibrary;
import uk.co.real_logic.fix_gateway.messages.SessionReplyStatus;
import uk.co.real_logic.fix_gateway.messages.SessionState;
import uk.co.real_logic.fix_gateway.session.Session;

import java.util.List;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static uk.co.real_logic.fix_gateway.CommonMatchers.hasConnectionId;
import static uk.co.real_logic.fix_gateway.TestFixtures.launchMediaDriver;
import static uk.co.real_logic.fix_gateway.Timing.assertEventuallyTrue;
import static uk.co.real_logic.fix_gateway.decoder.Constants.MSG_SEQ_NUM;
import static uk.co.real_logic.fix_gateway.system_tests.SystemTestUtil.*;

public class GatewayToGatewaySystemTest extends AbstractGatewayToGatewaySystemTest
{

    @Before
    public void launch()
    {
        mediaDriver = launchMediaDriver();

        // System GC required to ensure file closed. Hypothesis: finalizer thread holding onto object references?
        System.gc();

        delete(ACCEPTOR_LOGS);
        final EngineConfiguration config = acceptingConfig(port, "engineCounters", ACCEPTOR_ID, INITIATOR_ID);
        acceptingEngine = FixEngine.launch(config);
        initiatingEngine = launchInitiatingGateway(initAeronPort);

        acceptingLibrary = newAcceptingLibrary(acceptingHandler);
        initiatingLibrary = newInitiatingLibrary(initAeronPort, initiatingHandler, 1);

        connectSessions();
    }

    @Test
    public void messagesCanBeSentFromInitiatorToAcceptor()
    {
        messagesCanBeExchanged();
    }

    @Test
    public void messagesCanBeSentFromInitiatorToAcceptingLibrary()
    {
        acquireAcceptingSession();

        messagesCanBeExchanged();
    }

    @Test
    public void gatewayProcessesResendRequests()
    {
        acquireAcceptingSession();

        messagesCanBeSentFromInitiatorToAcceptor();

        sendResendRequest();

        assertMessageResent();
    }

    @Test
    public void messagesCanBeSentFromAcceptorToInitiator()
    {
        acquireAcceptingSession();

        messagesCanBeExchanged(
            acceptingSession, acceptingLibrary, initiatingLibrary, acceptingOtfAcceptor);
    }

    @Test
    public void initiatorSessionCanBeDisconnected()
    {
        acquireAcceptingSession();

        initiatingSession.startLogout();

        assertSessionsDisconnected();
    }

    @Test
    public void acceptorSessionCanBeDisconnected()
    {
        acquireAcceptingSession();

        acceptingSession.startLogout();

        assertSessionsDisconnected();
    }

    @Test
    public void twoSessionsCanConnect()
    {
        acquireAcceptingSession();

        acceptingSession.startLogout();
        assertSessionsDisconnected();

        acceptingOtfAcceptor.messages().clear();
        initiatingOtfAcceptor.messages().clear();

        wireSessions();

        messagesCanBeExchanged();
    }

    @Test
    public void sessionsListedInAdminApi()
    {
        final List<LibraryInfo> libraries = initiatingEngine.libraries(ADMIN_IDLE_STRATEGY);
        assertThat(libraries, hasSize(1));

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
    }

    @Test
    public void multipleLibrariesCanExchangeMessages()
    {
        final int initiator1MessageCount = initiatingOtfAcceptor.messages().size();

        final FakeOtfAcceptor initiatingOtfAcceptor2 = new FakeOtfAcceptor();
        final FakeHandler initiatingSessionHandler2 = new FakeHandler(initiatingOtfAcceptor2);
        try (final FixLibrary library2 = newInitiatingLibrary(initAeronPort, initiatingSessionHandler2, 2))
        {
            acceptingHandler.clearConnections();
            final Session session2 = initiate(library2, port, INITIATOR_ID2, ACCEPTOR_ID).resultIfPresent();

            assertConnected(session2);
            sessionLogsOn(library2, acceptingLibrary, session2);
            // TODO: fix test, sometimes fails due to acquiring the wrong session
            final Session acceptingSession2 = SystemTestUtil.acquireSession(acceptingHandler, acceptingLibrary);

            sendTestRequest(acceptingSession2);
            assertReceivedTestRequest(library2, acceptingLibrary, initiatingOtfAcceptor2);

            assertOriginalLibraryDoesNotReceiveMessages(initiator1MessageCount);
        }
    }

    @Test
    public void sequenceNumbersShouldResetOverDisconnects()
    {
        acquireAcceptingSession();

        messagesCanBeExchanged();
        assertSequenceFromInitToAcceptAt(2, 2);

        initiatingSession.startLogout();

        assertSessionsDisconnected();

        wireSessions();

        assertSequenceFromInitToAcceptAt(1, 1);

        sendTestRequest(initiatingSession);
        assertReceivedTestRequest(initiatingLibrary, acceptingLibrary, acceptingOtfAcceptor);
    }

    @Test
    public void acceptorsShouldHandleInitiatorDisconnectsGracefully()
    {
        acquireAcceptingSession();

        assertFalse("Premature Acceptor Disconnect", acceptingHandler.hasDisconnected());

        initiatingEngine.close();

        assertEventuallyTrue("Acceptor Disconnected", () ->
        {
            acceptingLibrary.poll(1);
            return acceptingHandler.hasDisconnected();
        });
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

    private void releaseSessionToEngine(
        final Session session,
        final FixLibrary library,
        final FixEngine engine)
    {
        final long connectionId = session.connectionId();

        final SessionReplyStatus status = library.releaseToGateway(session);

        assertEquals(SessionReplyStatus.OK, status);
        assertEquals(SessionState.DISABLED, session.state());
        assertThat(library.sessions(), hasSize(0));

        final List<SessionInfo> sessions = engine.gatewaySessions(ADMIN_IDLE_STRATEGY);
        assertThat(sessions,
            contains(hasConnectionId(connectionId)));
    }

    @Test
    public void librariesShouldBeAbleToAcquireReleasedInitiatedSessions()
    {
        acquireAcceptingSession();

        reacquireReleasedSession(
            initiatingSession, initiatingLibrary, initiatingEngine);
    }

    @Test
    public void librariesShouldBeAbleToAcquireReleasedAcceptedSessions()
    {
        acquireAcceptingSession();

        reacquireReleasedSession(
            acceptingSession, acceptingLibrary, acceptingEngine);
    }

    private void reacquireReleasedSession(
        final Session session, final FixLibrary library, final FixEngine engine)
    {
        final long sessionId = session.id();

        library.releaseToGateway(session);

        final SessionReplyStatus status = library.acquireSession(sessionId);
        assertEquals(SessionReplyStatus.OK, status);

        assertThat(engine.gatewaySessions(ADMIN_IDLE_STRATEGY), hasSize(0));

        engineIsManagingSession(engine, session.connectionId());
        assertContainsOnlySession(session, library);
    }

    private void assertContainsOnlySession(final Session session, final FixLibrary library)
    {
        final List<Session> sessions = library.sessions();
        assertThat(sessions, hasSize(1));

        final Session newSession = sessions.get(0);
        assertTrue(newSession.isConnected());
        assertEquals(session.connectionId(), newSession.connectionId());
        assertEquals(session.id(), newSession.id());
        assertEquals(session.username(), newSession.username());
        assertEquals(session.password(), newSession.password());
    }

    private void engineIsManagingSession(final FixEngine engine, final long connectionId)
    {
        final List<LibraryInfo> libraries = engine.libraries(ADMIN_IDLE_STRATEGY);
        assertThat(libraries.get(0).sessions(),
            contains(hasConnectionId(connectionId)));
    }

    @Test
    public void enginesShouldManageAcceptingSession()
    {
        acquireAcceptingSession();

        engineShouldManageSession(
            acceptingSession, acceptingLibrary, acceptingOtfAcceptor,
            initiatingSession, initiatingLibrary, initiatingOtfAcceptor);
    }

    @Test
    public void enginesShouldManageInitiatingSession()
    {
        acquireAcceptingSession();

        engineShouldManageSession(
            initiatingSession, initiatingLibrary, initiatingOtfAcceptor,
            acceptingSession, acceptingLibrary, acceptingOtfAcceptor);
    }

    private void engineShouldManageSession(
        final Session session,
        final FixLibrary library,
        final FakeOtfAcceptor otfAcceptor,
        final Session otherSession,
        final FixLibrary otherLibrary,
        final FakeOtfAcceptor otherAcceptor)
    {
        final long sessionId = session.id();
        final int lastReceivedMsgSeqNum = session.lastReceivedMsgSeqNum();
        final List<FixMessage> messages = otfAcceptor.messages();

        library.releaseToGateway(session);

        messagesCanBeExchanged(otherSession, otherLibrary, library, otherAcceptor);

        final SessionReplyStatus status = library.acquireSession(sessionId, lastReceivedMsgSeqNum);
        assertEquals(SessionReplyStatus.OK, status);

        messagesCanBeExchanged(otherSession, otherLibrary, library, otherAcceptor);

        // Callbacks for the missing messages whilst the gateway managed them
        final String expectedSeqNum = String.valueOf(lastReceivedMsgSeqNum + 1);
        assertEquals(1, messages
            .stream()
            .filter(msg -> msg.getMsgType().equals("1") && msg.get(MSG_SEQ_NUM).equals(expectedSeqNum))
            .count());
    }

    @Test
    public void librariesShouldNotBeAbleToAcquireSessionsThatDontExist()
    {
        final SessionReplyStatus status = initiatingLibrary.acquireSession(42);

        assertEquals(SessionReplyStatus.UNKNOWN_SESSION, status);
    }

    @Test
    public void librariesShouldBeNotifiedOfGatewayManagedSessionsOnConnect()
    {
        final FakeOtfAcceptor otfAcceptor2 = new FakeOtfAcceptor();
        final FakeHandler handler2 = new FakeHandler(otfAcceptor2);
        try (final FixLibrary library2 = FixLibrary.connect(
            acceptingLibraryConfig(handler2, ACCEPTOR_ID, INITIATOR_ID, "fix-acceptor-2")
                .libraryId(2)))
        {
            while (!handler2.hasSession())
            {
                library2.poll(1);
            }

            assertEquals(1, handler2.latestSessionId());
        }
    }
}
