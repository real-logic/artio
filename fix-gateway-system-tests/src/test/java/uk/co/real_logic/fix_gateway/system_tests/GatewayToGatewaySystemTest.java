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
import uk.co.real_logic.fix_gateway.engine.SessionInfo;
import uk.co.real_logic.fix_gateway.engine.framer.LibraryInfo;
import uk.co.real_logic.fix_gateway.library.FixLibrary;
import uk.co.real_logic.fix_gateway.library.session.Session;
import uk.co.real_logic.fix_gateway.library.session.SessionState;
import uk.co.real_logic.fix_gateway.messages.SessionReplyStatus;

import java.util.List;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static uk.co.real_logic.fix_gateway.CommonMatchers.hasConnectionId;
import static uk.co.real_logic.fix_gateway.TestFixtures.launchMediaDriver;
import static uk.co.real_logic.fix_gateway.system_tests.SystemTestUtil.*;

public class GatewayToGatewaySystemTest extends AbstractGatewayToGatewaySystemTest
{

    @Before
    public void launch()
    {
        mediaDriver = launchMediaDriver();

        acceptingEngine = launchAcceptingGateway(port);
        initiatingEngine = launchInitiatingGateway(initAeronPort);

        acceptingLibrary = newAcceptingLibrary(acceptingSessionHandler);
        initiatingLibrary = newInitiatingLibrary(initAeronPort, initiatingSessionHandler, 1);

        connectSessions();
    }

    @Test
    public void sessionHasBeenInitiated() throws InterruptedException
    {
        assertNotNull("Accepting Session not been setup", acceptingSession);
    }

    @Test
    public void messagesCanBeSentFromInitiatorToAcceptor()
    {
        sendTestRequest(initiatedSession);

        assertReceivedTestRequest(initiatingLibrary, acceptingLibrary, acceptingOtfAcceptor);
    }

    @Test
    public void messagesCanBeSentFromAcceptorToInitiator()
    {
        sendTestRequest(acceptingSession);

        assertReceivedTestRequest(initiatingLibrary, acceptingLibrary, initiatingOtfAcceptor);
    }

    @Test
    public void initiatorSessionCanBeDisconnected()
    {
        initiatedSession.startLogout();

        assertSessionsDisconnected();
    }

    @Test
    public void acceptorSessionCanBeDisconnected()
    {
        acceptingSession.startLogout();

        assertSessionsDisconnected();
    }

    @Test
    public void gatewayProcessesResendRequests()
    {
        messagesCanBeSentFromInitiatorToAcceptor();

        sendResendRequest();

        assertMessageResent();
    }

    @Test
    public void twoSessionsCanConnect()
    {
        acceptingSession.startLogout();
        assertSessionsDisconnected();

        acceptingOtfAcceptor.messages().clear();
        initiatingOtfAcceptor.messages().clear();

        connectSessions();

        sendTestRequest(initiatedSession);
        assertReceivedTestRequest(initiatingLibrary, acceptingLibrary, acceptingOtfAcceptor);
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
        assertEquals(initiatedSession.connectionId(), sessionInfo.connectionId());

        assertEquals(initiatedSession.connectedPort(), port);
        assertEquals(initiatedSession.connectedHost(), "localhost");

        assertNotEquals(0, acceptingSession.connectedPort());
        assertEquals("127.0.0.1", acceptingSession.connectedHost());
    }

    @Test
    public void multipleLibrariesCanExchangeMessages()
    {
        final int initiator1MessageCount = initiatingOtfAcceptor.messages().size();

        final FakeOtfAcceptor initiatingOtfAcceptor2 = new FakeOtfAcceptor();
        final FakeSessionHandler initiatingSessionHandler2 = new FakeSessionHandler(initiatingOtfAcceptor2);
        try (final FixLibrary library2 = newInitiatingLibrary(initAeronPort, initiatingSessionHandler2, 2))
        {
            final Session session2 = initiate(library2, port, INITIATOR_ID2, ACCEPTOR_ID);

            assertConnected(session2);
            sessionLogsOn(library2, acceptingLibrary, session2);
            final Session acceptingSession2 = acceptSession(acceptingSessionHandler, acceptingLibrary);

            sendTestRequest(acceptingSession2);
            assertReceivedTestRequest(library2, acceptingLibrary, initiatingOtfAcceptor2);

            assertOriginalLibraryDoesntReceiveMessages(initiator1MessageCount);
        }
    }

    @Test
    public void sequenceNumbersShouldResetOverDisconnects()
    {
        sendTestRequest(initiatedSession);
        assertReceivedTestRequest(initiatingLibrary, acceptingLibrary, acceptingOtfAcceptor);
        assertSequenceFromInitToAcceptAt(2);

        initiatedSession.startLogout();

        assertSessionsDisconnected();

        connectSessions();

        sendTestRequest(initiatedSession);
        assertReceivedTestRequest(initiatingLibrary, acceptingLibrary, acceptingOtfAcceptor, 4);
        assertSequenceFromInitToAcceptAt(2);
    }

    @Test
    public void acceptorsShouldHandleInitiatorDisconnectsGracefully()
    {
        //initiatingLibrary.close();
        initiatingEngine.close();

        //LockSupport.parkNanos(10_000_000_000L);
    }

    @Test
    public void librariesShouldBeAbleToReleaseSessionToTheGateway()
    {
        final long connectionId = initiatedSession.connectionId();

        final SessionReplyStatus status = releaseInitiatedSession();

        assertEquals(SessionReplyStatus.OK, status);
        assertEquals(SessionState.DISABLED, initiatedSession.state());
        assertThat(initiatingLibrary.sessions(), hasSize(0));

        final List<SessionInfo> sessions = initiatingEngine.gatewaySessions(ADMIN_IDLE_STRATEGY);
        assertThat(sessions,
            contains(hasConnectionId(connectionId)));
    }

    @Test
    public void librariesShouldBeAbleToAcquireReleasedSessions()
    {
        final long connectionId = initiatedSession.connectionId();
        final long sessionId = initiatedSession.id();

        releaseInitiatedSession();

        final SessionReplyStatus status = initiatingLibrary.acquireSession(connectionId, ADMIN_IDLE_STRATEGY);

        assertEquals(SessionReplyStatus.OK, status);

        assertThat(initiatingEngine.gatewaySessions(ADMIN_IDLE_STRATEGY), hasSize(0));

        final List<LibraryInfo> libraries = initiatingEngine.libraries(ADMIN_IDLE_STRATEGY);
        assertThat(libraries.get(0).sessions(),
            contains(hasConnectionId(connectionId)));

        final List<Session> sessions = initiatingLibrary.sessions();
        assertThat(sessions, hasSize(1));

        initiatedSession = sessions.get(0);
        assertTrue(initiatedSession.isConnected());
        assertEquals(connectionId, initiatedSession.connectionId());
        assertEquals(sessionId, initiatedSession.id());
    }

    @Test
    public void librariesShouldNotBeAbleToAcquireSessionsThatDontExist()
    {
        // TODO
    }

    private SessionReplyStatus releaseInitiatedSession()
    {
        return initiatingLibrary.releaseToGateway(initiatedSession, ADMIN_IDLE_STRATEGY);
    }
}
