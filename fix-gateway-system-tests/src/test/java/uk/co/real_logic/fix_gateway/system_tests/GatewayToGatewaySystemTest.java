/*
 * Copyright 2015 Real Logic Ltd.
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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.aeron.driver.MediaDriver;
import uk.co.real_logic.agrona.CloseHelper;
import uk.co.real_logic.fix_gateway.builder.ResendRequestEncoder;
import uk.co.real_logic.fix_gateway.engine.FixEngine;
import uk.co.real_logic.fix_gateway.engine.SessionInfo;
import uk.co.real_logic.fix_gateway.engine.framer.LibraryInfo;
import uk.co.real_logic.fix_gateway.library.FixLibrary;
import uk.co.real_logic.fix_gateway.library.session.Session;

import java.util.List;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static uk.co.real_logic.fix_gateway.TestFixtures.launchMediaDriver;
import static uk.co.real_logic.fix_gateway.TestFixtures.unusedPort;
import static uk.co.real_logic.fix_gateway.Timing.assertEventuallyTrue;
import static uk.co.real_logic.fix_gateway.system_tests.SystemTestUtil.*;

public class GatewayToGatewaySystemTest
{
    private int port = unusedPort();
    private int initAeronPort = unusedPort();

    private MediaDriver mediaDriver;
    private FixEngine acceptingEngine;
    private FixEngine initiatingEngine;
    private FixLibrary acceptingLibrary;
    private FixLibrary initiatingLibrary;
    private Session initiatedSession;
    private Session acceptingSession;

    private FakeOtfAcceptor acceptingOtfAcceptor = new FakeOtfAcceptor();
    private FakeSessionHandler acceptingSessionHandler = new FakeSessionHandler(acceptingOtfAcceptor);

    private FakeOtfAcceptor initiatingOtfAcceptor = new FakeOtfAcceptor();
    private FakeSessionHandler initiatingSessionHandler = new FakeSessionHandler(initiatingOtfAcceptor);

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
        final List<LibraryInfo> libraries = initiatingEngine.libraries();
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
        final FixLibrary library2 = newInitiatingLibrary(initAeronPort, initiatingSessionHandler2, 2);
        final Session session2 = initiate(library2, port, INITIATOR_ID2, ACCEPTOR_ID);

        assertConnected(session2);
        sessionLogsOn(library2, acceptingLibrary, session2);
        final Session acceptingSession2 = acceptSession(acceptingSessionHandler, acceptingLibrary);

        sendTestRequest(acceptingSession2);
        assertReceivedTestRequest(library2, acceptingLibrary, initiatingOtfAcceptor2);

        assertOriginalLibraryDoesntReceiveMessages(initiator1MessageCount);
    }

    private void assertOriginalLibraryDoesntReceiveMessages(final int initiator1MessageCount)
    {
        initiatingLibrary.poll(5);
        assertThat("Messages received by wrong initiator",
            initiatingOtfAcceptor.messages(), hasSize(initiator1MessageCount));
    }

    private void assertSessionsDisconnected()
    {
        assertSessionDisconnected(initiatingLibrary, acceptingLibrary, initiatedSession);
        assertSessionDisconnected(acceptingLibrary, initiatingLibrary, acceptingSession);

        assertEventuallyTrue("libraries receive disconnect messages", () ->
        {
            poll(initiatingLibrary, acceptingLibrary);
            assertNotSession(acceptingSessionHandler, acceptingSession);
            assertNotSession(initiatingSessionHandler, initiatedSession);
        });
    }

    private void assertNotSession(final FakeSessionHandler sessionHandler, final Session session)
    {
        assertThat(sessionHandler.sessions(), not(hasItem(session)));
    }

    private void connectSessions()
    {
        initiatedSession = initiate(initiatingLibrary, port, INITIATOR_ID, ACCEPTOR_ID);

        assertConnected(initiatedSession);
        sessionLogsOn(initiatingLibrary, acceptingLibrary, initiatedSession);
        acceptingSession = acceptSession(acceptingSessionHandler, acceptingLibrary);
    }

    private void assertMessageResent()
    {
        assertThat(acceptingOtfAcceptor.messages(), hasSize(0));
        assertEventuallyTrue("Failed to receive the reply", () ->
        {
            acceptingLibrary.poll(1);
            initiatingLibrary.poll(1);

            final String messageType = acceptingOtfAcceptor.lastMessage().getMessageType();
            assertEquals("1", messageType);
            assertEquals(INITIATOR_ID, acceptingOtfAcceptor.lastSenderCompId());
            assertNull("Detected Error", acceptingOtfAcceptor.lastError());
            assertTrue("Failed to complete parsing", acceptingOtfAcceptor.isCompleted());
        });
    }

    private void sendResendRequest()
    {
        final int seqNum = acceptingSession.lastReceivedMsgSeqNum();
        final ResendRequestEncoder resendRequest = new ResendRequestEncoder()
            .beginSeqNo(seqNum)
            .endSeqNo(seqNum);

        acceptingOtfAcceptor.messages().clear();

        acceptingSession.send(resendRequest);
    }

    @After
    public void close() throws Exception
    {
        CloseHelper.close(initiatingLibrary);
        CloseHelper.close(acceptingLibrary);

        CloseHelper.close(initiatingEngine);
        CloseHelper.close(acceptingEngine);

        CloseHelper.close(mediaDriver);
    }
}
