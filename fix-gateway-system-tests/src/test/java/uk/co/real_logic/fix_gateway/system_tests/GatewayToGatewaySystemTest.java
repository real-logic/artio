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
import uk.co.real_logic.fix_gateway.builder.ResendRequestEncoder;
import uk.co.real_logic.fix_gateway.decoder.TestRequestDecoder;
import uk.co.real_logic.fix_gateway.engine.FixEngine;
import uk.co.real_logic.fix_gateway.library.FixLibrary;
import uk.co.real_logic.fix_gateway.library.session.Session;

import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.*;
import static uk.co.real_logic.fix_gateway.TestFixtures.unusedPort;
import static uk.co.real_logic.fix_gateway.Timing.assertEventuallyTrue;
import static uk.co.real_logic.fix_gateway.system_tests.SystemTestUtil.*;

public class GatewayToGatewaySystemTest
{

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
        final int port = unusedPort();
        final int initAeronPort = unusedPort();
        final int acceptAeronPort = unusedPort();

        mediaDriver = launchMediaDriver();
        initiatingEngine = launchInitiatingGateway(initiatingSessionHandler, initAeronPort);
        acceptingEngine = launchAcceptingGateway(port, acceptingSessionHandler, ACCEPTOR_ID, INITIATOR_ID, acceptAeronPort);

        initiatingLibrary = new FixLibrary(initiatingConfig(initiatingSessionHandler, initAeronPort, "libraryCounters"));
        acceptingLibrary = new FixLibrary(acceptingConfig(port, acceptingSessionHandler, ACCEPTOR_ID, INITIATOR_ID,
            acceptAeronPort, "libraryCounters"));

        initiatedSession = initiate(initiatingLibrary, port, INITIATOR_ID, ACCEPTOR_ID);

        assertTrue("Session has failed to connect", initiatedSession.isConnected());
        sessionLogsOn(initiatingLibrary, acceptingLibrary, initiatedSession);
        acceptingSession = acceptSession(acceptingSessionHandler, acceptingLibrary);
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

        assertReceivedMessage(initiatingLibrary, acceptingLibrary, acceptingOtfAcceptor);
    }

    @Test
    public void messagesCanBeSentFromAcceptorToInitiator()
    {
        sendTestRequest(acceptingSession);

        assertReceivedMessage(initiatingLibrary, acceptingLibrary, initiatingOtfAcceptor);
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

    private void assertSessionsDisconnected()
    {
        assertSessionDisconnected(initiatingLibrary, acceptingLibrary, initiatedSession);
        assertSessionDisconnected(initiatingLibrary, acceptingLibrary, acceptingSession);
    }

    @Test
    public void gatewayProcessesResendRequests()
    {
        messagesCanBeSentFromInitiatorToAcceptor();

        sendResendRequest();

        assertMessageResent();
    }

    private void assertMessageResent()
    {
        assertEventuallyTrue("Failed to receive the reply", () ->
        {
            acceptingLibrary.poll(1);
            initiatingLibrary.poll(1);

            assertThat(acceptingOtfAcceptor.messageTypes(), hasItem(TestRequestDecoder.MESSAGE_TYPE));
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

        acceptingOtfAcceptor.messageTypes().clear();

        acceptingSession.send(resendRequest);
    }

    @After
    public void close() throws Exception
    {
        closeIfOpen(initiatingLibrary);
        closeIfOpen(acceptingLibrary);

        closeIfOpen(initiatingEngine);
        closeIfOpen(acceptingEngine);
        closeIfOpen(mediaDriver);
    }

}
