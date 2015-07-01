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
import org.junit.Ignore;
import org.junit.Test;
import uk.co.real_logic.aeron.driver.MediaDriver;
import uk.co.real_logic.fix_gateway.builder.ResendRequestEncoder;
import uk.co.real_logic.fix_gateway.decoder.TestRequestDecoder;
import uk.co.real_logic.fix_gateway.engine.FixEngine;
import uk.co.real_logic.fix_gateway.library.FixLibrary;
import uk.co.real_logic.fix_gateway.session.Session;

import java.util.concurrent.locks.LockSupport;

import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.*;
import static uk.co.real_logic.agrona.CloseHelper.quietClose;
import static uk.co.real_logic.fix_gateway.TestFixtures.unusedPort;
import static uk.co.real_logic.fix_gateway.Timing.assertEventuallyEquals;
import static uk.co.real_logic.fix_gateway.Timing.assertEventuallyTrue;
import static uk.co.real_logic.fix_gateway.session.SessionState.ACTIVE;
import static uk.co.real_logic.fix_gateway.system_tests.SystemTestUtil.*;

public class GatewayToGatewaySystemTest
{

    private MediaDriver mediaDriver;
    private FixEngine acceptingGateway;
    private FixEngine initiatingGateway;
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
        initiatingGateway = launchInitiatingGateway(initiatingSessionHandler, initAeronPort);
        acceptingGateway = launchAcceptingGateway(port, acceptingSessionHandler, ACCEPTOR_ID, INITIATOR_ID, acceptAeronPort);

        initiatingLibrary = new FixLibrary(initiatingConfig(initiatingSessionHandler, initAeronPort));
        acceptingLibrary = new FixLibrary(acceptingConfig(port, acceptingSessionHandler, ACCEPTOR_ID, INITIATOR_ID,
            acceptAeronPort));

        initiatedSession = initiate(initiatingLibrary, port, INITIATOR_ID, ACCEPTOR_ID);
    }

    private Session acceptSession()
    {
        while (acceptingSessionHandler.session() == null)
        {
            acceptingLibrary.poll(1);
            LockSupport.parkNanos(10_000);
        }
        return acceptingSessionHandler.session();
    }

    @Test
    public void sessionHasBeenInitiated() throws InterruptedException
    {
        assertTrue("Session has failed to connect", initiatedSession.isConnected());
        assertEventuallyTrue("Session has failed to logon", () ->
        {
            initiatingLibrary.poll(1);
            acceptingLibrary.poll(1);
            assertEquals(ACTIVE, initiatedSession.state());
        });

        acceptingSession = acceptSession();
        assertNotNull("Accepting Session not been setup", acceptingSession);
    }

    @Ignore
    @Test
    public void messagesCanBeSentFromInitiatorToAcceptor()
    {
        sendTestRequest(initiatedSession);

        assertReceivedMessage(acceptingSessionHandler, acceptingOtfAcceptor);
    }

    @Ignore
    @Test
    public void messagesCanBeSentFromAcceptorToInitiator()
    {
        sendTestRequest(acceptingSession);

        assertReceivedMessage(initiatingSessionHandler, initiatingOtfAcceptor);
    }

    @Ignore
    @Test
    public void initiatorSessionCanBeDisconnected()
    {
        initiatedSession.startLogout();

        assertDisconnected(acceptingSessionHandler, initiatedSession);
    }

    @Ignore
    @Test
    public void acceptorSessionCanBeDisconnected()
    {
        acceptingSession.startLogout();

        assertDisconnected(initiatingSessionHandler, acceptingSession);
    }

    @Ignore
    @Test
    public void gatewayProcessesResendRequests()
    {
        messagesCanBeSentFromInitiatorToAcceptor();

        sendResendRequest();

        assertMessageResent();
    }

    private void assertMessageResent()
    {
        assertEventuallyEquals("Failed to receive the reply", 1, () -> acceptingLibrary.poll(1));
        assertThat(acceptingOtfAcceptor.messageTypes(), hasItem(TestRequestDecoder.MESSAGE_TYPE));
        assertEquals(INITIATOR_ID, acceptingOtfAcceptor.lastSenderCompId());
        assertNull("Detected Error", acceptingOtfAcceptor.lastError());
        assertTrue("Failed to complete parsing", acceptingOtfAcceptor.isCompleted());
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
        quietClose(initiatingLibrary);
        quietClose(acceptingLibrary);

        quietClose(initiatingGateway);
        quietClose(acceptingGateway);
        quietClose(mediaDriver);
    }

}
