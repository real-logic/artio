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
import uk.co.real_logic.fix_gateway.FixGateway;
import uk.co.real_logic.fix_gateway.session.InitiatorSession;
import uk.co.real_logic.fix_gateway.session.Session;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static uk.co.real_logic.fix_gateway.TestFixtures.unusedPort;
import static uk.co.real_logic.fix_gateway.session.SessionState.ACTIVE;
import static uk.co.real_logic.fix_gateway.system_tests.SystemTestUtil.*;

public class GatewayToGatewaySystemTest
{

    private MediaDriver mediaDriver;
    private FixGateway acceptingGateway;
    private FixGateway initiatingGateway;
    private InitiatorSession initiatedSession;
    private Session acceptingSession;

    private FakeOtfAcceptor acceptingOtfAcceptor = new FakeOtfAcceptor();
    private FakeSessionHandler acceptingSessionHandler = new FakeSessionHandler(acceptingOtfAcceptor);

    private FakeOtfAcceptor initiatingOtfAcceptor = new FakeOtfAcceptor();
    private FakeSessionHandler initiatingSessionHandler = new FakeSessionHandler(initiatingOtfAcceptor);

    @Before
    public void launch()
    {
        final int port = unusedPort();
        mediaDriver = launchMediaDriver();
        acceptingGateway = launchAcceptingGateway(port, acceptingSessionHandler);
        initiatingGateway = launchInitiatingGateway(initiatingSessionHandler);
        initiatedSession = initiate(initiatingGateway, port);
        acceptingSession = acceptingSessionHandler.session();
    }

    @Test(timeout = 2000L)
    public void sessionHasBeenInitiated() throws InterruptedException
    {
        assertTrue("Session has failed to connect", initiatedSession.isConnected());
        assertTrue("Session has failed to logon", initiatedSession.state() == ACTIVE);

        assertNotNull("Accepting Session not been setup", acceptingSession);
        assertNotNull("Accepting Session not been passed a subscription", acceptingSessionHandler.subscription());
    }

    @Test(timeout = 2000L)
    public void messagesCanBeSentFromInitiatorToAcceptor() throws InterruptedException
    {
        sendTestRequest(initiatedSession);

        assertReceivedMessage(acceptingSessionHandler.subscription(), acceptingOtfAcceptor);
    }

    @Test(timeout = 2000L)
    public void messagesCanBeSentFromAcceptorToInitiator() throws InterruptedException
    {
        sendTestRequest(acceptingSession);

        assertReceivedMessage(initiatingSessionHandler.subscription(), initiatingOtfAcceptor);
    }

    @Test(timeout = 2000L)
    public void initiatorSessionCanBeDisconnected() throws InterruptedException
    {
        initiatedSession.startLogout();

        assertDisconnected(acceptingSessionHandler, initiatedSession);
    }

    @Test(timeout = 2000L)
    public void acceptorSessionCanBeDisconnected() throws InterruptedException
    {
        acceptingSession.startLogout();

        assertDisconnected(initiatingSessionHandler, acceptingSession);
    }

    @After
    public void close() throws Exception
    {
        if (acceptingGateway != null)
        {
            acceptingGateway.close();
        }

        if (initiatingGateway != null)
        {
            initiatingGateway.close();
        }

        if (mediaDriver != null)
        {
            mediaDriver.close();
        }
    }

}
