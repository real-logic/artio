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
import uk.co.real_logic.fix_gateway.SessionConfiguration;
import uk.co.real_logic.fix_gateway.StaticConfiguration;
import uk.co.real_logic.fix_gateway.admin.CompIdAuthenticationStrategy;
import uk.co.real_logic.fix_gateway.builder.TestRequestEncoder;
import uk.co.real_logic.fix_gateway.decoder.TestRequestDecoder;
import uk.co.real_logic.fix_gateway.framer.session.InitiatorSession;
import uk.co.real_logic.fix_gateway.framer.session.Session;
import uk.co.real_logic.fix_gateway.replication.GatewaySubscription;

import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.*;
import static uk.co.real_logic.fix_gateway.TestFixtures.unusedPort;
import static uk.co.real_logic.fix_gateway.Timing.assertEventuallyEquals;
import static uk.co.real_logic.fix_gateway.Timing.assertEventuallyTrue;
import static uk.co.real_logic.fix_gateway.framer.session.SessionState.ACTIVE;
import static uk.co.real_logic.fix_gateway.system_tests.SystemTestUtil.launchMediaDriver;

public class GatewayToGatewaySystemTest
{
    public static final long CONNECTION_ID = 0L;

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

        final StaticConfiguration acceptingConfig = new StaticConfiguration()
                .bind("localhost", port)
                .aeronChannel("udp://localhost:" + unusedPort())
                .authenticationStrategy(new CompIdAuthenticationStrategy("CCG"))
                .newSessionHandler(acceptingSessionHandler);
        acceptingGateway = FixGateway.launch(acceptingConfig);

        final StaticConfiguration initiatingConfig = new StaticConfiguration()
                .bind("localhost", unusedPort())
                .aeronChannel("udp://localhost:" + unusedPort())
                .newSessionHandler(initiatingSessionHandler);
        initiatingGateway = FixGateway.launch(initiatingConfig);

        final SessionConfiguration config = SessionConfiguration.builder()
                .address("localhost", port)
                .credentials("bob", "Uv1aegoh")
                .senderCompId("LEH_LZJ02")
                .targetCompId("CCG")
                .build();
        initiatedSession = initiatingGateway.initiate(config, null);
        acceptingSession = acceptingSessionHandler.session();
    }

    @Test
    public void sessionHasBeenInitiated() throws InterruptedException
    {
        assertTrue("Session has failed to connect", initiatedSession.isConnected());
        assertTrue("Session has failed to logon", initiatedSession.state() == ACTIVE);

        assertNotNull("Accepting Session not been setup", acceptingSession);
        assertNotNull("Accepting Session not been passed a subscription", acceptingSessionHandler.subscription());
    }

    @Test
    public void messagesCanBeSentFromInitiatorToAcceptor() throws InterruptedException
    {
        sendTestRequest(initiatedSession);

        assertReceivedMessage(acceptingSessionHandler.subscription(), acceptingOtfAcceptor);
    }

    @Test
    public void messagesCanBeSentFromAcceptorToInitiator() throws InterruptedException
    {
        sendTestRequest(acceptingSession);

        assertReceivedMessage(initiatingSessionHandler.subscription(), initiatingOtfAcceptor);
    }

    @Test
    public void initiatorSessionCanBeDisconnected() throws InterruptedException
    {
        initiatedSession.disconnect();

        assertDisconnected(acceptingSessionHandler, initiatedSession);
    }

    @Test
    public void acceptorSessionCanBeDisconnected() throws InterruptedException
    {
        acceptingSession.disconnect();

        assertDisconnected(initiatingSessionHandler, acceptingSession);
    }

    private void assertDisconnected(
        final FakeSessionHandler sessionHandler, final Session session) throws InterruptedException
    {
        assertFalse("Session is still connected", session.isConnected());

        assertEventuallyTrue("Failed to disconnect",
            () ->
            {
                sessionHandler.subscription().poll(1);
                assertEquals(CONNECTION_ID, sessionHandler.connectionId());
            });
    }

    private void sendTestRequest(final Session session)
    {
        final TestRequestEncoder testRequest = new TestRequestEncoder();
        testRequest.testReqID("hi");

        session.send(testRequest);
    }

    private void assertReceivedMessage(
        final GatewaySubscription subscription, final FakeOtfAcceptor acceptor) throws InterruptedException
    {
        assertEventuallyEquals("Failed to receive a message", 2, () -> subscription.poll(2));
        assertEquals(2, acceptor.messageTypes().size());
        assertThat(acceptor.messageTypes(), hasItem(TestRequestDecoder.MESSAGE_TYPE));
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
