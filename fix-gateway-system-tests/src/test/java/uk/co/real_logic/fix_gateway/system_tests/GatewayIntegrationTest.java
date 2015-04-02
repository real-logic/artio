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
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.fix_gateway.FixGateway;
import uk.co.real_logic.fix_gateway.SessionConfiguration;
import uk.co.real_logic.fix_gateway.StaticConfiguration;
import uk.co.real_logic.fix_gateway.admin.CompIdAuthenticationStrategy;
import uk.co.real_logic.fix_gateway.admin.NewSessionHandler;
import uk.co.real_logic.fix_gateway.admin.SessionHandler;
import uk.co.real_logic.fix_gateway.builder.TestRequestEncoder;
import uk.co.real_logic.fix_gateway.decoder.TestRequestDecoder;
import uk.co.real_logic.fix_gateway.dictionary.IntDictionary;
import uk.co.real_logic.fix_gateway.framer.session.InitiatorSession;
import uk.co.real_logic.fix_gateway.framer.session.Session;
import uk.co.real_logic.fix_gateway.otf.OtfParser;
import uk.co.real_logic.fix_gateway.replication.GatewaySubscription;

import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.*;
import static uk.co.real_logic.aeron.driver.ThreadingMode.SHARED;
import static uk.co.real_logic.fix_gateway.TestFixtures.unusedPort;
import static uk.co.real_logic.fix_gateway.Timing.assertEventuallyEquals;
import static uk.co.real_logic.fix_gateway.Timing.assertEventuallyTrue;
import static uk.co.real_logic.fix_gateway.framer.session.SessionState.ACTIVE;

public class GatewayIntegrationTest
{

    public static final long SESSION_ID = 0L;

    private MediaDriver mediaDriver;
    private FixGateway acceptingGateway;
    private FixGateway initiatingGateway;
    private InitiatorSession session;
    private FakeOtfAcceptor fakeOtfAcceptor = new FakeOtfAcceptor();

    private SessionHandler fakeSessionHandler = new SessionHandler()
    {
        private final OtfParser parser = new OtfParser(fakeOtfAcceptor, new IntDictionary());

        public void onMessage(
            final DirectBuffer buffer,
            final int offset,
            final int length,
            final long connectionId,
            final long sessionId,
            final int messageType)
        {
            parser.onMessage(buffer, offset, length, sessionId, messageType);
        }

        public void onDisconnect(final long connectionId)
        {
            // TODO: USE!
        }
    };

    private FakeNewSessionHandler newSessionHandler = new FakeNewSessionHandler(fakeSessionHandler);

    @Before
    public void launch()
    {
        //System.setProperty("fix.core.debug", "true");
        final int port = unusedPort();

        mediaDriver = MediaDriver.launch(new MediaDriver.Context().threadingMode(SHARED));

        final StaticConfiguration acceptingConfig = new StaticConfiguration()
                .registerFallbackAcceptor(fakeOtfAcceptor)
                .bind("localhost", port)
                .aeronChannel("udp://localhost:" + unusedPort())
                .authenticationStrategy(new CompIdAuthenticationStrategy("CCG"))
                .newSessionHandler(newSessionHandler);
        acceptingGateway = FixGateway.launch(acceptingConfig);

        final StaticConfiguration initiatingConfig = new StaticConfiguration()
                .bind("localhost", unusedPort())
                .aeronChannel("udp://localhost:" + unusedPort())
                .newSessionHandler(new NewSessionHandler() {
                    public void onConnect(final Session session, final GatewaySubscription subscription)
                    {

                    }

                    public void onDisconnect(final Session session)
                    {

                    }
                });
        initiatingGateway = FixGateway.launch(initiatingConfig);

        final SessionConfiguration config = SessionConfiguration.builder()
                .address("localhost", port)
                .credentials("bob", "Uv1aegoh")
                .senderCompId("LEH_LZJ02")
                .targetCompId("CCG")
                .build();
        session = initiatingGateway.initiate(config, null);
    }

    @Test
    public void sessionHasBeenInitiated() throws InterruptedException
    {
        assertTrue("Session has failed to connect", session.isConnected());
        assertTrue("Session has failed to logon", session.state() == ACTIVE);
        assertNotNull("Subscription has not been passed to handler", newSessionHandler.subscription());
    }

    @Test
    public void messagesCanBeSentFromInitiatorToAcceptor() throws InterruptedException
    {
        final TestRequestEncoder testRequest = new TestRequestEncoder();
        testRequest.testReqID("hi");

        session.send(testRequest);

        final GatewaySubscription subscription = newSessionHandler.subscription();

        assertEventuallyEquals("Failed to receive a message", 2, () -> subscription.poll(2));
        assertEquals(2, fakeOtfAcceptor.messageTypes().size());
        assertThat(fakeOtfAcceptor.messageTypes(), hasItem(TestRequestDecoder.MESSAGE_TYPE));
    }

    @Test
    public void initiatorSessionCanBeDisconnected() throws InterruptedException
    {
        session.disconnect();

        assertFalse("Session is still connected", session.isConnected());

        assertEventuallyTrue("Failed to disconnect",
            () ->
            {
                assertEquals(SESSION_ID, newSessionHandler.disconnectedSession().id());
            });
    }

    // TODO: shutdown a gateway and check logout
    // TODO: initiate/accept multiple sessions

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
