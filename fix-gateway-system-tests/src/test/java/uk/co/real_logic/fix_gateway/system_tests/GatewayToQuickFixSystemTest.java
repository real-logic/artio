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
import quickfix.*;
import uk.co.real_logic.aeron.driver.MediaDriver;
import uk.co.real_logic.fix_gateway.FixGateway;
import uk.co.real_logic.fix_gateway.SessionConfiguration;
import uk.co.real_logic.fix_gateway.StaticConfiguration;
import uk.co.real_logic.fix_gateway.framer.session.InitiatorSession;

import static org.junit.Assert.assertTrue;
import static uk.co.real_logic.fix_gateway.TestFixtures.unusedPort;
import static uk.co.real_logic.fix_gateway.framer.session.SessionState.ACTIVE;
import static uk.co.real_logic.fix_gateway.system_tests.SystemTestUtil.launchMediaDriver;

@Ignore
public class GatewayToQuickFixSystemTest
{
    private MediaDriver mediaDriver;
    private FixGateway initiatingGateway;
    private InitiatorSession initiatedSession;

    private FakeOtfAcceptor initiatingOtfAcceptor = new FakeOtfAcceptor();
    private FakeSessionHandler initiatingSessionHandler = new FakeSessionHandler(initiatingOtfAcceptor);

    private SocketAcceptor socketAcceptor;
    private FakeQuickFixApplication acceptor = new FakeQuickFixApplication();

    @Before
    public void launch() throws ConfigError
    {
        final int port = unusedPort();

        mediaDriver = launchMediaDriver();

        // "udp://localhost:" + unusedPort()

        final SessionSettings settings = new SessionSettings();
        final FileStoreFactory storeFactory = new FileStoreFactory(settings);
        final LogFactory logFactory = new ScreenLogFactory(settings);
        socketAcceptor = new SocketAcceptor(this.acceptor, storeFactory, settings, logFactory,
            new DefaultMessageFactory());

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
    }

    @Test
    public void sessionHasBeenInitiated() throws InterruptedException
    {
        assertTrue("Session has failed to connect", initiatedSession.isConnected());
        assertTrue("Session has failed to logon", initiatedSession.state() == ACTIVE);
    }

    @After
    public void close() throws Exception
    {
        if (socketAcceptor != null)
        {
            socketAcceptor.stop();
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
