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
import uk.co.real_logic.agrona.concurrent.SleepingIdleStrategy;
import uk.co.real_logic.fix_gateway.engine.FixEngine;
import uk.co.real_logic.fix_gateway.library.FixLibrary;
import uk.co.real_logic.fix_gateway.library.SessionConfiguration;
import uk.co.real_logic.fix_gateway.library.session.Session;

import static org.junit.Assert.assertEquals;
import static uk.co.real_logic.agrona.CloseHelper.quietClose;
import static uk.co.real_logic.fix_gateway.TestFixtures.launchMediaDriver;
import static uk.co.real_logic.fix_gateway.TestFixtures.unusedPort;
import static uk.co.real_logic.fix_gateway.system_tests.SystemTestUtil.*;

public class MultipleAddressSystemTest
{
    private static final int NONSENSE_PORT = 1000;

    private int port = unusedPort();
    private MediaDriver mediaDriver;
    private FixEngine acceptingEngine;
    private FixEngine initiatingEngine;
    private FixLibrary acceptingLibrary;
    private FixLibrary initiatingLibrary;
    private Session initiatedSession;

    private FakeOtfAcceptor acceptingOtfAcceptor = new FakeOtfAcceptor();
    private FakeSessionHandler acceptingSessionHandler = new FakeSessionHandler(acceptingOtfAcceptor);

    private FakeOtfAcceptor initiatingOtfAcceptor = new FakeOtfAcceptor();
    private FakeSessionHandler initiatingSessionHandler = new FakeSessionHandler(initiatingOtfAcceptor);

    @Before
    public void launch()
    {
        final int initAeronPort = unusedPort();
        final int acceptAeronPort = unusedPort();

        mediaDriver = launchMediaDriver();
        initiatingEngine = launchInitiatingGateway(initAeronPort);
        acceptingEngine = launchAcceptingGateway(port);

        initiatingLibrary = newInitiatingLibrary(initAeronPort, initiatingSessionHandler, 1);
        acceptingLibrary = newAcceptingLibrary(acceptingSessionHandler);
    }

    @Test
    public void shouldConnectToValidAddressIfMultipleGiven()
    {
        final SessionConfiguration config = SessionConfiguration.builder()
            .address("localhost", NONSENSE_PORT)
            .address("localhost", port)
            .address("localhost", NONSENSE_PORT)
            .credentials("bob", "Uv1aegoh")
            .senderCompId(INITIATOR_ID)
            .targetCompId(ACCEPTOR_ID)
            .build();

        initiatedSession = initiatingLibrary.initiate(config, new SleepingIdleStrategy(10));

        assertConnected(initiatedSession);
        assertEquals("localhost", initiatedSession.connectedHost());
        assertEquals(port, initiatedSession.connectedPort());
    }

    @After
    public void close() throws Exception
    {
        quietClose(initiatingLibrary);
        quietClose(acceptingLibrary);

        quietClose(initiatingEngine);
        quietClose(acceptingEngine);
        quietClose(mediaDriver);
    }
}
