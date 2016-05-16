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
import uk.co.real_logic.fix_gateway.library.Reply;
import uk.co.real_logic.fix_gateway.library.SessionConfiguration;
import uk.co.real_logic.fix_gateway.session.Session;

import static org.junit.Assert.assertEquals;
import static uk.co.real_logic.fix_gateway.TestFixtures.launchMediaDriver;
import static uk.co.real_logic.fix_gateway.TestFixtures.unusedPort;
import static uk.co.real_logic.fix_gateway.system_tests.SystemTestUtil.*;

public class MultipleAddressSystemTest extends AbstractGatewayToGatewaySystemTest
{
    private static final int NONSENSE_PORT = 1000;

    @Before
    public void launch()
    {
        final int initAeronPort = unusedPort();

        mediaDriver = launchMediaDriver();
        initiatingEngine = launchInitiatingGateway(initAeronPort);
        delete(ACCEPTOR_LOGS);
        final EngineConfiguration config = acceptingConfig(port, "engineCounters", ACCEPTOR_ID, INITIATOR_ID);
        acceptingEngine = FixEngine.launch(config);

        initiatingLibrary = newInitiatingLibrary(initAeronPort, initiatingHandler, 1);
        acceptingLibrary = newAcceptingLibrary(acceptingHandler);
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

        final Reply<Session> reply = initiatingLibrary.initiate(config);
        awaitReply(initiatingLibrary, reply);

        final Session session = reply.resultIfPresent();
        assertConnected(session);
        assertEquals("localhost", session.connectedHost());
        assertEquals(port, session.connectedPort());
    }

}
