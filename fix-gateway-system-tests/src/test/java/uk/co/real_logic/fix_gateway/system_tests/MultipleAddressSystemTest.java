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

import io.aeron.driver.MediaDriver;
import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.fix_gateway.Reply;
import uk.co.real_logic.fix_gateway.TestFixtures;
import uk.co.real_logic.fix_gateway.engine.FixEngine;
import uk.co.real_logic.fix_gateway.engine.LowResourceEngineScheduler;
import uk.co.real_logic.fix_gateway.library.SessionConfiguration;
import uk.co.real_logic.fix_gateway.session.Session;

import static io.aeron.driver.ThreadingMode.INVOKER;
import static org.junit.Assert.assertEquals;
import static uk.co.real_logic.fix_gateway.TestFixtures.*;
import static uk.co.real_logic.fix_gateway.system_tests.SystemTestUtil.*;

public class MultipleAddressSystemTest extends AbstractGatewayToGatewaySystemTest
{
    private static final int NONSENSE_PORT = 1000;

    @Before
    public void launch()
    {
        final int libraryAeronPort = unusedPort();

        final MediaDriver.Context context = mediaDriverContext(TestFixtures.TERM_BUFFER_LENGTH, true);
        context.threadingMode(INVOKER);
        mediaDriver = launchMediaDriver(context);

        delete(ACCEPTOR_LOGS);
        acceptingEngine = FixEngine.launch(
            acceptingConfig(port, ACCEPTOR_ID, INITIATOR_ID)
                .scheduler(new LowResourceEngineScheduler(context.driverAgentInvoker())));

        initiatingEngine = launchInitiatingEngine(libraryAeronPort);

        initiatingLibrary = newInitiatingLibrary(libraryAeronPort, initiatingHandler);
        testSystem = new TestSystem(initiatingLibrary);
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

        final Reply<Session> reply = testSystem.awaitReply(initiatingLibrary.initiate(config));

        final Session session = reply.resultIfPresent();
        assertConnected(session);
        assertEquals("localhost", session.connectedHost());
        assertEquals(port, session.connectedPort());
    }
}
