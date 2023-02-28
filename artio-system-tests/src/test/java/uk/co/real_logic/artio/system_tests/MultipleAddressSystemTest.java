/*
 * Copyright 2015-2023 Real Logic Limited, Adaptive Financial Consulting Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.co.real_logic.artio.system_tests;

import io.aeron.driver.MediaDriver;
import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.artio.MonitoringAgentFactory;
import uk.co.real_logic.artio.Reply;
import uk.co.real_logic.artio.TestFixtures;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.engine.LowResourceEngineScheduler;
import uk.co.real_logic.artio.library.SessionConfiguration;
import uk.co.real_logic.artio.session.Session;

import static org.junit.Assert.assertEquals;
import static uk.co.real_logic.artio.TestFixtures.*;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.*;

public class MultipleAddressSystemTest extends AbstractGatewayToGatewaySystemTest
{
    private static final int NONSENSE_PORT = 1000;

    @Before
    public void launch()
    {
        final int libraryAeronPort = unusedPort();

        final MediaDriver.Context context = mediaDriverContext(TestFixtures.TERM_BUFFER_LENGTH, true);
        mediaDriver = launchMediaDriver(context);

        acceptingEngine = FixEngine.launch(
            acceptingConfig(port, ACCEPTOR_ID, INITIATOR_ID, nanoClock)
                .scheduler(new LowResourceEngineScheduler())
                .deleteLogFileDirOnStart(true));

        final EngineConfiguration initiatingConfig = initiatingConfig(libraryAeronPort, nanoClock);
        initiatingConfig.deleteLogFileDirOnStart(true);
        initiatingConfig.monitoringAgentFactory(MonitoringAgentFactory.none());
        initiatingEngine = FixEngine.launch(initiatingConfig);

        initiatingLibrary = newInitiatingLibrary(libraryAeronPort, initiatingHandler, nanoClock);
        testSystem = new TestSystem(initiatingLibrary);
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
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
