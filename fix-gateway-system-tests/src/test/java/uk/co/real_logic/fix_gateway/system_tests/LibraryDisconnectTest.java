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
import uk.co.real_logic.fix_gateway.engine.FixEngine;

import java.util.concurrent.locks.LockSupport;

import static uk.co.real_logic.fix_gateway.TestFixtures.launchMediaDriver;
import static uk.co.real_logic.fix_gateway.system_tests.SystemTestUtil.*;

public class LibraryDisconnectTest extends AbstractGatewayToGatewaySystemTest
{

    @Before
    public void launch()
    {
        mediaDriver = launchMediaDriver();

        delete(ACCEPTOR_LOGS);
        acceptingEngine = FixEngine.launch(
            acceptingConfig(port, "engineCounters", ACCEPTOR_ID, INITIATOR_ID)
              .replyTimeoutInMs(400));
        initiatingEngine = launchInitiatingGateway(initAeronPort);

        acceptingLibrary = newAcceptingLibrary(acceptingHandler);
        initiatingLibrary = newInitiatingLibrary(initAeronPort, initiatingHandler, 1);

        wireSessions();
    }

    @Test
    public void engineShouldManageConnectionsFromDisconnectedLibrary()
    {
        engineAcquiresAcceptingLibrariesSession();

        messagesCanBeExchanged(
            initiatingSession, initiatingLibrary, null, initiatingOtfAcceptor);
    }

    private void engineAcquiresAcceptingLibrariesSession()
    {
        while (acceptingEngine.gatewaySessions(ADMIN_IDLE_STRATEGY).isEmpty())
        {
            LockSupport.parkNanos(100);
        }
    }

}
