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
import uk.co.real_logic.fix_gateway.library.FixLibrary;

import java.util.concurrent.locks.LockSupport;

import static io.aeron.CommonContext.IPC_CHANNEL;
import static org.junit.Assert.assertEquals;
import static uk.co.real_logic.fix_gateway.TestFixtures.launchMediaDriver;
import static uk.co.real_logic.fix_gateway.session.Session.LIBRARY_DISCONNECTED;
import static uk.co.real_logic.fix_gateway.system_tests.SystemTestUtil.*;

public class LibraryDisconnectTest extends AbstractGatewayToGatewaySystemTest
{

    private static final int REPLY_TIMEOUT_IN_MS = 1000;

    @Before
    public void launch()
    {
        mediaDriver = launchMediaDriver();

        delete(ACCEPTOR_LOGS);
        acceptingEngine = FixEngine.launch(
            acceptingConfig(port, "engineCounters", ACCEPTOR_ID, INITIATOR_ID)
              .replyTimeoutInMs(REPLY_TIMEOUT_IN_MS));
        initiatingEngine = launchInitiatingGateway(libraryAeronPort);

        acceptingLibrary = FixLibrary.connect(
            acceptingLibraryConfig(acceptingHandler, ACCEPTOR_ID, INITIATOR_ID, IPC_CHANNEL)
            .replyTimeoutInMs(REPLY_TIMEOUT_IN_MS));
        initiatingLibrary = newInitiatingLibrary(libraryAeronPort, initiatingHandler);

        wireSessions();
    }

    @Test
    public void engineShouldManageConnectionsFromDisconnectedLibrary()
    {
        engineAcquiresAcceptingLibrariesSession();

        messagesCanBeExchanged(
            initiatingSession, initiatingLibrary, null, initiatingOtfAcceptor);

        while (acceptingLibrary.isConnected())
        {
            acceptingLibrary.poll(2);
        }

        assertEquals(LIBRARY_DISCONNECTED, acceptingSession.sendSequenceReset(1));
    }

    private void engineAcquiresAcceptingLibrariesSession()
    {
        while (acceptingEngine.gatewaySessions(ADMIN_IDLE_STRATEGY).isEmpty())
        {
            LockSupport.parkNanos(100);
        }
    }

}
