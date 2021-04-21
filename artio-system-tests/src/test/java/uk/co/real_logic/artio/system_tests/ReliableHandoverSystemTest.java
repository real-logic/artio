/*
 * Copyright 2021 Monotonic Ltd.
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

import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.library.LibraryConfiguration;

import static uk.co.real_logic.artio.TestFixtures.launchMediaDriver;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.*;

public class ReliableHandoverSystemTest extends AbstractGatewayToGatewaySystemTest
{
    @Before
    public void launch()
    {
        mediaDriver = launchMediaDriver();
        acceptingHandler.spamLogonMessages();

        final EngineConfiguration acceptingConfig = acceptingConfig(port, ACCEPTOR_ID, INITIATOR_ID, nanoClock)
            .deleteLogFileDirOnStart(true);
        acceptingEngine = FixEngine.launch(acceptingConfig);

        initiatingEngine = launchInitiatingEngine(libraryAeronPort, nanoClock);

        final LibraryConfiguration acceptingLibraryConfig = acceptingLibraryConfig(acceptingHandler, nanoClock);
        acceptingLibrary = connect(acceptingLibraryConfig);
        initiatingLibrary = newInitiatingLibrary(libraryAeronPort, initiatingHandler, nanoClock);
        testSystem = new TestSystem(acceptingLibrary, initiatingLibrary);
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void messagesCanBeSentFromInitiatorToAcceptor()
    {
        connectSessions();

        assertLastLogonEquals(1, 0);
        assertSequenceResetTimeAtLatestLogon(initiatingSession);

        messagesCanBeExchanged();

        assertInitiatingSequenceIndexIs(0);
    }
}
