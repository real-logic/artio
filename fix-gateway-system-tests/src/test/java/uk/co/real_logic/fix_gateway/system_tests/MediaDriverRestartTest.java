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
import uk.co.real_logic.fix_gateway.CloseChecker;
import uk.co.real_logic.fix_gateway.TestFixtures;
import uk.co.real_logic.fix_gateway.engine.EngineConfiguration;
import uk.co.real_logic.fix_gateway.engine.FixEngine;
import uk.co.real_logic.fix_gateway.library.LibraryConfiguration;

import static java.util.Collections.singletonList;
import static uk.co.real_logic.fix_gateway.TestFixtures.closeMediaDriver;
import static uk.co.real_logic.fix_gateway.system_tests.SystemTestUtil.*;

public class MediaDriverRestartTest extends AbstractGatewayToGatewaySystemTest
{

    private static final int DRIVER_TIMEOUT_MS = 1000;

    @Before
    public void launch()
    {
        delete(ACCEPTOR_LOGS);

        start(true);
    }

    @Test
    public void shouldSurviveCompleteRestart() throws InterruptedException
    {
        acceptingLibrary.close();
        initiatingLibrary.close();
        acceptingEngine.close();
        initiatingEngine.close();
        closeMediaDriver(mediaDriver);

        Thread.sleep(DRIVER_TIMEOUT_MS);

        start(false);

        messagesCanBeExchanged();
    }

    private void start(final boolean dirsDeleteOnStart)
    {
        final MediaDriver.Context context = TestFixtures.mediaDriverContext(
            TestFixtures.TERM_BUFFER_LENGTH, dirsDeleteOnStart);
        context.driverTimeoutMs(DRIVER_TIMEOUT_MS);
        context.warnIfDirectoriesExist(false);

        mediaDriver = MediaDriver.launch(context);
        final String aeronDirectoryName = context.aeronDirectoryName();
        CloseChecker.onOpen(aeronDirectoryName, mediaDriver);

        final EngineConfiguration acceptingConfig = acceptingConfig(
            port, "engineCounters", ACCEPTOR_ID, INITIATOR_ID);
        acceptingConfig.aeronContext().driverTimeoutMs(DRIVER_TIMEOUT_MS);

        acceptingEngine = FixEngine.launch(acceptingConfig);

        delete(CLIENT_LOGS);
        final EngineConfiguration initiatingConfig = initiatingConfig(libraryAeronPort, "engineCounters");
        initiatingConfig.aeronContext().driverTimeoutMs(DRIVER_TIMEOUT_MS);
        initiatingEngine = FixEngine.launch(initiatingConfig);

        final LibraryConfiguration acceptingLibraryConfig = acceptingLibraryConfig(acceptingHandler);
        acceptingLibraryConfig.aeronContext().driverTimeoutMs(DRIVER_TIMEOUT_MS);
        acceptingLibrary = connect(acceptingLibraryConfig);

        final LibraryConfiguration configuration = new LibraryConfiguration()
            .sessionAcquireHandler(initiatingHandler)
            .sentPositionHandler(initiatingHandler)
            .sessionExistsHandler(initiatingHandler)
            .libraryAeronChannels(singletonList("aeron:udp?endpoint=localhost:" + libraryAeronPort));
        configuration.aeronContext().driverTimeoutMs(DRIVER_TIMEOUT_MS);

        initiatingLibrary = connect(configuration);
        testSystem = new TestSystem(acceptingLibrary, initiatingLibrary);

        connectSessions();
    }

}
