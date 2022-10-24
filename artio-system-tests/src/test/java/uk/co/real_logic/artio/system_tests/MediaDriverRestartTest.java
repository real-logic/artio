/*
 * Copyright 2015-2022 Real Logic Limited, Adaptive Financial Consulting Ltd.
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

import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.driver.MediaDriver;
import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.artio.CloseChecker;
import uk.co.real_logic.artio.TestFixtures;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.library.LibraryConfiguration;

import static java.util.Collections.singletonList;
import static uk.co.real_logic.artio.TestFixtures.IN_CI;
import static uk.co.real_logic.artio.TestFixtures.closeMediaDriver;
import static uk.co.real_logic.artio.TestFixtures.archiveContext;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.*;

public class MediaDriverRestartTest extends AbstractGatewayToGatewaySystemTest
{
    private static final int DRIVER_TIMEOUT_MS = IN_CI ? 10_000 : 1000;

    @Before
    public void launch()
    {
        start(true);
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
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

    private void start(final boolean dirDeleteOnStart)
    {
        final MediaDriver.Context context = TestFixtures.mediaDriverContext(
            TestFixtures.TERM_BUFFER_LENGTH, dirDeleteOnStart);
        context.driverTimeoutMs(DRIVER_TIMEOUT_MS);
        context.warnIfDirectoryExists(false);

        mediaDriver = ArchivingMediaDriver.launch(context, archiveContext());
        final String aeronDirectoryName = context.aeronDirectoryName();
        CloseChecker.onOpen(aeronDirectoryName, mediaDriver);

        final EngineConfiguration acceptingConfig = acceptingConfig(
            port, ACCEPTOR_ID, INITIATOR_ID, nanoClock);
        acceptingConfig.deleteLogFileDirOnStart(true);
        acceptingConfig.aeronContext().driverTimeoutMs(DRIVER_TIMEOUT_MS);

        acceptingEngine = FixEngine.launch(acceptingConfig);

        delete(CLIENT_LOGS);
        final EngineConfiguration initiatingConfig = initiatingConfig(libraryAeronPort, nanoClock);
        initiatingConfig.aeronContext().driverTimeoutMs(DRIVER_TIMEOUT_MS);
        initiatingEngine = FixEngine.launch(initiatingConfig);

        final LibraryConfiguration acceptingLibraryConfig = acceptingLibraryConfig(acceptingHandler, nanoClock);
        acceptingLibraryConfig.aeronContext().driverTimeoutMs(DRIVER_TIMEOUT_MS);
        acceptingLibrary = connect(acceptingLibraryConfig);

        final LibraryConfiguration configuration = new LibraryConfiguration()
            .sessionAcquireHandler(initiatingHandler)
            .sessionExistsHandler(initiatingHandler)
            .libraryAeronChannels(singletonList("aeron:udp?endpoint=localhost:" + libraryAeronPort));
        configuration.aeronContext().driverTimeoutMs(DRIVER_TIMEOUT_MS);

        initiatingLibrary = connect(configuration);
        testSystem = new TestSystem(acceptingLibrary, initiatingLibrary);

        connectSessions();
    }
}
