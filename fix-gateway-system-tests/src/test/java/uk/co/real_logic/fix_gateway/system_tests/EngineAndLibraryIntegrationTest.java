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
import uk.co.real_logic.aeron.Aeron;
import uk.co.real_logic.aeron.driver.MediaDriver;
import uk.co.real_logic.agrona.concurrent.YieldingIdleStrategy;
import uk.co.real_logic.fix_gateway.EngineConfiguration;
import uk.co.real_logic.fix_gateway.engine.FixEngine;
import uk.co.real_logic.fix_gateway.engine.LibraryInfo;
import uk.co.real_logic.fix_gateway.library.FixLibrary;
import uk.co.real_logic.fix_gateway.library.LibraryConfiguration;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static uk.co.real_logic.aeron.driver.ThreadingMode.SHARED;
import static uk.co.real_logic.agrona.CloseHelper.quietClose;
import static uk.co.real_logic.fix_gateway.TestFixtures.unusedPort;
import static uk.co.real_logic.fix_gateway.system_tests.SystemTestUtil.*;

// TODO: figure out how to configure timing and detection in a sensible way
public class EngineAndLibraryIntegrationTest
{

    private static final long TIMEOUT = TimeUnit.MILLISECONDS.toNanos(10);

    private int aeronPort = unusedPort();
    private MediaDriver mediaDriver;
    private FixEngine engine;
    private FixLibrary library;

    private FakeOtfAcceptor otfAcceptor = new FakeOtfAcceptor();
    private FakeSessionHandler sessionHandler = new FakeSessionHandler(otfAcceptor);

    @Before
    public void launch()
    {
        final MediaDriver.Context context = new MediaDriver.Context()
            .threadingMode(SHARED)
            .dirsDeleteOnStart(true)
            .imageLivenessTimeoutNs(TIMEOUT)
            .clientLivenessTimeoutNs(TIMEOUT)
            .sharedIdleStrategy(new YieldingIdleStrategy());

        mediaDriver = MediaDriver.launch(context);

        delete(ACCEPTOR_LOGS);
        final EngineConfiguration config = acceptingConfig(unusedPort(), aeronPort, "engineCounters");
        setupAeronClient(config.aeronContext());
        engine = FixEngine.launch(config);
    }

    @Test
    public void engineInitiallyHasNoConnectedLibraries()
    {
        assertNoActiveLibraries();
    }

    @Test
    public void engineDetectsLibraryConnect()
    {
        connectLibrary();

        final List<LibraryInfo> libraries = engine.libraries();
        assertThat(libraries, hasSize(1));
        assertTrue("Is not acceptor", libraries.get(0).isAcceptor());
    }

    @Ignore
    @Test
    public void engineDetectsLibraryDisconnect()
    {
        connectLibrary();
        library.close();

        System.out.println("before park" + System.currentTimeMillis());
        // should be 3 * TIMEOUT
        LockSupport.parkNanos(15 * TIMEOUT);

        assertNoActiveLibraries();
    }

    @Test
    public void libraryDetectsEngineDisconnect()
    {

    }

    private void assertNoActiveLibraries()
    {
        assertThat("libraries haven't disconnected yet", engine.libraries(), hasSize(0));
    }

    private void connectLibrary()
    {
        final LibraryConfiguration config = acceptingLibraryConfig(
            sessionHandler, ACCEPTOR_ID, INITIATOR_ID, aeronPort, "fix-acceptor");
        setupAeronClient(config.aeronContext());
        library = new FixLibrary(config);
    }

    private void setupAeronClient(Aeron.Context context)
    {
        context.keepAliveInterval(TIMEOUT / 4)
               .idleStrategy(new YieldingIdleStrategy());
    }

    @After
    public void close() throws Exception
    {
        quietClose(library);
        quietClose(engine);
        quietClose(mediaDriver);
    }
}
