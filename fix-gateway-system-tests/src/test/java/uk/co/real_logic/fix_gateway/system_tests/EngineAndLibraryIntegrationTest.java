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
import uk.co.real_logic.agrona.CloseHelper;
import uk.co.real_logic.fix_gateway.engine.EngineConfiguration;
import uk.co.real_logic.fix_gateway.engine.FixEngine;
import uk.co.real_logic.fix_gateway.engine.Library;
import uk.co.real_logic.fix_gateway.library.FixLibrary;
import uk.co.real_logic.fix_gateway.library.LibraryConfiguration;

import java.util.List;

import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static uk.co.real_logic.fix_gateway.TestFixtures.launchMediaDriver;
import static uk.co.real_logic.fix_gateway.TestFixtures.unusedPort;
import static uk.co.real_logic.fix_gateway.Timing.assertEventuallyTrue;
import static uk.co.real_logic.fix_gateway.library.LibraryConfiguration.DEFAULT_LIBRARY_ID;
import static uk.co.real_logic.fix_gateway.system_tests.SystemTestUtil.*;

public class EngineAndLibraryIntegrationTest
{
    private static final long TIMEOUT_IN_MS = 100;
    private static final long AWAIT_TIMEOUT = 50 * TIMEOUT_IN_MS;

    private int aeronPort = unusedPort();
    private MediaDriver mediaDriver;
    private FixEngine engine;
    private FixLibrary library;

    private FakeOtfAcceptor otfAcceptor = new FakeOtfAcceptor();
    private FakeSessionHandler sessionHandler = new FakeSessionHandler(otfAcceptor);

    @Before
    public void launch()
    {
        mediaDriver = launchMediaDriver();

        delete(ACCEPTOR_LOGS);
        final EngineConfiguration config = acceptingConfig(unusedPort(), aeronPort, "engineCounters");
        config.replyTimeoutInMs(TIMEOUT_IN_MS);
        engine = FixEngine.launch(config);
    }

    @Test
    public void engineInitiallyHasNoConnectedLibraries()
    {
        assertNoActiveLibraries();
    }

    @Test
    public void libraryDetectsEngine()
    {
        connectLibrary();

        awaitLibraryConnect();
    }

    @Test
    public void engineDetectsLibraryConnect()
    {
        connectLibrary();

        awaitLibraryConnect();

        final List<Library> libraries = engine.libraries();
        assertThat(libraries, hasSize(1));
        final Library library = libraries.get(0);
        assertTrue("Is not acceptor", library.isAcceptor());
        assertEquals("Has the wrong id", DEFAULT_LIBRARY_ID, library.libraryId());
    }

    @Test
    public void engineDetectsLibraryDisconnect()
    {
        connectLibrary();

        awaitLibraryConnect();

        library.close();

        assertEventuallyTrue(
            "libraries haven't disconnected yet",
            this::assertNoActiveLibraries,
            AWAIT_TIMEOUT);
    }

    @Test
    public void libraryDetectsEngineDisconnect()
    {
        connectLibrary();

        awaitLibraryConnect();

        CloseHelper.close(engine);

        assertEventuallyTrue(
            "Engine still hasn't disconnected", () ->
            {
                library.poll(5);
                final boolean notConnected = !library.isConnected();
                return notConnected;
            },
            AWAIT_TIMEOUT, 1);
    }

    private void awaitLibraryConnect()
    {
        assertEventuallyTrue(
            "Library hasn't seen Engine", () ->
            {
                library.poll(5);
                final boolean connected = library.isConnected();
                return connected;
            },
            AWAIT_TIMEOUT, 1);
    }

    private void assertNoActiveLibraries()
    {
        assertThat("libraries haven't disconnected yet", engine.libraries(), hasSize(0));
    }

    private void connectLibrary()
    {
        final LibraryConfiguration config = acceptingLibraryConfig(
            sessionHandler, ACCEPTOR_ID, INITIATOR_ID, aeronPort, "fix-acceptor");
        config.replyTimeoutInMs(TIMEOUT_IN_MS);
        library = new FixLibrary(config);
    }

    @After
    public void close() throws Exception
    {
        CloseHelper.close(library);
        CloseHelper.close(engine);
        CloseHelper.close(mediaDriver);
    }
}
