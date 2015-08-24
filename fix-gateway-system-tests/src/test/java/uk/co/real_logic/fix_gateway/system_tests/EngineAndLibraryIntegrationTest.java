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
        assertNoActiveLibraries(0);
    }

    @Test
    public void engineDetectsLibraryConnect()
    {
        awaitLibraryConnect(connectLibrary());

        final List<Library> libraries = hasLibraries(1);
        assertLibrary(libraries.get(0), true, DEFAULT_LIBRARY_ID);
    }

    @Test
    public void engineDetectsLibraryDisconnect()
    {
        final FixLibrary library = connectLibrary();
        awaitLibraryConnect(library);

        library.close();

        assertLibrariesDisconnect(0, null);
    }

    @Test
    public void engineDetectsMultipleLibraryInstances()
    {
        awaitLibraryConnect(connectLibrary(2));

        awaitLibraryConnect(connectLibrary(3));

        final List<Library> libraries = hasLibraries(2);

        assertLibrary2(libraries);
        assertLibrary(libraries.get(1), true, 2);
    }

    @Test
    public void engineDetectsDisconnectOfSpecificLibraryInstances()
    {
        setupTwoLibrariesAndCloseTheFirst();
    }

    private FixLibrary setupTwoLibrariesAndCloseTheFirst()
    {
        final FixLibrary library1 = connectLibrary(2);
        awaitLibraryConnect(library1);

        final FixLibrary library2 = connectLibrary(3);
        awaitLibraryConnect(library2);

        library1.close();

        assertLibrariesDisconnect(1, library2);

        final List<Library> libraries = hasLibraries(1);
        assertLibrary2(libraries);

        return library2;
    }

    @Test
    public void engineMakesNewLibraryAcceptorLibrary()
    {
        setupTwoLibrariesAndCloseTheFirst();

        awaitLibraryConnect(connectLibrary(4));

        final List<Library> libraries = hasLibraries(2);
        assertLibrary2(libraries);
        assertLibrary(libraries.get(1), true, 4);
    }

    @Test
    public void libraryDetectsEngine()
    {
        awaitLibraryConnect(connectLibrary());
    }

    @Test
    public void libraryDetectsEngineDisconnect()
    {
        final FixLibrary library = connectLibrary(DEFAULT_LIBRARY_ID);

        awaitLibraryConnect(library);

        CloseHelper.close(engine);

        assertEventuallyTrue(
            "Engine still hasn't disconnected", () ->
            {
                library.poll(5);
                final boolean notConnected = !library.isConnected();
                return notConnected;
            },
            AWAIT_TIMEOUT,
            1);
    }

    private void assertLibrary2(final List<Library> libraries)
    {
        assertLibrary(libraries.get(0), false, 3);
    }

    private void assertLibrariesDisconnect(final int count, final FixLibrary library)
    {
        assertEventuallyTrue(
            "libraries haven't disconnected yet",
            () -> {
                if (library != null)
                {
                    library.poll(1);
                }
                return engine.libraries().size() == count;
            },
            AWAIT_TIMEOUT,
            1);
    }

    private void assertLibrary(final Library library, final boolean expectedAcceptor, final int libraryId)
    {
        assertEquals(expectedAcceptor, library.isAcceptor());
        assertEquals("Has the wrong id", libraryId, library.libraryId());
    }

    private List<Library> hasLibraries(final int count)
    {
        final List<Library> libraries = engine.libraries();
        assertThat(libraries, hasSize(count));
        return libraries;
    }

    private void awaitLibraryConnect(final FixLibrary library)
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

    private void assertNoActiveLibraries(final int count)
    {
        assertThat("libraries haven't disconnected yet", engine.libraries(), hasSize(count));
    }

    private FixLibrary connectLibrary()
    {
        return connectLibrary(DEFAULT_LIBRARY_ID);
    }

    private FixLibrary connectLibrary(final int libraryId)
    {
        final LibraryConfiguration config =
            acceptingLibraryConfig(sessionHandler, ACCEPTOR_ID, INITIATOR_ID, aeronPort, "fix-acceptor")
                .libraryId(libraryId)
                .replyTimeoutInMs(TIMEOUT_IN_MS);

        return new FixLibrary(config);
    }

    @After
    public void close() throws Exception
    {
        CloseHelper.close(engine);
        CloseHelper.close(mediaDriver);
    }
}
