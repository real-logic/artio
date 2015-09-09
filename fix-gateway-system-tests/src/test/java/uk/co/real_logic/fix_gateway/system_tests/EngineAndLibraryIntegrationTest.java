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
import uk.co.real_logic.fix_gateway.engine.framer.LibraryInfo;
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
    private MediaDriver mediaDriver;
    private FixEngine engine;
    private FixLibrary library;
    private FixLibrary library2;

    private FakeOtfAcceptor otfAcceptor = new FakeOtfAcceptor();
    private FakeSessionHandler sessionHandler = new FakeSessionHandler(otfAcceptor);

    @Before
    public void launch()
    {
        delete(ACCEPTOR_LOGS);
        mediaDriver = launchMediaDriver();

        final EngineConfiguration config = acceptingConfig(unusedPort(), "engineCounters");
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
        library = connectLibrary();

        awaitLibraryConnect(library);

        final List<LibraryInfo> libraries = hasLibraries(1);
        assertLibrary(libraries.get(0), true, DEFAULT_LIBRARY_ID);
    }

    @Test
    public void engineDetectsLibraryDisconnect()
    {
        library = connectLibrary();
        awaitLibraryConnect(library);

        library.close();

        assertLibrariesDisconnect(0, null, engine);
    }

    @Test
    public void engineDetectsMultipleLibraryInstances()
    {
        library = connectLibrary(2);
        awaitLibraryConnect(library);

        library2 = connectLibrary(3);
        awaitLibraryConnect(library2);

        final List<LibraryInfo> libraries = hasLibraries(2);

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
        library = connectLibrary(2);
        awaitLibraryConnect(library);

        library2 = connectLibrary(3);
        awaitLibraryConnect(library2);

        library.close();

        assertLibrariesDisconnect(1, library2, engine);

        final List<LibraryInfo> libraries = hasLibraries(1);
        assertLibrary2(libraries);

        return library2;
    }

    @Test
    public void engineMakesNewLibraryAcceptorLibrary()
    {
        setupTwoLibrariesAndCloseTheFirst();

        library = connectLibrary(4);

        awaitLibraryConnect(library);

        final List<LibraryInfo> libraries = hasLibraries(2);
        assertLibrary2(libraries);
        assertLibrary(libraries.get(1), true, 4);
    }

    @Test
    public void libraryDetectsEngine()
    {
        library = connectLibrary();

        awaitLibraryConnect(library);
    }

    @Test
    public void libraryDetectsEngineDisconnect()
    {
        library = connectLibrary(DEFAULT_LIBRARY_ID);

        awaitLibraryConnect(library);

        CloseHelper.close(engine);

        assertEventuallyTrue(
            "Engine still hasn't disconnected", () ->
            {
                library.poll(5);
                return !library.isConnected();
            },
            AWAIT_TIMEOUT,
            1);
    }

    private void assertLibrary2(final List<LibraryInfo> libraries)
    {
        assertLibrary(libraries.get(0), false, 3);
    }

    private void assertLibrary(final LibraryInfo library, final boolean expectedAcceptor, final int libraryId)
    {
        assertEquals(expectedAcceptor, library.isAcceptor());
        assertEquals("Has the wrong id", libraryId, library.libraryId());
    }

    private List<LibraryInfo> hasLibraries(final int count)
    {
        final List<LibraryInfo> libraries = engine.libraries();
        assertThat(libraries, hasSize(count));
        return libraries;
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
            acceptingLibraryConfig(sessionHandler, ACCEPTOR_ID, INITIATOR_ID, "fix-acceptor-" + libraryId)
                .libraryId(libraryId)
                .replyTimeoutInMs(TIMEOUT_IN_MS);

        return new FixLibrary(config);
    }

    @After
    public void close() throws Exception
    {
        CloseHelper.close(library);
        CloseHelper.close(library2);
        CloseHelper.close(engine);
        CloseHelper.close(mediaDriver);
    }
}
