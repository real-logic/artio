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
import uk.co.real_logic.aeron.driver.MediaDriver;
import uk.co.real_logic.fix_gateway.engine.FixEngine;
import uk.co.real_logic.fix_gateway.engine.LibraryInfo;
import uk.co.real_logic.fix_gateway.library.FixLibrary;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static uk.co.real_logic.agrona.CloseHelper.quietClose;
import static uk.co.real_logic.fix_gateway.TestFixtures.launchMediaDriver;
import static uk.co.real_logic.fix_gateway.TestFixtures.unusedPort;
import static uk.co.real_logic.fix_gateway.system_tests.SystemTestUtil.*;

// TODO: figure out how to configure timing and detection in a sensible way
@Ignore
public class EngineAndLibraryIntegrationTest
{

    static
    {
        /*System.setProperty(Configuration.IMAGE_LIVENESS_TIMEOUT_PROP_NAME,
            String.valueOf(TimeUnit.MILLISECONDS.toNanos(10)));*/
    }

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
        engine = launchAcceptingGateway(unusedPort(), aeronPort);
    }

    @Test
    public void engineInitialiallyHasNoConnectedLibraries()
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

    @Test
    public void engineDetectsLibraryDisconnect()
    {
        connectLibrary();
        library.close();

        LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1000));

        assertNoActiveLibraries();
    }

    @Test
    public void libraryDetectsEngineDisconnect()
    {

    }

    private void assertNoActiveLibraries()
    {
        assertThat(engine.libraries(), hasSize(0));
    }

    private void connectLibrary()
    {
        library = new FixLibrary(
            acceptingLibraryConfig(sessionHandler, ACCEPTOR_ID, INITIATOR_ID, aeronPort, "fix-acceptor"));
    }

    @After
    public void close() throws Exception
    {
        quietClose(library);
        quietClose(engine);
        quietClose(mediaDriver);
    }
}
