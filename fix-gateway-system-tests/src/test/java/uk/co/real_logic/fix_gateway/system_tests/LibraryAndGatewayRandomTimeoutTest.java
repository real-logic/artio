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
import uk.co.real_logic.agrona.IoUtil;
import uk.co.real_logic.fix_gateway.FixGatewayException;
import uk.co.real_logic.fix_gateway.engine.FixEngine;
import uk.co.real_logic.fix_gateway.library.FixLibrary;
import uk.co.real_logic.fix_gateway.library.LibraryConfiguration;

import java.io.File;

import static uk.co.real_logic.fix_gateway.TestFixtures.cleanupDirectory;
import static uk.co.real_logic.fix_gateway.TestFixtures.launchMediaDriver;
import static uk.co.real_logic.fix_gateway.TestFixtures.unusedPort;
import static uk.co.real_logic.fix_gateway.system_tests.SystemTestUtil.*;

public class LibraryAndGatewayRandomTimeoutTest
{
    private int aeronPort = unusedPort();
    private int port = unusedPort();
    private MediaDriver mediaDriver;
    private FixEngine initiatingEngine;
    private FixLibrary initiatingLibrary;

    private FakeOtfAcceptor initiatingOtfAcceptor = new FakeOtfAcceptor();
    private FakeSessionHandler initiatingSessionHandler = new FakeSessionHandler(initiatingOtfAcceptor);

    @Before
    public void launch()
    {
        mediaDriver = launchMediaDriver();
    }

    @Test(expected = FixGatewayException.class)
    public void libraryShouldRefuseConnectionWhenTheresNoAcceptor()
    {
        launchEngine();
        launchLibrary();

        initiate(initiatingLibrary, port, INITIATOR_ID, ACCEPTOR_ID);
    }

    @Test(expected = IllegalStateException.class)
    public void libraryShouldRefuseConnectionWhenEngineNotStarted()
    {
        launchLibrary();
    }

    @Test(expected = IllegalStateException.class)
    public void libraryShouldRefuseConnectionWhenEngineClosed()
    {
        launchEngine();
        launchLibrary();
        initiatingEngine.close();

        initiate(initiatingLibrary, port, INITIATOR_ID, ACCEPTOR_ID);
    }

    private void launchLibrary()
    {
        initiatingLibrary = FixLibrary.connect(
            new LibraryConfiguration()
                .newSessionHandler(initiatingSessionHandler)
                .aeronChannel("udp://localhost:" + aeronPort)
                .monitoringFile(IoUtil.tmpDirName() + "fix-client" + File.separator + "libraryCounters"));
    }

    private void launchEngine()
    {
        initiatingEngine = launchInitiatingGateway(aeronPort);
    }

    @After
    public void close() throws Exception
    {
        closeIfOpen(initiatingLibrary);
        closeIfOpen(initiatingEngine);
        closeIfOpen(mediaDriver);
        cleanupDirectory(mediaDriver);
    }
}
