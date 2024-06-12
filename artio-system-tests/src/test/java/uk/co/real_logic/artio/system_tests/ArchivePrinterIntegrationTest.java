/*
 * Copyright 2015-2024 Real Logic Limited, Adaptive Financial Consulting Ltd.
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static uk.co.real_logic.artio.TestFixtures.launchMediaDriver;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.ACCEPTOR_LOGS;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.acceptingLibraryConfig;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.connect;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.delete;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.launchInitiatingEngine;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.newInitiatingLibrary;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;

import org.junit.Before;
import org.junit.Test;

import io.aeron.Aeron;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.logger.FixArchivePrinter;
import uk.co.real_logic.artio.library.LibraryConfiguration;

public class ArchivePrinterIntegrationTest extends AbstractGatewayToGatewaySystemTest
{
    private final FakeConnectHandler fakeConnectHandler = new FakeConnectHandler();

    @Before
    public void launch()
    {
        delete(ACCEPTOR_LOGS);

        mediaDriver = launchMediaDriver();

        launchAcceptingEngine();
        initiatingEngine = launchInitiatingEngine(libraryAeronPort, nanoClock);

        final LibraryConfiguration acceptingLibraryConfig = acceptingLibraryConfig(acceptingHandler, nanoClock);
        acceptingLibraryConfig.libraryConnectHandler(fakeConnectHandler);
        acceptingLibrary = connect(acceptingLibraryConfig);
        initiatingLibrary = newInitiatingLibrary(libraryAeronPort, initiatingHandler, nanoClock);
        testSystem = new TestSystem(acceptingLibrary, initiatingLibrary);

        connectSessions();
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldUseDefaultDelimiter()
    {
        setupAndExchangeMessages();

        final EngineConfiguration configuration = acceptingEngine.configuration();
        final Aeron.Context context = configuration.aeronContext();
        final ByteArrayOutputStream outputBytes = new ByteArrayOutputStream();
        final FixArchivePrinter fixArchivePrinter = new FixArchivePrinter(new PrintStream(outputBytes), System.err);
        final String[] args = new String[] {
            "--aeron-channel=" + configuration.libraryAeronChannel(),
            "--log-file-dir=" + configuration.logFileDir(),
            "--aeron-dir-name=" + context.aeronDirectory().getAbsolutePath(),
            "--query-stream-id=" + configuration.outboundLibraryStream()
        };

        fixArchivePrinter.scan(args);

        assertThat(outputBytes.toString(), containsString("\u0001112=hi"));
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void canUseSpecifiedCharAsDelimiter()
    {
        setupAndExchangeMessages();

        final EngineConfiguration configuration = acceptingEngine.configuration();
        final Aeron.Context context = configuration.aeronContext();
        final ByteArrayOutputStream outputBytes = new ByteArrayOutputStream();
        final FixArchivePrinter fixArchivePrinter = new FixArchivePrinter(new PrintStream(outputBytes), System.err);
        final String[] args = new String[] {
            "--delimiter=|",
            "--aeron-channel=" + configuration.libraryAeronChannel(),
            "--log-file-dir=" + configuration.logFileDir(),
            "--aeron-dir-name=" + context.aeronDirectory().getAbsolutePath(),
            "--query-stream-id=" + configuration.outboundLibraryStream()
        };

        fixArchivePrinter.scan(args);

        assertThat(outputBytes.toString(), containsString("|112=hi"));
    }

    private void setupAndExchangeMessages()
    {
        messagesCanBeExchanged();

        assertInitiatingSequenceIndexIs(0);
    }
}
