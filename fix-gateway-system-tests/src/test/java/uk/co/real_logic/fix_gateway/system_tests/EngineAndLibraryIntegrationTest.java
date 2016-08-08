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
import org.agrona.CloseHelper;
import org.agrona.concurrent.IdleStrategy;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.fix_gateway.engine.EngineConfiguration;
import uk.co.real_logic.fix_gateway.engine.FixEngine;
import uk.co.real_logic.fix_gateway.engine.framer.LibraryInfo;
import uk.co.real_logic.fix_gateway.library.FixLibrary;
import uk.co.real_logic.fix_gateway.library.LibraryConfiguration;
import uk.co.real_logic.fix_gateway.validation.AuthenticationStrategy;
import uk.co.real_logic.fix_gateway.validation.MessageValidationStrategy;

import java.util.Arrays;
import java.util.List;

import static io.aeron.CommonContext.IPC_CHANNEL;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertThat;
import static uk.co.real_logic.fix_gateway.CommonConfiguration.backoffIdleStrategy;
import static uk.co.real_logic.fix_gateway.TestFixtures.*;
import static uk.co.real_logic.fix_gateway.Timing.assertEventuallyTrue;
import static uk.co.real_logic.fix_gateway.system_tests.SystemTestUtil.*;
import static uk.co.real_logic.fix_gateway.util.CustomMatchers.hasFluentProperty;

public class EngineAndLibraryIntegrationTest
{
    private static final IdleStrategy ADMIN_IDLE_STRATEGY = backoffIdleStrategy();
    private static final int SHORT_TIMEOUT_IN_MS = 100;

    private MediaDriver mediaDriver;
    private FixEngine engine;
    private FixLibrary library;
    private FixLibrary library2;

    private FakeOtfAcceptor otfAcceptor = new FakeOtfAcceptor();
    private FakeHandler sessionHandler = new FakeHandler(otfAcceptor);

    @Before
    public void launch()
    {
        mediaDriver = launchMediaDriver();

        launchEngine(SHORT_TIMEOUT_IN_MS);
    }

    private void launchEngine(final int replyTimeoutInMs)
    {
        delete(ACCEPTOR_LOGS);
        final EngineConfiguration config = acceptingConfig(
            unusedPort(), "engineCounters", ACCEPTOR_ID, INITIATOR_ID);
        config.replyTimeoutInMs(replyTimeoutInMs);
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

        assertHasLibraries(matchesLibrary(library.libraryId()));
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
        setupTwoLibraries();

        assertHasLibraries(
            matchesLibrary(library.libraryId()),
            matchesLibrary(library2.libraryId()));
    }

    @Test
    public void engineDetectsDisconnectOfSpecificLibraryInstances()
    {
        setupTwoLibrariesAndCloseTheFirst();
    }

    private FixLibrary setupTwoLibrariesAndCloseTheFirst()
    {
        setupTwoLibraries();

        library.close();

        assertLibrariesDisconnect(1, library2, engine);

        assertHasLibraries(matchesLibrary(library2.libraryId()));

        return library2;
    }

    private void setupTwoLibraries()
    {
        library = connectLibrary();
        awaitLibraryConnect(library);

        library2 = connectLibrary();
        awaitLibraryConnect(library2);
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
        library = connectLibrary();

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

    private Matcher<LibraryInfo> matchesLibrary(final int libraryId)
    {
        return hasFluentProperty("libraryId", is(libraryId));
    }

    @SafeVarargs
    private final void assertHasLibraries(final Matcher<LibraryInfo>... libraryMatchers)
    {
        final List<LibraryInfo> libraries = engine.libraries(ADMIN_IDLE_STRATEGY);
        assertThat(libraries, containsInAnyOrder(libraryMatchers));
    }

    private void assertNoActiveLibraries(final int count)
    {
        assertThat("libraries haven't disconnected yet", engine.libraries(ADMIN_IDLE_STRATEGY), hasSize(count));
    }

    private FixLibrary connectLibrary()
    {
        final MessageValidationStrategy validationStrategy = MessageValidationStrategy.targetCompId(ACCEPTOR_ID)
            .and(MessageValidationStrategy.senderCompId(Arrays.asList(INITIATOR_ID, INITIATOR_ID2)));

        final AuthenticationStrategy authenticationStrategy = AuthenticationStrategy.of(validationStrategy);

        final LibraryConfiguration config = new LibraryConfiguration();
        config
            .sessionAcquireHandler(sessionHandler)
            .libraryAeronChannels(singletonList(IPC_CHANNEL))
            .authenticationStrategy(authenticationStrategy)
            .messageValidationStrategy(validationStrategy)
            .replyTimeoutInMs(TIMEOUT_IN_MS);

        return FixLibrary.connect(config);
    }

    @After
    public void close() throws Exception
    {
        CloseHelper.close(library);
        CloseHelper.close(library2);
        CloseHelper.close(engine);
        CloseHelper.close(mediaDriver);
        cleanupDirectory(mediaDriver);
    }
}
