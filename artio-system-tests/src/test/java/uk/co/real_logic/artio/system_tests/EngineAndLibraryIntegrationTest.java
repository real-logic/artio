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
import org.agrona.CloseHelper;
import org.agrona.concurrent.EpochNanoClock;
import org.agrona.concurrent.OffsetEpochNanoClock;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.artio.FixMatchers;
import uk.co.real_logic.artio.dictionary.generation.Exceptions;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.engine.framer.LibraryInfo;
import uk.co.real_logic.artio.library.FixLibrary;
import uk.co.real_logic.artio.library.LibraryConfiguration;
import uk.co.real_logic.artio.validation.MessageValidationStrategy;

import java.util.Arrays;

import static io.aeron.CommonContext.IPC_CHANNEL;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static uk.co.real_logic.artio.TestFixtures.*;
import static uk.co.real_logic.artio.Timing.assertEventuallyTrue;
import static uk.co.real_logic.artio.engine.FixEngine.ENGINE_LIBRARY_ID;
import static uk.co.real_logic.artio.messages.SessionReplyStatus.OK;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.*;

public class EngineAndLibraryIntegrationTest
{
    private static final int SHORT_TIMEOUT_IN_MS = 100;

    private final EpochNanoClock nanoClock = new OffsetEpochNanoClock();
    private ArchivingMediaDriver mediaDriver;
    private FixEngine engine;
    private FixLibrary library;
    private FixLibrary library2;

    private final FakeConnectHandler fakeConnectHandler = new FakeConnectHandler();
    private final FakeOtfAcceptor otfAcceptor = new FakeOtfAcceptor();
    private final FakeHandler sessionHandler = new FakeHandler(otfAcceptor);
    private final TestSystem testSystem = new TestSystem();
    private final int port = unusedPort();

    @Before
    public void launch()
    {
        mediaDriver = launchMediaDriver();

        launchEngine(SHORT_TIMEOUT_IN_MS);
    }

    @After
    public void close()
    {
        try
        {
            Exceptions.closeAll(library, library2, engine);
        }
        finally
        {
            cleanupMediaDriver(mediaDriver);
        }
    }

    private void launchEngine(final int replyTimeoutInMs)
    {
        final EngineConfiguration config = acceptingConfig(port, ACCEPTOR_ID, INITIATOR_ID, nanoClock);
        config.deleteLogFileDirOnStart(true);
        config.slowConsumerTimeoutInMs(replyTimeoutInMs);
        config.replyTimeoutInMs(replyTimeoutInMs);
        engine = FixEngine.launch(config);
    }

    @Test
    public void engineInitiallyHasNoConnectedLibraries()
    {
        assertNumActiveLibraries(0);
    }

    @Test
    public void engineDetectsLibraryConnect()
    {
        library = connectLibrary();

        assertEventuallyHasLibraries(
            FixMatchers.matchesLibrary(library.libraryId()),
            FixMatchers.matchesLibrary(ENGINE_LIBRARY_ID));
    }

    @Test
    public void engineDetectsLibraryDisconnect()
    {
        library = connectLibrary();
        awaitLibraryConnect(library);

        testSystem.close(library);

        assertLibrariesDisconnect(0, engine);
    }

    @Test
    public void engineDetectsMultipleLibraryInstances()
    {
        setupTwoLibraries();

        assertEventuallyHasLibraries(
            FixMatchers.matchesLibrary(library.libraryId()),
            FixMatchers.matchesLibrary(library2.libraryId()),
            FixMatchers.matchesLibrary(ENGINE_LIBRARY_ID));
    }

    @Test
    public void engineDetectsDisconnectOfSpecificLibraryInstances()
    {
        setupTwoLibrariesAndCloseTheFirst();
    }

    private void setupTwoLibrariesAndCloseTheFirst()
    {
        setupTwoLibraries();

        testSystem.close(library);

        assertLibrariesDisconnect(1, engine);

        assertEventuallyHasLibraries(
            FixMatchers.matchesLibrary(library2.libraryId()),
            FixMatchers.matchesLibrary(ENGINE_LIBRARY_ID));
    }

    private void setupTwoLibraries()
    {
        library = connectLibrary();

        library2 = connectLibrary();
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
            () -> "Engine still hasn't disconnected",
            () ->
            {
                library.poll(5);
                return !library.isConnected();
            },
            AWAIT_TIMEOUT_IN_MS,
            () ->
            {
            }
        );
    }

    //  -Dfix.core.debug=GATEWAY_MESSAGE
    @Test(timeout = 10_000L)
    public void libraryShouldReconnectToEngine() throws Exception
    {
        final int beyondTimeout = SHORT_TIMEOUT_IN_MS + 1;

        library = connectLibrary();
        awaitLibraryConnect(library);
        assertNumActiveLibraries(1);

        try (FixConnection connection = FixConnection.initiate(port))
        {
            connection.logon(true);
            connection.readLogon();

            final long sessionId = sessionHandler.awaitSessionId(testSystem::poll);
            assertEquals(OK, requestSession(library, sessionId, testSystem));

            Thread.sleep(beyondTimeout);
            assertEventuallyTrue("engine fails to timeout library", () -> libraries(engine).size() == 1);
            // Poll until engine heartbeat messages are all read in order to force a library timeout
            library.poll(50);

            assertEventuallyTrue("Library still connected", () ->
            {
                Thread.sleep(beyondTimeout);
                testSystem.poll();
                assertFalse("library still connected", library.isConnected());
            });

            assertEventuallyTrue("library reconnect fails", () ->
            {
                testSystem.poll();
                return libraries(engine).size() == 2 && library.isConnected();
            });
        }
    }

    @Test
    public void shouldNotAllowClosingMidPoll()
    {
        fakeConnectHandler.shouldCloseOnConnect(true);

        library = connectLibrary();
        awaitLibraryConnect(library);

        assertThat(fakeConnectHandler.exception(), isA(IllegalArgumentException.class));
    }

    @SafeVarargs
    private final void assertEventuallyHasLibraries(final Matcher<LibraryInfo>... libraryMatchers)
    {
        SystemTestUtil.assertEventuallyHasLibraries(testSystem, engine, libraryMatchers);
    }

    private void assertNumActiveLibraries(final int count)
    {
        // +1 to account for the engine managed sessions library object.
        assertThat("libraries haven't disconnected yet", libraries(engine), hasSize(count + 1));
    }

    private FixLibrary connectLibrary()
    {
        final MessageValidationStrategy validationStrategy = MessageValidationStrategy.targetCompId(ACCEPTOR_ID)
            .and(MessageValidationStrategy.senderCompId(Arrays.asList(INITIATOR_ID, INITIATOR_ID2)));

        final LibraryConfiguration config = new LibraryConfiguration();
        config
            .sessionExistsHandler(sessionHandler)
            .sessionAcquireHandler(sessionHandler)
            .libraryConnectHandler(fakeConnectHandler)
            .libraryAeronChannels(singletonList(IPC_CHANNEL))
            .messageValidationStrategy(validationStrategy)
            .replyTimeoutInMs(SHORT_TIMEOUT_IN_MS);

        return testSystem.add(connect(config));
    }

    private void assertLibrariesDisconnect(final int count, final FixEngine engine)
    {
        assertEventuallyTrue(
            () -> "libraries haven't disconnected yet",
            () ->
            {
                testSystem.poll();
                return libraries(engine).size() == count + 1;
            },
            AWAIT_TIMEOUT_IN_MS,
            () ->
            {
            }
        );
    }
}
