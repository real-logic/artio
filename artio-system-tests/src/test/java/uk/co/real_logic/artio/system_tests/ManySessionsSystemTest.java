/*
 * Copyright 2019 Adaptive Financial Consulting Ltd.
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

import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.artio.Reply;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.engine.LowResourceEngineScheduler;
import uk.co.real_logic.artio.library.LibraryConfiguration;
import uk.co.real_logic.artio.session.Session;
import uk.co.real_logic.artio.validation.AuthenticationStrategy;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.aeron.CommonContext.IPC_CHANNEL;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertFalse;
import static uk.co.real_logic.artio.TestFixtures.launchMediaDriver;
import static uk.co.real_logic.artio.Timing.assertEventuallyTrue;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.*;

public class ManySessionsSystemTest extends AbstractGatewayToGatewaySystemTest
{
    private static final int NUMBER_OF_SESSIONS = 10;

    @Before
    public void launch()
    {
        delete(ACCEPTOR_LOGS);

        mediaDriver = launchMediaDriver();

        final EngineConfiguration configuration = new EngineConfiguration();
        final AuthenticationStrategy authenticationStrategy = AuthenticationStrategy.none();
        configuration.authenticationStrategy(authenticationStrategy);

        acceptingEngine = FixEngine.launch(
            configuration
                .bindTo("localhost", port)
                .libraryAeronChannel(IPC_CHANNEL)
                .monitoringFile(acceptorMonitoringFile("engineCounters"))
                .logFileDir(ACCEPTOR_LOGS)
                .scheduler(new LowResourceEngineScheduler()));

        initiatingEngine = launchInitiatingEngine(libraryAeronPort, nanoClock);

        final LibraryConfiguration acceptingLibraryConfig = new LibraryConfiguration()
            .sessionExistsHandler(acceptingHandler)
            .sessionAcquireHandler(acceptingHandler)
            .libraryAeronChannels(singletonList(IPC_CHANNEL))
            .libraryName("accepting");

        acceptingLibrary = connect(acceptingLibraryConfig);
        initiatingLibrary = newInitiatingLibrary(libraryAeronPort, initiatingHandler, nanoClock);
        testSystem = new TestSystem(acceptingLibrary, initiatingLibrary);
    }

    @SuppressWarnings("unchecked")
    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldConnectManySessions()
    {
        final long timeoutInMs = 5 * TEST_REPLY_TIMEOUT_IN_MS;

        final Reply<Session>[] replies = new Reply[NUMBER_OF_SESSIONS];
        for (int i = 0; i < NUMBER_OF_SESSIONS; i++)
        {
            replies[i] = initiate(initiatingLibrary, port, initId(i), accId(i), timeoutInMs);
            testSystem.poll();
        }

        testSystem.awaitCompletedReplies(replies);

        final List<Session> sessions = Stream.of(replies)
            .map(Reply::resultIfPresent)
            .collect(Collectors.toList());

        sessions.forEach(this::messagesCanBeExchanged);
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldBeNotifiedOnSessionLogoutAndDisconnect()
    {
        final Reply<Session> sessionReply = initiate(initiatingLibrary, port, INITIATOR_ID, ACCEPTOR_ID);

        acquireAcceptingSession();

        testSystem.awaitCompletedReplies(sessionReply);
        initiatingSession = sessionReply.resultIfPresent();

        assertFalse(acceptingHandler.hasDisconnected());

        assertThat(initiatingSession.logoutAndDisconnect(), greaterThan(0L));

        assertSessionDisconnected(initiatingSession);

        assertEventuallyTrue("SessionHandler.onDisconnect has not been called", () ->
        {
            testSystem.poll();
            return acceptingHandler.hasDisconnected();
        });
    }

    private static String accId(final int i)
    {
        return ACCEPTOR_ID + i;
    }

    private static String initId(final int i)
    {
        return INITIATOR_ID + i;
    }
}
