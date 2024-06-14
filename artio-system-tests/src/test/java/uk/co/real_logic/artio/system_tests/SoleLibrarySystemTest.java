/*
 * Copyright 2019-2020 Adaptive Financial Consulting Ltd., Monotonic Ltd.
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

import uk.co.real_logic.artio.engine.*;
import uk.co.real_logic.artio.engine.framer.LibraryInfo;
import uk.co.real_logic.artio.library.FixLibrary;
import uk.co.real_logic.artio.library.LibraryConfiguration;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.*;
import static uk.co.real_logic.artio.FixMatchers.hasConnectionId;
import static uk.co.real_logic.artio.FixMatchers.hasSessionId;
import static uk.co.real_logic.artio.TestFixtures.launchMediaDriver;
import static uk.co.real_logic.artio.Timing.DEFAULT_TIMEOUT_IN_MS;
import static uk.co.real_logic.artio.Timing.assertEventuallyTrue;
import static uk.co.real_logic.artio.messages.InitialAcceptedSessionOwner.SOLE_LIBRARY;
import static uk.co.real_logic.artio.messages.SessionState.ACTIVE;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.*;

public class SoleLibrarySystemTest extends AbstractGatewayToGatewaySystemTest
{
    private LockStepFramerEngineScheduler scheduler;

    private void launch()
    {
        launch(true, false);
    }

    private void launch(final boolean logMessages, final boolean useScheduler)
    {
        mediaDriver = launchMediaDriver();


        final EngineConfiguration acceptingConfig = acceptingConfig(port, ACCEPTOR_ID, INITIATOR_ID, nanoClock)
            .deleteLogFileDirOnStart(true)
            .replyTimeoutInMs(120_000)
            .initialAcceptedSessionOwner(SOLE_LIBRARY);

        if (useScheduler)
        {
            scheduler = new LockStepFramerEngineScheduler();
            acceptingConfig.scheduler(scheduler);
        }

        acceptingEngine = FixEngine.launch(acceptingConfig);

        final EngineConfiguration initiatingConfig =
            initiatingConfig(libraryAeronPort, nanoClock).deleteLogFileDirOnStart(true)
            .initialAcceptedSessionOwner(SOLE_LIBRARY)
            .logInboundMessages(logMessages)
            .logOutboundMessages(logMessages);
        initiatingEngine = FixEngine.launch(initiatingConfig);

        final LibraryConfiguration acceptingLibraryConfig = acceptingLibraryConfig(acceptingHandler, nanoClock);
        acceptingLibrary = FixLibrary.connect(acceptingLibraryConfig);
        assertEventuallyTrue(
            () -> "Unable to connect to engine",
            () ->
            {
                if (useScheduler)
                {
                    scheduler.invokeFramer();
                }
                acceptingLibrary.poll(LIBRARY_LIMIT);

                return acceptingLibrary.isConnected();
            },
            DEFAULT_TIMEOUT_IN_MS,
            acceptingLibrary::close);
        initiatingLibrary = connect(initiatingLibraryConfig(libraryAeronPort, initiatingHandler, nanoClock));
        testSystem = new TestSystem(scheduler, acceptingLibrary, initiatingLibrary);
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldOnlyHandOffSessionToApplicationWhenConnected()
    {
        launch();

        connectAndAcquire();
        assertNotNull("should automatically receive the session upon logon In SOLE_LIBRARY mode",
            acceptingSession);
        assertEquals(ACTIVE, acceptingSession.state());

        assertFalse("should not receive session exists callback in sole library mode",
            acceptingHandler.hasSeenSession());

        assertSequenceResetTimeAtLatestLogon(acceptingSession);
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldSupportUnreleasedOfflineSessionsInSoleLibraryMode()
    {
        launch();

        connectAndAcquire();
        disconnectSessions();
        assertThat(acceptingLibrary.sessions(), hasItem(acceptingSession));
        final long sessionId = acceptingSession.id();
        assertCountersClosed(false, acceptingSession);

        assertOfflineSession(sessionId, acceptingSession);
        assertCountersClosed(false, acceptingSession);

        final LibraryInfo libInfo = libraryInfoById(libraries(acceptingEngine), acceptingLibrary.libraryId()).get();
        final List<ConnectedSessionInfo> sessions = libInfo.sessions();
        assertThat(sessions, contains(allOf(
            hasConnectionId(SessionInfo.UNK_SESSION),
            hasSessionId(sessionId))));

        connectSessions();
        assertEquals(ACTIVE, acceptingSession.state());
        assertEquals(1, acceptingSession.lastSentMsgSeqNum());
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldSupportManySessionReconnections()
    {
        launch(false, true);

        // 300 > number in groupSizeEncoding
        for (int i = 0; i < 300; i++)
        {
            connectAndAcquire();
            messagesCanBeExchanged();
            disconnectSessions();
        }

        assertThat(acceptingLibrary.sessions(), hasItem(acceptingSession));
        final long sessionId = acceptingSession.id();
        assertCountersClosed(false, acceptingSession);

        assertOfflineSession(sessionId, acceptingSession);
        assertCountersClosed(false, acceptingSession);

        final long testDeadlineMs = System.currentTimeMillis() + 10_000;

        // trigger library timeout
        while (acceptingLibrary.isConnected())
        {
            acceptingLibrary.poll(10);
            initiatingLibrary.poll(10);

            if (System.currentTimeMillis() > testDeadlineMs)
            {
                fail("Failed to disconnect library");
            }
        }

        // library reconnect
        while (!acceptingLibrary.isConnected())
        {
            testSystem.poll();

            if (System.currentTimeMillis() > testDeadlineMs)
            {
                fail("Failed to reconnect library");
            }
        }

        connectAndAcquire();
        assertEquals(ACTIVE, acceptingSession.state());
        messagesCanBeExchanged();
    }

    private void connectAndAcquire()
    {
        connectSessions();
        acceptingSession = acceptingHandler.lastSession();
    }

    // Replicates a bug reported in issue #361 where reconnecting initiators can't reconnect.
    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldAllowReconnectingInitiatorsToReconnect()
    {
        launch();

        connectAndAcquire();
        messagesCanBeExchanged();
        disconnectSessions();

        connectSessions();
        messagesCanBeExchanged();
        disconnectSessions();
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldAcquireSessionsWithLoggingSwitchedOff()
    {
        // Equivalent invariant tested in Engine mode in NoLoggingGatewayToGatewaySystemTest
        launch(false, false);

        connectAndAcquire();
        acceptingMessagesCanBeExchanged();

        // timeout initiatingLibrary
        testSystem.remove(initiatingLibrary);
        awaitLibraryDisconnect(initiatingEngine, testSystem);

        acceptingMessagesCanBeExchanged();
    }
}
