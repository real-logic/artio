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

import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.artio.engine.ConnectedSessionInfo;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.engine.SessionInfo;
import uk.co.real_logic.artio.engine.framer.LibraryInfo;
import uk.co.real_logic.artio.library.LibraryConfiguration;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static uk.co.real_logic.artio.FixMatchers.hasConnectionId;
import static uk.co.real_logic.artio.FixMatchers.hasSessionId;
import static uk.co.real_logic.artio.TestFixtures.launchMediaDriver;
import static uk.co.real_logic.artio.messages.InitialAcceptedSessionOwner.SOLE_LIBRARY;
import static uk.co.real_logic.artio.messages.SessionState.ACTIVE;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.*;

public class SoleLibrarySystemTest extends AbstractGatewayToGatewaySystemTest
{

    @Before
    public void launch()
    {
        mediaDriver = launchMediaDriver();

        final EngineConfiguration acceptingConfig = acceptingConfig(port, ACCEPTOR_ID, INITIATOR_ID)
            .deleteLogFileDirOnStart(true)
            .initialAcceptedSessionOwner(SOLE_LIBRARY);
        acceptingEngine = FixEngine.launch(acceptingConfig);

        final EngineConfiguration initiatingConfig = initiatingConfig(libraryAeronPort);
        initiatingConfig.deleteLogFileDirOnStart(true);
        initiatingConfig.initialAcceptedSessionOwner(SOLE_LIBRARY);
        initiatingEngine = FixEngine.launch(initiatingConfig);

        final LibraryConfiguration acceptingLibraryConfig = acceptingLibraryConfig(acceptingHandler);
        acceptingLibrary = connect(acceptingLibraryConfig);
        initiatingLibrary = connect(initiatingLibraryConfig(libraryAeronPort, initiatingHandler));
        testSystem = new TestSystem(acceptingLibrary, initiatingLibrary);
    }

    @Test
    public void shouldOnlyHandOffSessionToApplicationWhenConnected()
    {
        connectSessions();

        acceptingSession = acceptingHandler.lastSession();
        assertNotNull("should automatically receive the session upon logon In SOLE_LIBRARY mode",
            acceptingSession);
        assertEquals(ACTIVE, acceptingSession.state());

        assertFalse("should not receive session exists callback in sole library mode",
            acceptingHandler.hasSeenSession());

        assertSequenceResetTimeAtLatestLogon(acceptingSession);
    }

    @Test
    public void shouldSupportUnreleasedOfflineSessionsInSoleLibraryMode()
    {
        connectSessions();
        acceptingSession = acceptingHandler.lastSession();
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
    }

    // Replicates a bug reported in issue #361 where reconnecting initiators can't reconnect.
    @Test
    public void shouldNotAffectUseAsInitiator()
    {
        connectSessions();
        acceptingSession = acceptingHandler.lastSession();
        messagesCanBeExchanged();
        disconnectSessions();

        connectSessions();
        messagesCanBeExchanged();
        disconnectSessions();
    }
}
