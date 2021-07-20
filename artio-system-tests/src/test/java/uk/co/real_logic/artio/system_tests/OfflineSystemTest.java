/*
 * Copyright 2021 Monotonic Ltd.
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
import uk.co.real_logic.artio.engine.framer.LibraryInfo;
import uk.co.real_logic.artio.library.FixLibrary;
import uk.co.real_logic.artio.messages.SessionReplyStatus;
import uk.co.real_logic.artio.messages.SessionState;
import uk.co.real_logic.artio.session.Session;

import java.util.List;
import java.util.Optional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.*;
import static uk.co.real_logic.artio.Timing.assertEventuallyTrue;
import static uk.co.real_logic.artio.Timing.withTimeout;
import static uk.co.real_logic.artio.messages.SessionReplyStatus.OK;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.*;


/**
 * Offline session tests that don't require persistent sequence numbers.
 */
public class OfflineSystemTest extends AbstractGatewayToGatewaySystemTest
{

    @Before
    public void launch()
    {
        launchGatewayToGateway();
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldBeAbleToLookupOfflineSession()
    {
        acquireAcceptingSession();
        messagesCanBeExchanged();
        clearMessages();

        logoutAcceptingSession();
        assertSessionsDisconnected();

        final long sessionId = acceptingSession.id();
        final long lastSequenceResetTime = acceptingSession.lastSequenceResetTimeInNs();
        final long lastLogonTime = acceptingSession.lastLogonTimeInNs();
        acceptingSession = null;

        assertNotEquals(Session.UNKNOWN_TIME, lastSequenceResetTime);
        assertNotEquals(Session.UNKNOWN_TIME, lastLogonTime);

        acquireAcceptingSession();

        assertOfflineSession(sessionId, acceptingSession);
        assertEquals(lastSequenceResetTime, acceptingSession.lastSequenceResetTimeInNs());
        assertEquals(lastLogonTime, acceptingSession.lastLogonTimeInNs());

        assertAllSessionsOnlyContains(acceptingEngine, acceptingSession);

        assertReplayReceivedMessages();

        connectSessions();

        assertEventuallyTrue("offline session is reconnected", () ->
        {
            testSystem.poll();

            return acceptingSession.state() == SessionState.ACTIVE;
        }, 3_000L);
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void engineShouldNotAcquireTimedOutOfflineSessions()
    {
        logoutInitiatingSession();
        assertSessionDisconnected(initiatingSession);

        acquireAcceptingSession();
        assertEquals(SessionState.DISCONNECTED, acceptingSession.state());

        testSystem.remove(acceptingLibrary);

        final List<LibraryInfo> libraries = withTimeout("Library failed to timeout", () ->
            Optional.of(libraries(acceptingEngine, testSystem)).filter(infos -> infos.size() == 1), 5_000);
        assertThat(libraries.get(0).sessions(), hasSize(0));
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldNotAcquireOrInitiateOfflineSessionOwnedByAnotherLibrary()
    {
        final long sessionId = initiatingSession.id();

        logoutInitiatingSession();
        assertSessionDisconnected(initiatingSession);
        acceptingHandler.clearSessionExistsInfos();

        try (FixLibrary initiatingLibrary2 = testSystem.connect(
            initiatingLibraryConfig(libraryAeronPort, initiatingHandler, nanoClock)))
        {
            assertTrue(initiatingLibrary.isConnected());

            final SessionReplyStatus reply = requestSession(initiatingLibrary2, sessionId, testSystem);
            assertEquals(OK, reply);
            assertFalse(acceptingHandler.hasSeenSession());
            final Session offlineInitiatingSession = initiatingHandler.lastSession();
            assertNotSame(initiatingSession, offlineInitiatingSession);
            assertOfflineSession(sessionId, offlineInitiatingSession);

            final SessionReplyStatus failedStatus = requestSession(initiatingLibrary, sessionId, testSystem);
            assertEquals(SessionReplyStatus.OTHER_SESSION_OWNER, failedStatus);

            final Reply<Session> failedReply = initiate(initiatingLibrary, port, INITIATOR_ID, ACCEPTOR_ID);
            completeFailedSession(failedReply);
            assertThat(failedReply.error().getMessage(), containsString("DUPLICATE"));
        }
    }
}
