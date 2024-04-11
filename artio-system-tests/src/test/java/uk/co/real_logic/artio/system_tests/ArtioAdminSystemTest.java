/*
 * Copyright 2020 Monotonic Ltd.
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

import io.aeron.exceptions.TimeoutException;
import org.agrona.CloseHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.artio.FixGatewayException;
import uk.co.real_logic.artio.Reply;
import uk.co.real_logic.artio.admin.ArtioAdmin;
import uk.co.real_logic.artio.admin.ArtioAdminConfiguration;
import uk.co.real_logic.artio.admin.FixAdminSession;
import uk.co.real_logic.artio.engine.DefaultEngineScheduler;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.library.LibraryConfiguration;
import uk.co.real_logic.artio.session.Session;

import java.util.List;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static uk.co.real_logic.artio.TestFixtures.launchMediaDriver;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.*;
import static uk.co.real_logic.artio.system_tests.TestSystem.LONG_AWAIT_TIMEOUT_IN_MS;
import static uk.co.real_logic.artio.util.CustomMatchers.assertThrows;

public class ArtioAdminSystemTest extends AbstractGatewayToGatewaySystemTest
{
    private ArtioAdmin artioAdmin;

    @Before
    public void launch()
    {
        mediaDriver = launchMediaDriver();

        acceptingEngine = FixEngine.launch(acceptingConfig(port, ACCEPTOR_ID, INITIATOR_ID, nanoClock)
            .scheduler(new DefaultEngineScheduler())
            .deleteLogFileDirOnStart(true));

        initiatingEngine = launchInitiatingEngine(libraryAeronPort, nanoClock);

        final LibraryConfiguration acceptingLibraryConfig = acceptingLibraryConfig(acceptingHandler, nanoClock);
        acceptingLibrary = connect(acceptingLibraryConfig);
        initiatingLibrary = newInitiatingLibrary(libraryAeronPort, initiatingHandler, nanoClock);
        testSystem = new TestSystem(acceptingLibrary, initiatingLibrary);
    }

    @After
    public void teardown()
    {
        CloseHelper.close(artioAdmin);
    }

    @Test(timeout = LONG_AWAIT_TIMEOUT_IN_MS)
    public void shouldQuerySessionStatus()
    {
        connectSessions();
        acquireAcceptingSession();
        messagesCanBeExchanged();

        testSystem.awaitLongBlocking(() ->
        {
            launchArtioAdmin();
            assertFalse(artioAdmin.isClosed());

            final List<FixAdminSession> allFixSessions = artioAdmin.allFixSessions();
            assertThat(allFixSessions, hasSize(1));

            assertSessionEquals(acceptingSession, allFixSessions.get(0));

            artioAdmin.close();
            assertTrue(artioAdmin.isClosed());

            artioAdmin.close();
            assertTrue("Close not idempotent", artioAdmin.isClosed());
        });
    }

    @Test(timeout = LONG_AWAIT_TIMEOUT_IN_MS)
    public void shouldQueryMultipleSessions()
    {
        connectSessions();
        acquireAcceptingSession();
        messagesCanBeExchanged();

        // Create another session that will then be disconnected
        Reply<Session> successfulReply = initiate(initiatingLibrary, port, INITIATOR_ID3, ACCEPTOR_ID);
        final Session offlineSession = completeConnectSessions(successfulReply);
        logoutSession(offlineSession);
        assertSessionDisconnected(offlineSession);

        // A second session, temporarily gateway managed
        successfulReply = initiate(initiatingLibrary, port, INITIATOR_ID2, ACCEPTOR_ID);
        final Session otherInitSession = completeConnectSessions(successfulReply);
        messagesCanBeExchanged(otherInitSession);
        assertThat(acceptingLibrary.sessions(), hasSize(1));

        // admin API queries the index for sequence numbers, so we need to wait
        awaitIndexerCaughtUp(
            testSystem,
            mediaDriver.mediaDriver().aeronDirectoryName(),
            acceptingEngine,
            acceptingLibrary);

        testSystem.awaitLongBlocking(() ->
        {
            launchArtioAdmin();

            final List<FixAdminSession> allFixSessions = artioAdmin.allFixSessions();
            assertThat(allFixSessions, hasSize(3));

            FixAdminSession fixAdminSession = allFixSessions.get(0);
            assertTrue(fixAdminSession.isConnected());
            assertEquals(INITIATOR_ID2, fixAdminSession.sessionKey().remoteCompId());
            assertEquals(otherInitSession.lastSentMsgSeqNum(), fixAdminSession.lastReceivedMsgSeqNum());
            assertEquals(otherInitSession.lastReceivedMsgSeqNum(), fixAdminSession.lastSentMsgSeqNum());

            assertSessionEquals(acceptingSession, allFixSessions.get(1));

            fixAdminSession = allFixSessions.get(2);
            assertFalse(fixAdminSession.isConnected());
            assertEquals(INITIATOR_ID3, fixAdminSession.sessionKey().remoteCompId());
            assertEquals(2, fixAdminSession.lastReceivedMsgSeqNum());
            assertEquals(2, fixAdminSession.lastSentMsgSeqNum());
        });
    }

    @Test(timeout = LONG_AWAIT_TIMEOUT_IN_MS)
    public void shouldDisconnectSession()
    {
        connectSessions();
        acquireAcceptingSession();
        messagesCanBeExchanged();

        testSystem.awaitLongBlocking(() ->
        {
            launchArtioAdmin();

            artioAdmin.disconnectSession(acceptingSession.id());
        });

        assertSessionsDisconnected();
    }

    @Test(timeout = LONG_AWAIT_TIMEOUT_IN_MS)
    public void shouldThrowWhenAttemptingToDisconnectUnknownSession()
    {
        testSystem.awaitLongBlocking(() ->
        {
            launchArtioAdmin();

            assertThrows(
                () -> artioAdmin.disconnectSession(-1337L),
                FixGatewayException.class,
                containsString("unknown"));
        });
    }

    @Test(timeout = LONG_AWAIT_TIMEOUT_IN_MS)
    public void shouldThrowWhenAttemptingToDisconnectOfflineSession()
    {
        createOfflineSession();

        testSystem.awaitLongBlocking(() ->
        {
            launchArtioAdmin();

            assertThrows(
                () -> artioAdmin.disconnectSession(1),
                FixGatewayException.class,
                containsString("not currently authenticated"));
        });
    }

    @Test(timeout = LONG_AWAIT_TIMEOUT_IN_MS)
    public void shouldResetSequenceNumbersOfGatewayManagedSession()
    {
        connectSessions();
        messagesCanBeExchanged();
        messagesCanBeExchanged();
        assertInitSeqNum(3, 3, 0);

        testSystem.awaitLongBlocking(() ->
        {
            launchArtioAdmin();

            artioAdmin.resetSequenceNumbers(1);
        });

        awaitInitSequenceReset();
        messagesCanBeExchanged();
    }

    @Test(timeout = LONG_AWAIT_TIMEOUT_IN_MS)
    public void shouldResetSequenceNumbersOfLibraryManagedSession()
    {
        connectSessions();
        acquireAcceptingSession();
        messagesCanBeExchanged();
        messagesCanBeExchanged();
        assertInitSeqNum(3, 3, 0);
        assertAccSeqNum(3, 3, 0);

        testSystem.awaitLongBlocking(() ->
        {
            launchArtioAdmin();

            artioAdmin.resetSequenceNumbers(1);
        });

        awaitInitSequenceReset();
        awaitSequenceReset(acceptingSession);
        messagesCanBeExchanged();
    }

    @Test(timeout = LONG_AWAIT_TIMEOUT_IN_MS)
    public void shouldResetSequenceNumbersOfOfflineSession()
    {
        createOfflineSession();

        testSystem.awaitLongBlocking(() ->
        {
            launchArtioAdmin();

            artioAdmin.resetSequenceNumbers(1);

            final List<FixAdminSession> fixAdminSessions = artioAdmin.allFixSessions();
            assertThat(fixAdminSessions, hasSize(1));

            final FixAdminSession fixAdminSession = fixAdminSessions.get(0);
            assertFalse(fixAdminSession.isConnected());
            assertEquals(0, fixAdminSession.lastReceivedMsgSeqNum());
            assertEquals(0, fixAdminSession.lastSentMsgSeqNum());
        });
    }

    @Test(timeout = LONG_AWAIT_TIMEOUT_IN_MS)
    public void shouldThrowWhenAttemptingToResetSequenceNumbersOfUnknownSession()
    {
        testSystem.awaitLongBlocking(() ->
        {
            launchArtioAdmin();

            assertThrows(
                () -> artioAdmin.resetSequenceNumbers(-1337L),
                FixGatewayException.class,
                containsString("unknown"));
        });
    }

    @Test
    public void shouldThrowExceptionIfAdminClientFailsToConnectToTheFixEngine()
    {
        final ArtioAdminConfiguration config = new ArtioAdminConfiguration()
            .inboundAdminStream(1010101010)
            .outboundAdminStream(2020202020)
            .aeronChannel("aeron:ipc")
            .connectTimeoutNs(123);

        assertThrows(
            () -> ArtioAdmin.launch(config),
            TimeoutException.class,
            equalTo("ERROR - Failed to connect to FixEngine using channel=aeron:ipc " +
            "outboundAdminStreamId=2020202020 inboundAdminStreamId=1010101010 subscription.isConnected=false " +
            "publication.isConnected=false after 123 ns"));
    }

    private void createOfflineSession()
    {
        connectSessions();
        messagesCanBeExchanged();
        messagesCanBeExchanged();
        assertInitSeqNum(3, 3, 0);
        logoutInitiatingSession();
        assertSessionDisconnected(initiatingSession);
    }

    private void awaitInitSequenceReset()
    {
        awaitSequenceReset(initiatingSession);
    }

    private void awaitSequenceReset(final Session session)
    {
        testSystem.await(
            "Sequence never reset",
            () -> session.lastReceivedMsgSeqNum() == 1 && this.initiatingSession.lastSentMsgSeqNum() == 1);
    }

    private void assertSessionEquals(final Session session, final FixAdminSession adminSession)
    {
        assertEquals(session.connectionId(), adminSession.connectionId());
        assertEquals(session.connectedHost(), adminSession.connectedHost());
        assertEquals(session.connectedPort(), adminSession.connectedPort());
        assertEquals(session.id(), adminSession.sessionId());
        assertEquals(session.compositeKey().toString(), adminSession.sessionKey().toString());
        connectTimeRange.assertWithinRange(adminSession.lastLogonTime());
        assertEquals(session.lastReceivedMsgSeqNum(), adminSession.lastReceivedMsgSeqNum(), 1);
        assertEquals(session.lastSentMsgSeqNum(), adminSession.lastSentMsgSeqNum(), 1);
        assertTrue(adminSession.isConnected());
        assertFalse(adminSession.isSlow());
    }

    private void launchArtioAdmin()
    {
        final ArtioAdminConfiguration config = new ArtioAdminConfiguration();
        config.aeronChannel(acceptingEngine.configuration().libraryAeronChannel());
        config.replyTimeoutInNs(MINUTES.toNanos(2));
        artioAdmin = ArtioAdmin.launch(config);
    }
}
