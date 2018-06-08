/*
 * Copyright 2015-2017 Real Logic Ltd.
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
package uk.co.real_logic.artio.system_tests;

import org.agrona.IoUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import uk.co.real_logic.artio.Reply;
import uk.co.real_logic.artio.builder.ResendRequestEncoder;
import uk.co.real_logic.artio.Constants;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.library.DynamicLibraryScheduler;
import uk.co.real_logic.artio.library.SessionConfiguration;
import uk.co.real_logic.artio.messages.SessionReplyStatus;
import uk.co.real_logic.artio.session.Session;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.function.Consumer;

import static org.junit.Assert.*;
import static uk.co.real_logic.artio.TestFixtures.launchMediaDriver;
import static uk.co.real_logic.artio.Timing.*;
import static uk.co.real_logic.artio.Constants.SEQUENCE_RESET_MESSAGE_AS_STR;
import static uk.co.real_logic.artio.library.FixLibrary.NO_MESSAGE_REPLAY;
import static uk.co.real_logic.artio.library.SessionConfiguration.AUTOMATIC_INITIAL_SEQUENCE_NUMBER;
import static uk.co.real_logic.artio.messages.SessionReplyStatus.MISSING_MESSAGES;
import static uk.co.real_logic.artio.messages.SessionReplyStatus.SEQUENCE_NUMBER_TOO_HIGH;
import static uk.co.real_logic.artio.system_tests.FixMessage.hasMessageSequenceNumber;
import static uk.co.real_logic.artio.system_tests.FixMessage.hasSequenceIndex;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.*;
import static uk.co.real_logic.artio.validation.PersistenceLevel.REPLICATED;

public class PersistentSequenceNumberGatewayToGatewaySystemTest extends AbstractGatewayToGatewaySystemTest
{
    private static final long TEST_TIMEOUT = 10_000L;
    private static final int DOES_NOT_MATTER = -1;
    private static final int DEFAULT_SEQ_NUM_AFTER = 4;

    private File backupLocation = null;

    private Runnable onAcquireSession = () ->
    {
        final long sessionId = getAcceptingSessionId();

        acquireSession(sessionId, NO_MESSAGE_REPLAY, NO_MESSAGE_REPLAY);
    };

    private Consumer<Reply<Session>> onInitiateReply = reply ->
    {
        initiatingSession = reply.resultIfPresent();
        assertConnected(initiatingSession);
        sessionLogsOn(testSystem, initiatingSession, DEFAULT_TIMEOUT_IN_MS);
    };

    private Runnable duringRestart = this::nothing;
    private Runnable beforeReconnect = this::nothing;
    private boolean printErrorMessages = true;
    private boolean resetSequenceNumbersOnLogon = false;

    @Before
    public void setUp() throws IOException
    {
        deleteAcceptorLogs();
        delete(CLIENT_LOGS);
        backupLocation = File.createTempFile("backup", "tmp");
    }

    @After
    public void cleanupBackup()
    {
        if (null != backupLocation)
        {
            assertTrue("Failed to delete: " + backupLocation, backupLocation.delete());
        }
    }

    @Test(timeout = TEST_TIMEOUT)
    public void sequenceNumbersCanPersistOverRestarts()
    {
        exchangeMessagesAroundARestart(AUTOMATIC_INITIAL_SEQUENCE_NUMBER, DEFAULT_SEQ_NUM_AFTER);

        assertSequenceIndicesAre(0);
    }

    @Test(timeout = TEST_TIMEOUT)
    public void previousMessagesAreReplayed()
    {
        onAcquireSession = this::requestReplayWhenReacquiringSession;

        exchangeMessagesAroundARestart(AUTOMATIC_INITIAL_SEQUENCE_NUMBER, DEFAULT_SEQ_NUM_AFTER);

        assertSequenceIndicesAre(0);
    }

    // Replicate a bug that Nick Reported
    @Test(timeout = TEST_TIMEOUT)
    public void shouldCopeWithReplayOfMissingMessages()
    {
        duringRestart = this::deleteAcceptorLogs;

        onAcquireSession = () -> assertFailStatusWhenReplayRequested(SEQUENCE_NUMBER_TOO_HIGH);

        resetSequenceNumbersOnLogon = true;

        exchangeMessagesAroundARestart(AUTOMATIC_INITIAL_SEQUENCE_NUMBER, DOES_NOT_MATTER);

        assertOnlyAcceptorSequenceReset();
    }

    @Test(timeout = TEST_TIMEOUT)
    public void shouldCopeWithReplayOfMissingMessagesAfterPartialCleanout()
    {
        // Avoid spamming the build log with error missing messages error message
        printErrorMessages = false;

        duringRestart = this::deleteArchiveOfAcceptorLogs;

        onAcquireSession = () -> assertFailStatusWhenReplayRequested(MISSING_MESSAGES);

        resetSequenceNumbersOnLogon = true;

        exchangeMessagesAroundARestart(AUTOMATIC_INITIAL_SEQUENCE_NUMBER, DOES_NOT_MATTER);

        assertSequenceIndicesAre(1);
    }

    @Test(timeout = TEST_TIMEOUT)
    public void messagesCanBeReplayedOverRestart()
    {
        exchangeMessagesAroundARestart(AUTOMATIC_INITIAL_SEQUENCE_NUMBER, DEFAULT_SEQ_NUM_AFTER);

        final ResendRequestEncoder resendRequest = new ResendRequestEncoder();
        resendRequest.beginSeqNo(1).endSeqNo(1);

        initiatingOtfAcceptor.messages().clear();

        assertEventuallyTrue(
            "Unable to send resend request",
            () ->
            {
                testSystem.poll();
                return initiatingSession.send(resendRequest) > 0;
            });

        final FixMessage message = withTimeout(
            "Failed to receive reply",
            () ->
            {
                testSystem.poll();
                return initiatingOtfAcceptor.hasReceivedMessage(SEQUENCE_RESET_MESSAGE_AS_STR).findFirst();
            },
            2_000);

        assertEquals(message.get(Constants.MSG_SEQ_NUM), "1");
        assertEquals(message.get(Constants.SENDER_COMP_ID), ACCEPTOR_ID);
        assertEquals(message.get(Constants.TARGET_COMP_ID), INITIATOR_ID);

        assertSequenceIndicesAre(0);
    }

    @Test(timeout = TEST_TIMEOUT)
    public void customInitialSequenceNumbersCanBeSet()
    {
        exchangeMessagesAroundARestart(4, DEFAULT_SEQ_NUM_AFTER);

        assertSequenceIndicesAre(0);
    }

    @Test(timeout = TEST_TIMEOUT)
    public void sessionsCanBeReset()
    {
        beforeReconnect = this::resetSessions;

        exchangeMessagesAroundARestart(AUTOMATIC_INITIAL_SEQUENCE_NUMBER, 1);

        // Different sessions themselves, so we start again at 0
        assertSequenceIndicesAre(0);
    }

    @Test(timeout = TEST_TIMEOUT)
    public void sequenceNumbersCanBeResetWhileSessionDisconnected()
    {
        beforeReconnect = this::resetSequenceNumbers;

        exchangeMessagesAroundARestart(AUTOMATIC_INITIAL_SEQUENCE_NUMBER, 2);

        assertSequenceIndicesAre(1);
    }

    @Test(timeout = TEST_TIMEOUT)
    public void sequenceNumbersCanBeResetOnLogon()
    {
        resetSequenceNumbersOnLogon = true;

        exchangeMessagesAroundARestart(AUTOMATIC_INITIAL_SEQUENCE_NUMBER, 1);

        acceptingOtfAcceptor.logonMessagesHaveSequenceNumbers(1);
        initiatingOtfAcceptor.logonMessagesHaveSequenceNumbers(1);
        assertSequenceIndicesAre(1);
    }

    @Ignore
    @Test(timeout = TEST_TIMEOUT)
    public void shouldReceiveRelevantErrorsDuringConnect()
    {
        launch(AUTOMATIC_INITIAL_SEQUENCE_NUMBER, this::nothing, false);
    }

    private void resetSequenceNumbers()
    {
        testSystem.awaitCompletedReplies(
            initiatingEngine.resetSequenceNumber(initiatingSession.id()),
            acceptingEngine.resetSequenceNumber(acceptingSession.id()));
    }

    private void resetSessions()
    {
        testSystem.awaitCompletedReplies(
            acceptingEngine.resetSessionIds(backupLocation, ADMIN_IDLE_STRATEGY),
            initiatingEngine.resetSessionIds(backupLocation, ADMIN_IDLE_STRATEGY));
    }

    private void launch(
        final int initialSequenceNumber,
        final Runnable beforeConnect,
        final boolean resetSequenceNumbersOnLogon)
    {
        mediaDriver = launchMediaDriver();

        final EngineConfiguration config = acceptingConfig(port, ACCEPTOR_ID, INITIATOR_ID);
        config.sessionPersistenceStrategy(logon -> REPLICATED);
        config.printErrorMessages(printErrorMessages);
        acceptingEngine = FixEngine.launch(config);
        initiatingEngine = launchInitiatingEngineWithSameLogs(libraryAeronPort);

        // Use so that the SharedLibraryScheduler is integration tested
        final DynamicLibraryScheduler libraryScheduler = new DynamicLibraryScheduler();

        acceptingLibrary = connect(acceptingLibraryConfig(acceptingHandler).scheduler(libraryScheduler));

        initiatingLibrary = connect(initiatingLibraryConfig(libraryAeronPort, initiatingHandler)
            .scheduler(libraryScheduler));

        testSystem = new TestSystem(acceptingLibrary, initiatingLibrary);

        beforeConnect.run();

        connectPersistingSessions(initialSequenceNumber, resetSequenceNumbersOnLogon);
    }

    private void connectPersistingSessions(final int initialSequenceNumber, final boolean resetSeqNum)
    {
        final SessionConfiguration config = SessionConfiguration.builder()
            .address("localhost", port)
            .credentials("bob", "Uv1aegoh")
            .senderCompId(INITIATOR_ID)
            .targetCompId(ACCEPTOR_ID)
            .sequenceNumbersPersistent(true)
            .initialReceivedSequenceNumber(initialSequenceNumber)
            .initialSentSequenceNumber(initialSequenceNumber)
            .resetSeqNum(resetSeqNum)
            .build();

        final Reply<Session> reply = initiatingLibrary.initiate(config);
        awaitLibraryReply(initiatingLibrary, reply);

        onInitiateReply.accept(reply);

        onAcquireSession.run();
    }

    private void exchangeMessagesAroundARestart(final int initialSequenceNumber, final int seqNumAfter)
    {
        launch(AUTOMATIC_INITIAL_SEQUENCE_NUMBER, this::nothing, resetSequenceNumbersOnLogon);

        assertSequenceIndicesAre(0);

        assertTestRequestSentAndReceived(initiatingSession, testSystem, acceptingOtfAcceptor);
        assertSequenceFromInitToAcceptAt(2, 2);

        final long initiatedSessionId = initiatingSession.id();
        final long acceptingSessionId = acceptingSession.id();

        initiatingSession.startLogout();
        assertSessionsDisconnected();

        assertInitiatingSequenceIndexIs(0);
        clearMessages();
        close();

        duringRestart.run();

        launch(initialSequenceNumber, this.beforeReconnect, resetSequenceNumbersOnLogon);

        assertEquals("initiatedSessionId not stable over restarts", initiatedSessionId, initiatingSession.id());
        assertEquals("acceptingSessionId not stable over restarts", acceptingSessionId, acceptingSession.id());
        if (seqNumAfter != DOES_NOT_MATTER)
        {
            assertSequenceFromInitToAcceptAt(seqNumAfter, seqNumAfter);
        }

        assertTestRequestSentAndReceived(initiatingSession, testSystem, acceptingOtfAcceptor);
    }

    private void nothing()
    {
    }

    private long getAcceptingSessionId()
    {
        return acceptingHandler.awaitSessionId(testSystem::poll);
    }

    private void acquireSession(final long sessionId, final int lastReceivedMsgSeqNum, final int sequenceIndex)
    {
        acceptingSession = SystemTestUtil.acquireSession(
            acceptingHandler, acceptingLibrary, sessionId, testSystem, lastReceivedMsgSeqNum, sequenceIndex);
    }

    private void requestReplayWhenReacquiringSession()
    {
        final long sessionId = getAcceptingSessionId();

        if (acceptingSession != null)
        {
            final int lastReceivedMsgSeqNum = acceptingSession.lastReceivedMsgSeqNum();
            final int sequenceIndex = acceptingSession.sequenceIndex();
            acquireSession(sessionId, lastReceivedMsgSeqNum, sequenceIndex);

            final FixMessage firstReplayedMessage = acceptingOtfAcceptor.messages().get(0);
            assertThat(firstReplayedMessage, hasMessageSequenceNumber(lastReceivedMsgSeqNum));
            assertThat(firstReplayedMessage, hasSequenceIndex(sequenceIndex));
        }
        else
        {
            acquireSession(sessionId, NO_MESSAGE_REPLAY, NO_MESSAGE_REPLAY);
        }
    }

    private void assertOnlyAcceptorSequenceReset()
    {
        // Only delected the acceptor logs so they have different sequence indices.

        assertAcceptingSessionHasSequenceIndex(0);
        acceptingOtfAcceptor.allMessagesHaveSequenceIndex(0);
        assertInitiatingSequenceIndexIs(1);
        initiatingOtfAcceptor.allMessagesHaveSequenceIndex(1);
    }

    private void assertFailStatusWhenReplayRequested(final SessionReplyStatus replyStatus)
    {
        final long sessionId = getAcceptingSessionId();

        if (acceptingSession != null)
        {
            // Require replay of at least one message that has been sent
            final int lastReceivedMsgSeqNum = acceptingSession.lastReceivedMsgSeqNum() - 1;
            final int sequenceIndex = acceptingSession.sequenceIndex();
            final SessionReplyStatus reply = requestSession(
                acceptingLibrary, sessionId, lastReceivedMsgSeqNum, sequenceIndex, testSystem);
            assertEquals(replyStatus, reply);

            acceptingSession = acceptingHandler.lastSession();
            acceptingHandler.resetSession();
        }
        else
        {
            acquireSession(sessionId, NO_MESSAGE_REPLAY, NO_MESSAGE_REPLAY);
        }
    }

    private void deleteAcceptorLogs()
    {
        delete(ACCEPTOR_LOGS);
    }

    private void deleteArchiveOfAcceptorLogs()
    {
        final File dir = new File(ACCEPTOR_LOGS);
        if (dir.exists())
        {
            Arrays.stream(Objects.requireNonNull(dir.list()))
                .filter((name) -> name.contains("archive"))
                .forEach((name) -> IoUtil.delete(new File(dir, name), false));
        }
    }
}
