/*
 * Copyright 2015-2019 Real Logic Ltd, Adaptive Financial Consulting Ltd.
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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.artio.Constants;
import uk.co.real_logic.artio.Reply;
import uk.co.real_logic.artio.TestFixtures;
import uk.co.real_logic.artio.Timing;
import uk.co.real_logic.artio.builder.ResendRequestEncoder;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.library.DynamicLibraryScheduler;
import uk.co.real_logic.artio.library.SessionConfiguration;
import uk.co.real_logic.artio.messages.SessionReplyStatus;
import uk.co.real_logic.artio.session.Session;

import java.io.File;
import java.io.IOException;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.*;
import static uk.co.real_logic.artio.Constants.*;
import static uk.co.real_logic.artio.TestFixtures.launchMediaDriver;
import static uk.co.real_logic.artio.TestFixtures.mediaDriverContext;
import static uk.co.real_logic.artio.library.FixLibrary.NO_MESSAGE_REPLAY;
import static uk.co.real_logic.artio.library.SessionConfiguration.AUTOMATIC_INITIAL_SEQUENCE_NUMBER;
import static uk.co.real_logic.artio.messages.SessionReplyStatus.OK;
import static uk.co.real_logic.artio.system_tests.FixMessage.hasMessageSequenceNumber;
import static uk.co.real_logic.artio.system_tests.FixMessage.hasSequenceIndex;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.*;
import static uk.co.real_logic.artio.validation.PersistenceLevel.INDEXED;

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
        assertEquals("Repy failed: " + reply, Reply.State.COMPLETED, reply.state());
        initiatingSession = reply.resultIfPresent();
        assertConnected(initiatingSession);
    };

    private Runnable duringRestart = () -> dirsDeleteOnStart = false;
    private Runnable beforeReconnect = this::nothing;
    private boolean printErrorMessages = true;
    private boolean resetSequenceNumbersOnLogon = false;
    private boolean dirsDeleteOnStart = true;

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

    @Test(timeout = TEST_TIMEOUT)
    public void shouldCopeWithCatchupReplayOfMissingMessages()
    {
        printErrorMessages = false;

        duringRestart = this::deleteAcceptorLogs;

        onAcquireSession = () -> assertReplyStatusWhenReplayRequested(OK);

        resetSequenceNumbersOnLogon = true;

        exchangeMessagesAroundARestart(AUTOMATIC_INITIAL_SEQUENCE_NUMBER, DOES_NOT_MATTER);

        assertOnlyAcceptorSequenceReset();
    }

    @Test(timeout = TEST_TIMEOUT)
    public void shouldCopeWithCatchupReplayOfMissingMessagesWithHighInitialSequenceNumberSet()
    {
        final int highInitialSequenceNumber = 1000;

        exchangeMessagesAroundARestart(
            highInitialSequenceNumber,
            4,
            highInitialSequenceNumber,
            5);

        final FixMessage gapFillMessage =
            testSystem.awaitMessageOf(acceptingOtfAcceptor, SEQUENCE_RESET_MESSAGE_AS_STR);
        final int newSeqNo = Integer.valueOf(gapFillMessage.get(Constants.NEW_SEQ_NO));
        final String gapFillFlag = gapFillMessage.get(Constants.GAP_FILL_FLAG);

        assertEquals("Y", gapFillFlag);
        assertThat(newSeqNo, greaterThan(highInitialSequenceNumber));

        Timing.assertEventuallyTrue("", () -> testSystem.poll(), 100);
        assertEquals(1, initiatingOtfAcceptor.hasReceivedMessage(RESEND_REQUEST_MESSAGE_AS_STR).count());
    }

    @Test(timeout = TEST_TIMEOUT)
    public void messagesCanBeReplayedOverRestart()
    {
        exchangeMessagesAroundARestart(AUTOMATIC_INITIAL_SEQUENCE_NUMBER, DEFAULT_SEQ_NUM_AFTER);

        final ResendRequestEncoder resendRequest = new ResendRequestEncoder();
        resendRequest.beginSeqNo(1).endSeqNo(1);

        initiatingOtfAcceptor.messages().clear();

        testSystem.send(initiatingSession, resendRequest);

        final FixMessage message = testSystem.awaitMessageOf(initiatingOtfAcceptor, SEQUENCE_RESET_MESSAGE_AS_STR);

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

        exchangeMessagesAroundARestart(AUTOMATIC_INITIAL_SEQUENCE_NUMBER, 1);

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

    @Test(timeout = TEST_TIMEOUT)
    public void shouldReceiveRelevantLogoutErrorTextDuringConnect()
    {
        onInitiateReply = reply ->
        {
            initiatingSession = reply.resultIfPresent();
            assertTrue("reply did not end in an error", reply.hasErrored());
        };

        onAcquireSession = this::nothing;

        // connect but fail to logon because the initial sequence number is invalid.
        launch(this::nothing);
        connectPersistingSessions(0, false);

        // No session established due to error during logon
        assertNull(initiatingSession);

        // In this case we just get immediately disconnected.
        final FixMessage lastMessage = testSystem.awaitMessageOf(initiatingOtfAcceptor, LOGOUT_MESSAGE_AS_STR);

        assertEquals(LOGOUT_MESSAGE_AS_STR, lastMessage.msgType());
        assertEquals(1, lastMessage.messageSequenceNumber());
        assertEquals("MsgSeqNum too low, expecting 1 but received 0", lastMessage.get(Constants.TEXT));
    }

    @Test
    public void shouldPersistSequenceNumbersWithoutARestart()
    {
        launch(this::nothing);
        connectPersistingSessions(AUTOMATIC_INITIAL_SEQUENCE_NUMBER, false);

        assertSequenceIndicesAre(0);

        assertTestRequestSentAndReceived(initiatingSession, testSystem, acceptingOtfAcceptor);
        assertSequenceFromInitToAcceptAt(2, 2);

        final long initiatedSessionId = initiatingSession.id();
        final long acceptingSessionId = acceptingSession.id();

        assertThat(initiatingSession.startLogout(), greaterThan(0L));
        assertSessionsDisconnected();
        assertInitiatingSequenceIndexIs(0);
        assertAcceptingSessionHasSequenceIndex(0);

        clearMessages();
        initiatingSession = null;
        acceptingSession = null;

        connectPersistingSessions(AUTOMATIC_INITIAL_SEQUENCE_NUMBER, false);

        assertEquals(initiatedSessionId, initiatingSession.id());
        assertEquals(acceptingSessionId, acceptingSession.id());

        // Sequence numbers not reset, so the same sequence index
        assertInitiatingSequenceIndexIs(0);
        assertAcceptingSessionHasSequenceIndex(0);
        messagesCanBeExchanged();

        // 5: logon, test-req/heartbeat, logout. logon 2, test-req/heartbeat 2
        assertSequenceFromInitToAcceptAt(5, 5);
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

    private void launch(final Runnable beforeConnect)
    {
        mediaDriver = launchMediaDriver(mediaDriverContext(
            TestFixtures.TERM_BUFFER_LENGTH,
            dirsDeleteOnStart));

        final EngineConfiguration config = acceptingConfig(port, ACCEPTOR_ID, INITIATOR_ID);
        config.sessionPersistenceStrategy(logon -> INDEXED);
        config.printErrorMessages(printErrorMessages);
        acceptingEngine = FixEngine.launch(config);
        final EngineConfiguration initiatingConfig = initiatingConfig(libraryAeronPort);
        initiatingConfig.printErrorMessages(printErrorMessages);
        initiatingEngine = FixEngine.launch(initiatingConfig);

        // Use so that the SharedLibraryScheduler is integration tested
        final DynamicLibraryScheduler libraryScheduler = new DynamicLibraryScheduler();

        acceptingLibrary = connect(acceptingLibraryConfig(acceptingHandler).scheduler(libraryScheduler));

        initiatingLibrary = connect(initiatingLibraryConfig(libraryAeronPort, initiatingHandler)
            .scheduler(libraryScheduler));

        testSystem = new TestSystem(acceptingLibrary, initiatingLibrary);

        beforeConnect.run();
    }

    private void connectPersistingSessions(final int initialSequenceNumber, final boolean resetSeqNum)
    {
        this.connectPersistingSessions(initialSequenceNumber, initialSequenceNumber, resetSeqNum);
    }

    private void connectPersistingSessions(
        final int initiatorInitialSentSequenceNumber,
        final int initiatorInitialReceivedSequenceNumber,
        final boolean resetSeqNum)
    {
        final SessionConfiguration config = SessionConfiguration.builder()
            .address("localhost", port)
            .credentials("bob", "Uv1aegoh")
            .senderCompId(INITIATOR_ID)
            .targetCompId(ACCEPTOR_ID)
            .sequenceNumbersPersistent(true)
            .initialReceivedSequenceNumber(initiatorInitialReceivedSequenceNumber)
            .initialSentSequenceNumber(initiatorInitialSentSequenceNumber)
            .resetSeqNum(resetSeqNum)
            .build();

        final Reply<Session> reply = initiatingLibrary.initiate(config);
        testSystem.awaitReply(reply);

        onInitiateReply.accept(reply);

        onAcquireSession.run();
    }

    private void exchangeMessagesAroundARestart(final int initialSequenceNumber, final int seqNumAfter)
    {
        exchangeMessagesAroundARestart(initialSequenceNumber, initialSequenceNumber, seqNumAfter, seqNumAfter);
    }

    private void exchangeMessagesAroundARestart(
        final int initialSentSequenceNumber,
        final int initialReceivedSequenceNumber,
        final int expectedInitToAccSeqNum,
        final int expectedAccToInitSeqNum)
    {
        launch(this::nothing);
        connectPersistingSessions(AUTOMATIC_INITIAL_SEQUENCE_NUMBER, resetSequenceNumbersOnLogon);

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

        launch(this.beforeReconnect);
        connectPersistingSessions(
            initialSentSequenceNumber,
            initialReceivedSequenceNumber,
            resetSequenceNumbersOnLogon);

        assertEquals("initiatedSessionId not stable over restarts", initiatedSessionId, initiatingSession.id());
        assertEquals("acceptingSessionId not stable over restarts", acceptingSessionId, acceptingSession.id());
        if (expectedInitToAccSeqNum != DOES_NOT_MATTER)
        {
            assertSequenceFromInitToAcceptAt(expectedInitToAccSeqNum, expectedAccToInitSeqNum);
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

    private void assertReplyStatusWhenReplayRequested(final SessionReplyStatus replyStatus)
    {
        final long sessionId = getAcceptingSessionId();

        if (acceptingSession != null)
        {
            // Require replay of at least one message that has been sent
            final int lastReceivedMsgSeqNum = acceptingSession.lastReceivedMsgSeqNum() - 2;
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

}
