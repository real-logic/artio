/*
 * Copyright 2015-2020 Real Logic Limited, Adaptive Financial Consulting Ltd., Monotonic Ltd.
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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.artio.*;
import uk.co.real_logic.artio.builder.ResendRequestEncoder;
import uk.co.real_logic.artio.builder.TestRequestEncoder;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.library.DynamicLibraryScheduler;
import uk.co.real_logic.artio.messages.SessionReplyStatus;
import uk.co.real_logic.artio.session.Session;

import java.io.File;
import java.io.IOException;
import java.util.function.Consumer;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.Assert.*;
import static uk.co.real_logic.artio.Constants.*;
import static uk.co.real_logic.artio.Reply.State.ERRORED;
import static uk.co.real_logic.artio.TestFixtures.launchMediaDriver;
import static uk.co.real_logic.artio.TestFixtures.mediaDriverContext;
import static uk.co.real_logic.artio.library.FixLibrary.NO_MESSAGE_REPLAY;
import static uk.co.real_logic.artio.library.SessionConfiguration.AUTOMATIC_INITIAL_SEQUENCE_NUMBER;
import static uk.co.real_logic.artio.messages.SessionReplyStatus.OK;
import static uk.co.real_logic.artio.system_tests.FixMessage.hasMessageSequenceNumber;
import static uk.co.real_logic.artio.system_tests.FixMessage.hasSequenceIndex;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.*;
import static uk.co.real_logic.artio.validation.SessionPersistenceStrategy.alwaysPersistent;

public class PersistentSequenceNumberGatewayToGatewaySystemTest extends AbstractGatewayToGatewaySystemTest
{
    private static final int HIGH_INITIAL_SEQUENCE_NUMBER = 1000;
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
        assertEquals("Reply failed: " + reply, Reply.State.COMPLETED, reply.state());
        initiatingSession = reply.resultIfPresent();
        assertConnected(initiatingSession);
    };

    private final ErrorCounter errorCounter = new ErrorCounter();
    private Runnable duringRestart = () -> dirsDeleteOnStart = false;
    private Runnable beforeReconnect = this::nothing;
    private boolean printErrorMessages = true;
    private boolean resetSequenceNumbersOnLogon = false;
    private boolean dirsDeleteOnStart = true;

    private TimeRange firstConnectTimeRange;

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
        assertLastLogonEquals(4, 0);

        assertSequenceResetBeforeLastLogon(initiatingSession);
        assertSequenceResetBeforeLastLogon(acceptingSession);
    }

    @Test(timeout = TEST_TIMEOUT)
    public void previousMessagesAreReplayed()
    {
        onAcquireSession = this::requestReplayWhenReacquiringSession;

        exchangeMessagesAroundARestart(AUTOMATIC_INITIAL_SEQUENCE_NUMBER, DEFAULT_SEQ_NUM_AFTER);

        assertSequenceIndicesAre(0);
        assertLastLogonEquals(4, 0);
    }

    @Test(timeout = TEST_TIMEOUT)
    public void shouldCopeWithCatchupReplayOfMissingMessages()
    {
        duringRestart = () ->
        {
            dirsDeleteOnStart = false;
            deleteAcceptorLogs();
        };

        onAcquireSession = () -> assertReplyStatusWhenReplayRequested(OK);

        resetSequenceNumbersOnLogon = true;

        exchangeMessagesAroundARestart(AUTOMATIC_INITIAL_SEQUENCE_NUMBER, DOES_NOT_MATTER);

        assertOnlyAcceptorSequenceReset();
        assertLastLogonEquals(1, 0);
    }

    @Test(timeout = TEST_TIMEOUT)
    public void shouldCopeWithResendRequestOfMissingMessagesWithHighInitialSequenceNumberSet()
    {
        exchangeMessagesAroundARestart(
            HIGH_INITIAL_SEQUENCE_NUMBER,
            4,
            HIGH_INITIAL_SEQUENCE_NUMBER,
            5);

        final FixMessage gapFillMessage =
            testSystem.awaitMessageOf(acceptingOtfAcceptor, SEQUENCE_RESET_MESSAGE_AS_STR);
        final int newSeqNo = Integer.parseInt(gapFillMessage.get(Constants.NEW_SEQ_NO));
        final String gapFillFlag = gapFillMessage.get(Constants.GAP_FILL_FLAG);

        assertEquals("Y", gapFillFlag);
        assertThat(newSeqNo, greaterThan(HIGH_INITIAL_SEQUENCE_NUMBER));

        // Test that we don't accidentally send another resend request
        // Reproduction of reported bug
        Timing.assertEventuallyTrue("", () -> testSystem.poll(), 100);
        assertEquals(1, initiatingOtfAcceptor.receivedMessage(RESEND_REQUEST_MESSAGE_AS_STR).count());
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
        assertLastLogonEquals(4, 0);
    }

    @Test(timeout = TEST_TIMEOUT)
    public void customInitialSequenceNumbersCanBeSet()
    {
        exchangeMessagesAroundARestart(4, DEFAULT_SEQ_NUM_AFTER);

        assertSequenceIndicesAre(0);
        assertLastLogonEquals(4, 0);

        assertSequenceResetBeforeLastLogon(initiatingSession);
        assertSequenceResetBeforeLastLogon(acceptingSession);
    }

    @Test(timeout = TEST_TIMEOUT)
    public void sessionsCanBeReset()
    {
        beforeReconnect = this::resetSessions;

        exchangeMessagesAroundARestart(AUTOMATIC_INITIAL_SEQUENCE_NUMBER, 1);

        // Different sessions themselves, so we start again at 0
        assertSequenceIndicesAre(0);
        assertLastLogonEquals(1, 0);

        assertSequenceResetTimeAtLatestLogon(initiatingSession);
        assertSequenceResetTimeAtLatestLogon(acceptingSession);
    }

    @Test(timeout = TEST_TIMEOUT)
    public void sequenceNumbersCanBeResetWhileSessionDisconnected()
    {
        beforeReconnect = this::resetSequenceNumbers;

        exchangeMessagesAroundARestart(AUTOMATIC_INITIAL_SEQUENCE_NUMBER, 1);

        assertSequenceIndicesAre(1);
        assertLastLogonEquals(1, 1);

        assertSequenceResetTimeAtLatestLogon(initiatingSession);
        assertSequenceResetTimeAtLatestLogon(acceptingSession);
    }

    @Test(timeout = TEST_TIMEOUT)
    public void sequenceNumbersCanBeResetOnLogon()
    {
        resetSequenceNumbersOnLogon = true;

        exchangeMessagesAroundARestart(AUTOMATIC_INITIAL_SEQUENCE_NUMBER, 1);

        acceptingOtfAcceptor.logonMessagesHaveSequenceNumbers(1);
        initiatingOtfAcceptor.logonMessagesHaveSequenceNumbers(1);
        assertSequenceIndicesAre(1);
        assertLastLogonEquals(1, 1);

        assertSequenceResetTimeAtLatestLogon(initiatingSession);
        assertSequenceResetTimeAtLatestLogon(acceptingSession);
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
        connectPersistingSessions();

        assertSequenceIndicesAre(0);

        assertTestRequestSentAndReceived(initiatingSession, testSystem, acceptingOtfAcceptor);
        assertSequenceFromInitToAcceptAt(2, 2);

        final long initiatedSessionId = initiatingSession.id();
        final long acceptingSessionId = acceptingSession.id();

        logoutInitiatingSession();
        assertSessionsDisconnected();
        assertInitiatingSequenceIndexIs(0);
        assertAcceptingSessionHasSequenceIndex(0);

        clearMessages();
        initiatingSession = null;
        acceptingSession = null;

        connectPersistingSessions();

        assertEquals(initiatedSessionId, initiatingSession.id());
        assertEquals(acceptingSessionId, acceptingSession.id());

        // Sequence numbers not reset, so the same sequence index
        assertInitiatingSequenceIndexIs(0);
        assertAcceptingSessionHasSequenceIndex(0);
        messagesCanBeExchanged();

        // 5: logon, test-req/heartbeat, logout. logon 2, test-req/heartbeat 2
        assertSequenceFromInitToAcceptAt(5, 5);
        assertLastLogonEquals(4, 0);
    }

    @Test
    public void shouldNotAllowResendRequestSpamming()
    {
        printErrorMessages = false;

        launch(this::nothing);

        final ReplayCountChecker replayCountChecker =
            ReplayCountChecker.start(acceptingEngine, testSystem, 1);

        connectPersistingSessions();
        // Send execution report with seqNum=1
        final ReportFactory reportFactory = new ReportFactory();
        final int messageCount = 100;
        for (int i = 0; i < messageCount; i++)
        {
            assertEquals(CONTINUE, reportFactory.sendReport(acceptingSession, Side.BUY));
        }

        final int lastSeqNum = messageCount + 1;
        assertSequenceFromInitToAcceptAt(1, lastSeqNum);

        logoutInitiatingSession();
        assertSessionsDisconnected();

        clearMessages();
        initiatingSession = null;
        acceptingSession = null;

        connectPersistingSessions();

        final int replayRequests = 1000;
        for (int i = 0; i < replayRequests; i++)
        {
            sendResendRequest(2, 0, initiatingOtfAcceptor, initiatingSession);
        }

        testSystem.awaitSend("Failed to requestDisconnect", () -> initiatingSession.requestDisconnect());
        assertSessionDisconnected(acceptingSession);

        testSystem.removeOperation(replayCountChecker);
        replayCountChecker.assertBelowThreshold();

        // Give the system some time to process and suppress resend requests.
        testSystem.awaitBlocking(() ->
        {
            try
            {
                Thread.sleep(200);
            }
            catch (final InterruptedException e)
            {
                e.printStackTrace();
            }
        });

        assertThat(errorCounter.lastObservationCount(), lessThan(messageCount * 5));
    }

    @Test
    public void shouldRejectIncorrectInitiatorSequenceNumber()
    {
        printErrorMessages = false;

        launch(this::nothing);
        connectPersistingSessions();

        assertTestRequestSentAndReceived(initiatingSession, testSystem, acceptingOtfAcceptor);
        assertSequenceFromInitToAcceptAt(2, 2);

        logoutInitiatingSession();
        assertSessionsDisconnected();

        final int initiatorSequenceNumber = initiatingSession.lastSentMsgSeqNum() + 1;
        int acceptorSequenceNumber = acceptingSession.lastSentMsgSeqNum() + 1;

        assertTrue(acceptingOtfAcceptor.messages().get(0).isValid());

        clearMessages();
        initiatingSession = null;
        acceptingSession = null;

        // Replication of bug #295
        cannotConnectWithSequence(acceptorSequenceNumber, 1);
        cannotConnectWithSequence(acceptorSequenceNumber, 2);
        acceptorSequenceNumber += 2;

        final Reply<Session> okReply = connectPersistentSessions(
            initiatorSequenceNumber, acceptorSequenceNumber, false);
        assertEquals(Reply.State.COMPLETED, okReply.state());
        initiatingSession = okReply.resultIfPresent();
        acceptorSequenceNumber++;

        // An old message with a possDupFlag=Y set shouldn't break sequence number indices
        final int oldSequenceNumber = initiatingSession.lastSentMsgSeqNum() - 2;
        initiatingSession.lastSentMsgSeqNum(oldSequenceNumber);
        final TestRequestEncoder testRequest = new TestRequestEncoder().testReqID(testReqId());
        testRequest.header().possDupFlag(true);
        testSystem.send(initiatingSession, testRequest);

        initiatingSession.requestDisconnect();
        assertSessionDisconnected(initiatingSession);
        sessionNoLongerManaged(initiatingHandler, initiatingSession);

        cannotConnectWithSequence(acceptorSequenceNumber, oldSequenceNumber + 1);

        connectPersistingSessions();
    }

    private void cannotConnectWithSequence(
        final int acceptorSequenceNumber, final int initiatorInitialSentSequenceNumber)
    {
        final Reply<Session> reply = connectPersistentSessions(
            initiatorInitialSentSequenceNumber, acceptorSequenceNumber, false);
        assertEquals(ERRORED, reply.state());
    }

    @Test
    public void shouldReadOldMetaDataOverPersistentConnectionReconnect()
    {
        launch(this::nothing);
        connectPersistingSessions();

        writeMetaData();

        logoutInitiatingSession();
        assertSessionsDisconnected();

        connectPersistingSessions();

        readMetaData(acceptingSession.id());
    }

    @Test
    public void shouldStoreAndForwardMessagesSentWhilstOffline()
    {
        launch(this::nothing);
        connectPersistingSessions();

        disconnectSessions();

        final long sessionId = acceptingSession.id();

        clearMessages();
        initiatingSession = null;
        acceptingSession = null;

        acquireAcceptingSession();
        assertOfflineSession(sessionId, acceptingSession);

        // Send a test execution report offline that can be replayed
        final ReportFactory reportFactory = new ReportFactory();
        assertEquals(CONTINUE, reportFactory.sendReport(acceptingSession, Side.BUY));

        onAcquireSession = this::nothing;
        connectPersistingSessions();

        final FixMessage executionReport = testSystem.awaitMessageOf(
            initiatingOtfAcceptor, EXECUTION_REPORT_MESSAGE_AS_STR);
        assertEquals(ReportFactory.MSFT, executionReport.get(SYMBOL));
        assertEquals("Y", executionReport.possDup());
    }

    @Test
    public void shouldResetSequenceNumbersOfOfflineSessions()
    {
        printErrorMessages = false;

        launch(this::nothing);
        connectPersistingSessions();
        assertEquals(0, acceptingSession.sequenceIndex());
        disconnectSessions();
        clearMessages();

        acquireAcceptingSession();
        assertOfflineSession(acceptingSession.id(), acceptingSession);
        assertEquals(0, acceptingSession.sequenceIndex());

        final int acceptorSequenceNumber = acceptingSession.lastSentMsgSeqNum() + 1;
        cannotConnectWithSequence(acceptorSequenceNumber, 1);

        assertEquals(0, acceptingSession.sequenceIndex());
        assertThat(acceptingSession.trySendSequenceReset(1, 1),
            greaterThan(0L));
        assertEquals(1, acceptingSession.sequenceIndex());

        initiatingOtfAcceptor.messages().clear();

        onAcquireSession = this::nothing;
        connectPersistingSessions(1, 1, false);

        final FixMessage logon = initiatingOtfAcceptor.receivedMessage(LOGON_MESSAGE_AS_STR).findFirst().get();
        assertEquals(1, logon.messageSequenceNumber());

        // Ensure that the sequenceIndex is correct after the reset
        assertEquals(1, acceptingSession.sequenceIndex());
    }

    private void connectPersistingSessions()
    {
        connectPersistingSessions(AUTOMATIC_INITIAL_SEQUENCE_NUMBER, false);
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
            acceptingEngine.resetSessionIds(backupLocation),
            initiatingEngine.resetSessionIds(backupLocation));
    }

    private void launch(final Runnable beforeConnect)
    {
        mediaDriver = launchMediaDriver(mediaDriverContext(
            TestFixtures.TERM_BUFFER_LENGTH,
            dirsDeleteOnStart));

        final EngineConfiguration config = acceptingConfig(port, ACCEPTOR_ID, INITIATOR_ID);
        config.sessionPersistenceStrategy(alwaysPersistent());
        if (!printErrorMessages)
        {
            config.customErrorConsumer(errorCounter);
        }
        acceptingEngine = FixEngine.launch(config);
        final EngineConfiguration initiatingConfig = initiatingConfig(libraryAeronPort);
        if (!printErrorMessages)
        {
            initiatingConfig.customErrorConsumer(errorCounter);
        }
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
        final Reply<Session> reply = connectPersistentSessions(
            initiatorInitialSentSequenceNumber, initiatorInitialReceivedSequenceNumber, resetSeqNum);

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
        assertLastLogonEquals(1, 0);
        assertSequenceResetTimeAtLatestLogon(initiatingSession);
        assertSequenceResetTimeAtLatestLogon(acceptingSession);

        assertSequenceIndicesAre(0);

        assertTestRequestSentAndReceived(initiatingSession, testSystem, acceptingOtfAcceptor);
        assertSequenceFromInitToAcceptAt(2, 2);

        final long initiatedSessionId = initiatingSession.id();
        final long acceptingSessionId = acceptingSession.id();

        logoutInitiatingSession();
        assertSessionsDisconnected();

        assertInitiatingSequenceIndexIs(0);
        clearMessages();
        acceptingHandler.clearSessionExistsInfos();
        close();

        duringRestart.run();
        firstConnectTimeRange = connectTimeRange;

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

    private void assertSequenceResetBeforeLastLogon(final Session session)
    {
        firstConnectTimeRange.assertWithinRange(session.lastSequenceResetTime());
        connectTimeRange.assertWithinRange(session.lastLogonTime());
        assertNotEquals(session.lastLogonTime(), session.lastSequenceResetTime());
    }
}
