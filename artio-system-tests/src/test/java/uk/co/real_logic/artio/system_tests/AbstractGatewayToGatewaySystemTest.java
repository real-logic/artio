/*
 * Copyright 2015-2020 Real Logic Limited, Adaptive Financial Consulting Ltd.
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
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.After;
import uk.co.real_logic.artio.*;
import uk.co.real_logic.artio.Reply.State;
import uk.co.real_logic.artio.builder.ResendRequestEncoder;
import uk.co.real_logic.artio.engine.ConnectedSessionInfo;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.engine.logger.FixArchiveScanner;
import uk.co.real_logic.artio.engine.logger.FixMessageConsumer;
import uk.co.real_logic.artio.library.FixLibrary;
import uk.co.real_logic.artio.library.SessionConfiguration;
import uk.co.real_logic.artio.messages.MetaDataStatus;
import uk.co.real_logic.artio.messages.SessionReplyStatus;
import uk.co.real_logic.artio.messages.SessionState;
import uk.co.real_logic.artio.session.InternalSession;
import uk.co.real_logic.artio.session.Session;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.assertTrue;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static uk.co.real_logic.artio.Constants.*;
import static uk.co.real_logic.artio.FixMatchers.*;
import static uk.co.real_logic.artio.GatewayProcess.NO_CONNECTION_ID;
import static uk.co.real_logic.artio.TestFixtures.cleanupMediaDriver;
import static uk.co.real_logic.artio.TestFixtures.unusedPort;
import static uk.co.real_logic.artio.Timing.assertEventuallyTrue;
import static uk.co.real_logic.artio.engine.EngineConfiguration.DEFAULT_ARCHIVE_SCANNER_STREAM;
import static uk.co.real_logic.artio.messages.SessionReplyStatus.OK;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.*;

public class AbstractGatewayToGatewaySystemTest
{
    static final int META_DATA_VALUE = 123;
    static final int META_DATA_WRONG_VALUE = 124;
    static final long META_DATA_SESSION_ID = 1L;
    static final long META_DATA_WRONG_SESSION_ID = 2L;

    protected int port = unusedPort();
    protected int libraryAeronPort = unusedPort();
    protected ArchivingMediaDriver mediaDriver;
    protected TestSystem testSystem;

    FixEngine acceptingEngine;
    FixEngine initiatingEngine;
    FixLibrary acceptingLibrary;
    FixLibrary initiatingLibrary;
    Session initiatingSession;
    Session acceptingSession;

    FakeOtfAcceptor acceptingOtfAcceptor = new FakeOtfAcceptor();
    FakeHandler acceptingHandler = new FakeHandler(acceptingOtfAcceptor);

    FakeOtfAcceptor initiatingOtfAcceptor = new FakeOtfAcceptor();
    FakeHandler initiatingHandler = new FakeHandler(initiatingOtfAcceptor);

    TimeRange connectTimeRange;

    @After
    public void close()
    {
        closeInitiatingEngine();
        closeAcceptingEngine();

        CloseHelper.close(initiatingLibrary);
        closeAcceptingLibrary();

        cleanupMediaDriver(mediaDriver);
    }

    void closeAcceptingLibrary()
    {
        CloseHelper.close(acceptingLibrary);
        testSystem.remove(acceptingLibrary);
    }

    void closeInitiatingEngine()
    {
        testSystem.awaitBlocking(() -> CloseHelper.close(initiatingEngine));
    }

    void closeAcceptingEngine()
    {
        testSystem.awaitBlocking(() -> CloseHelper.close(acceptingEngine));
    }

    void assertOriginalLibraryDoesNotReceiveMessages(final int initiatorMessageCount)
    {
        initiatingLibrary.poll(LIBRARY_LIMIT);
        assertThat("Messages received by wrong initiator",
            initiatingOtfAcceptor.messages(), hasSize(initiatorMessageCount));
    }

    void assertSequenceFromInitToAcceptAt(final int expectedInitToAccSeqNum, final int expectedAccToInitSeqNum)
    {
        assertEquals(expectedInitToAccSeqNum, initiatingSession.lastSentMsgSeqNum());
        assertEquals(expectedInitToAccSeqNum, acceptingSession.lastReceivedMsgSeqNum());

        awaitMessage(expectedAccToInitSeqNum, initiatingSession);

        assertEquals(expectedAccToInitSeqNum, initiatingSession.lastReceivedMsgSeqNum());
        assertEquals(expectedAccToInitSeqNum, acceptingSession.lastSentMsgSeqNum());
    }

    private void awaitMessage(final int sequenceNumber, final Session session)
    {
        assertEventuallyTrue(
            "Library Never reaches " + sequenceNumber,
            () ->
            {
                testSystem.poll();
                return session.lastReceivedMsgSeqNum() >= sequenceNumber;
            });
    }

    void disconnectSessions()
    {
        logoutAcceptingSession();

        assertSessionsDisconnected();
    }

    long logoutAcceptingSession()
    {
        return logoutSession(acceptingSession);
    }

    void logoutInitiatingSession()
    {
        logoutSession(initiatingSession);
    }

    void assertSessionsDisconnected()
    {
        assertSessionDisconnected(initiatingSession);
        assertSessionDisconnected(acceptingSession);

        assertEventuallyTrue("libraries receive disconnect messages",
            () ->
            {
                testSystem.poll();
                assertNotSession(acceptingHandler, acceptingSession);
                assertNotSession(initiatingHandler, initiatingSession);
            });
    }

    void sessionNoLongerManaged(final FakeHandler handler, final Session session)
    {
        assertEventuallyTrue("libraries receive disconnect messages",
            () ->
            {
                testSystem.poll();
                assertNotSession(handler, session);
            });
    }

    protected void assertSessionDisconnected(final Session session)
    {
        SystemTestUtil.assertSessionDisconnected(testSystem, session);
    }

    void assertNotSession(final FakeHandler sessionHandler, final Session session)
    {
        assertThat(sessionHandler.sessions(), not(hasItem(session)));
    }

    void assertHasSession(final FakeHandler sessionHandler, final Session session)
    {
        assertThat(sessionHandler.sessions(), not(hasItem(session)));
    }

    void wireSessions()
    {
        connectSessions();
        acquireAcceptingSession();
    }

    void acquireAcceptingSession()
    {
        acquireAcceptingSession(INITIATOR_ID);
    }

    void acquireAcceptingSession(final String initiatorId)
    {
        final long sessionId = acceptingHandler.awaitSessionId(testSystem::poll);

        acceptingSession = acquireSession(acceptingHandler, acceptingLibrary, sessionId, testSystem);
        assertEquals(initiatorId, acceptingHandler.lastInitiatorCompId());
        assertEquals(ACCEPTOR_ID, acceptingHandler.lastAcceptorCompId());
        assertNotNull("unable to acquire accepting session", acceptingSession);
    }

    void connectSessions()
    {
        connectTimeRange = new TimeRange();
        final Reply<Session> reply = initiate(initiatingLibrary, port, INITIATOR_ID, ACCEPTOR_ID);
        completeConnectInitiatingSession(reply);
        connectTimeRange.end();
    }

    void completeConnectInitiatingSession(final Reply<Session> reply)
    {
        initiatingSession = completeConnectSessions(reply);
    }

    Session completeConnectSessions(final Reply<Session> reply)
    {
        testSystem.awaitReply(reply);

        final Session session = reply.resultIfPresent();

        if (reply.error() != null)
        {
            reply.error().printStackTrace();
        }
        assertEquals(reply.toString(), State.COMPLETED, reply.state());
        assertConnected(session);

        return session;
    }

    void completeFailedSession(final Reply<Session> reply)
    {
        testSystem.awaitReply(reply);
        assertEquals(reply.toString(), State.ERRORED, reply.state());
    }

    FixMessage assertMessageResent(final int sequenceNumber, final String msgType, final boolean isGapFill)
    {
        assertThat(acceptingOtfAcceptor.messages(), hasSize(0));
        assertEventuallyTrue("Failed to receive the reply",
            () ->
            {
                testSystem.poll();

                final FixMessage message = acceptingOtfAcceptor.lastReceivedMessage();
                assertEquals(msgType, message.msgType());
                if (isGapFill)
                {
                    assertEquals("Y", message.get(GAP_FILL_FLAG));
                }
                else
                {
                    assertNotNull(message.get(ORIG_SENDING_TIME));
                }
                assertEquals("Y", message.possDup());
                assertEquals(String.valueOf(sequenceNumber), message.get(MSG_SEQ_NUM));
                assertEquals(INITIATOR_ID, message.get(Constants.SENDER_COMP_ID));
                assertNull("Detected Error", acceptingOtfAcceptor.lastError());
                assertTrue("Failed to complete parsing", acceptingOtfAcceptor.isCompleted());
            });

        return acceptingOtfAcceptor.lastReceivedMessage();
    }

    int acceptorSendsResendRequest()
    {
        final int seqNum = acceptingSession.lastReceivedMsgSeqNum();
        return acceptorSendsResendRequest(seqNum);
    }

    int acceptorSendsResendRequest(final int seqNum)
    {
        acceptorSendsResendRequest(seqNum, seqNum);

        return seqNum;
    }

    void acceptorSendsResendRequest(final int beginSeqNo, final int endSeqNo)
    {
        sendResendRequest(beginSeqNo, endSeqNo, acceptingOtfAcceptor, acceptingSession);
    }

    void sendResendRequest(
        final int beginSeqNo, final int endSeqNo, final FakeOtfAcceptor otfAcceptor, final Session session)
    {
        final ResendRequestEncoder resendRequest = new ResendRequestEncoder()
            .beginSeqNo(beginSeqNo)
            .endSeqNo(endSeqNo);

        otfAcceptor.messages().clear();

        while (session.trySend(resendRequest) < 0)
        {
            Thread.yield();
        }
    }

    void messagesCanBeExchanged()
    {
        messagesCanBeExchanged(initiatingSession);
    }

    void messagesCanBeExchanged(final Session session)
    {
        messagesCanBeExchanged(session, initiatingOtfAcceptor);
    }

    long messagesCanBeExchanged(final Session sendingSession, final FakeOtfAcceptor receivingAcceptor)
    {
        final String testReqID = testReqId();
        final long position = sendTestRequest(sendingSession, testReqID);

        assertReceivedSingleHeartbeat(testSystem, receivingAcceptor, testReqID);

        return position;
    }

    void clearMessages()
    {
        initiatingOtfAcceptor.messages().clear();
        acceptingOtfAcceptor.messages().clear();
    }

    void launchAcceptingEngine()
    {
        acceptingEngine = FixEngine.launch(
            acceptingConfig(port, ACCEPTOR_ID, INITIATOR_ID));
    }

    void assertSequenceIndicesAre(final int sequenceIndex)
    {
        assertAcceptingSessionHasSequenceIndex(sequenceIndex);
        assertInitiatingSequenceIndexIs(sequenceIndex);
        assertAllMessagesHaveSequenceIndex(sequenceIndex);
    }

    void assertAcceptingSessionHasSequenceIndex(final int sequenceIndex)
    {
        if (acceptingSession != null)
        {
            assertThat(acceptingSession, hasSequenceIndex(sequenceIndex));
        }
    }

    void assertInitiatingSequenceIndexIs(final int sequenceIndex)
    {
        assertThat(initiatingSession, hasSequenceIndex(sequenceIndex));

    }

    void assertAllMessagesHaveSequenceIndex(final int sequenceIndex)
    {
        acceptingOtfAcceptor.allMessagesHaveSequenceIndex(sequenceIndex);
        initiatingOtfAcceptor.allMessagesHaveSequenceIndex(sequenceIndex);
    }

    void sessionsCanReconnect()
    {
        acquireAcceptingSession();

        logoutSession(acceptingSession);
        assertSessionsDisconnected();

        assertAllMessagesHaveSequenceIndex(0);
        clearMessages();

        wireSessions();

        messagesCanBeExchanged();

        assertSequenceIndicesAre(1);
    }

    void releaseSessionToEngine(final Session session, final FixLibrary library, final FixEngine engine)
    {
        final long connectionId = session.connectionId();
        final long sessionId = session.id();

        final SessionReplyStatus status = releaseToEngine(library, session, testSystem);

        assertEquals(OK, status);
        assertEquals(SessionState.DISABLED, session.state());
        assertThat(library.sessions(), hasSize(0));

        final List<ConnectedSessionInfo> sessions = gatewayLibraryInfo(engine).sessions();
        assertThat(sessions, contains(allOf(
            hasConnectionId(connectionId),
            hasSessionId(sessionId))));
    }

    void assertCountersClosed(final boolean expectedClosed, final Session session)
    {
        assertEquals(expectedClosed, ((InternalSession)session).areCountersClosed());
    }

    void engineShouldManageSession(
        final Session session,
        final FixLibrary library,
        final FakeOtfAcceptor otfAcceptor,
        final Session otherSession,
        final FakeOtfAcceptor otherAcceptor)
    {
        final int lastReceivedMsgSeqNum = engineShouldManageSession(session, library, otherSession, otherAcceptor, OK);

        // Callbacks for the missing messages whilst the gateway managed them
        final List<FixMessage> messages = otfAcceptor.messages();
        final String expectedSeqNum = String.valueOf(lastReceivedMsgSeqNum + 1);
        final long messageCount = messages
            .stream()
            .filter((m) -> m.msgType().equals(TEST_REQUEST_MESSAGE_AS_STR) &&
            m.get(MSG_SEQ_NUM).equals(expectedSeqNum))
            .count();

        assertEquals("Expected a single test request" + messages.toString(), 1, messageCount);

        messagesCanBeExchanged(otherSession, otherAcceptor);
    }

    int engineShouldManageSession(
        final Session session,
        final FixLibrary library,
        final Session otherSession,
        final FakeOtfAcceptor otherAcceptor,
        final SessionReplyStatus expectedStatus)
    {
        final long sessionId = session.id();
        final int lastReceivedMsgSeqNum = session.lastReceivedMsgSeqNum();
        final int sequenceIndex = session.sequenceIndex();

        releaseToEngine(library, session, testSystem);

        messagesCanBeExchanged(otherSession, otherAcceptor);

        final SessionReplyStatus status = requestSession(
            library, sessionId, lastReceivedMsgSeqNum, sequenceIndex, testSystem);
        assertEquals(expectedStatus, status);

        final List<Session> sessions = library.sessions();
        assertThat(sessions, hasSize(1));

        return lastReceivedMsgSeqNum;
    }

    Reply<Session> connectPersistentSessions(
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

        connectTimeRange = new TimeRange();
        final Reply<Session> reply = initiatingLibrary.initiate(config);
        testSystem.awaitReply(reply);
        connectTimeRange.end();
        return reply;
    }

    void deleteAcceptorLogs()
    {
        delete(ACCEPTOR_LOGS);
    }

    void deleteClientLogs()
    {
        delete(CLIENT_LOGS);
    }

    void deleteLogs()
    {
        deleteAcceptorLogs();
        deleteClientLogs();
    }

    void launchMediaDriverWithDirs()
    {
        mediaDriver = TestFixtures.launchMediaDriverWithDirs();
    }

    void assertLastLogonEquals(final int lastLogonReceivedSequenceNumber, final int lastLogonSequenceIndex)
    {
        assertEquals(lastLogonReceivedSequenceNumber, acceptingHandler.lastLogonReceivedSequenceNumber());
        assertEquals(lastLogonSequenceIndex, acceptingHandler.lastLogonSequenceIndex());
    }

    void assertSequenceResetTimeAtLatestLogon(final Session session)
    {
        final long lastLogonTime = session.lastLogonTime();
        final long lastSequenceResetTime = session.lastSequenceResetTime();
        connectTimeRange.assertWithinRange(lastLogonTime);
        assertEquals("lastSequenceResetTime was not the same as lastLogonTime",
            lastLogonTime, lastSequenceResetTime);
    }

    List<String> getMessagesFromArchive(final EngineConfiguration configuration, final int queryStreamId)
    {
        final List<String> messages = new ArrayList<>();
        final FixMessageConsumer fixMessageConsumer =
            (message, buffer, offset, length, header) -> messages.add(message.body());

        final FixArchiveScanner.Context context = new FixArchiveScanner.Context()
            .aeronDirectoryName(configuration.aeronContext().aeronDirectoryName())
            .idleStrategy(CommonConfiguration.backoffIdleStrategy());

        try (FixArchiveScanner scanner = new FixArchiveScanner(context))
        {
            scanner.scan(
                configuration.libraryAeronChannel(),
                queryStreamId,
                fixMessageConsumer,
                false,
                DEFAULT_ARCHIVE_SCANNER_STREAM);
        }
        return messages;
    }

    void writeMetaData()
    {
        final UnsafeBuffer writeBuffer = new UnsafeBuffer(new byte[SIZE_OF_INT]);
        writeBuffer.putInt(0, META_DATA_VALUE);
        writeMetaData(writeBuffer);
    }

    void writeMetaData(final UnsafeBuffer writeBuffer)
    {
        final Reply<MetaDataStatus> reply = writeMetaData(writeBuffer, META_DATA_SESSION_ID);
        assertEquals(MetaDataStatus.OK, reply.resultIfPresent());
    }

    Reply<MetaDataStatus> writeMetaData(final UnsafeBuffer writeBuffer, final long sessionId)
    {
        return writeMetaData(writeBuffer, sessionId, 0);
    }

    Reply<MetaDataStatus> writeMetaData(
        final UnsafeBuffer writeBuffer, final long sessionId, final int metaDataOffset)
    {
        final Reply<MetaDataStatus> reply = acceptingLibrary.writeMetaData(
            sessionId, metaDataOffset, writeBuffer, 0, writeBuffer.capacity());

        testSystem.awaitCompletedReplies(reply);

        return reply;
    }

    UnsafeBuffer readSuccessfulMetaData(final UnsafeBuffer writeBuffer)
    {
        final FakeMetadataHandler handler = readMetaData(META_DATA_SESSION_ID);
        assertEquals(MetaDataStatus.OK, handler.status());

        final UnsafeBuffer readBuffer = handler.buffer();
        assertEquals(writeBuffer.capacity(), readBuffer.capacity());
        return readBuffer;
    }

    FakeMetadataHandler readMetaData(final long sessionId)
    {
        final FakeMetadataHandler handler = new FakeMetadataHandler();

        acceptingLibrary.readMetaData(sessionId, handler);

        Timing.assertEventuallyTrue("reading session meta data failed to terminate", () ->
        {
            testSystem.poll();

            return handler.callbackReceived();
        });

        return handler;
    }

    void assertOfflineSession(final long sessionId, final Session session)
    {
        assertEquals(sessionId, session.id());
        assertEquals("", session.connectedHost());
        assertEquals(Session.UNKNOWN, session.connectedPort());
        assertEquals(NO_CONNECTION_ID, session.connectionId());
        assertEquals(SessionState.DISCONNECTED, session.state());
    }

    Reply<?> resetSequenceNumber(final long sessionId)
    {
        return testSystem.awaitReply(acceptingEngine.resetSequenceNumber(sessionId));
    }
}
