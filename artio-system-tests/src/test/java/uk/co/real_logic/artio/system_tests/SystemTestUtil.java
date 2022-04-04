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

import org.agrona.IoUtil;
import org.agrona.collections.IntHashSet;
import org.agrona.concurrent.EpochNanoClock;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.hamcrest.Matcher;
import uk.co.real_logic.artio.CommonConfiguration;
import uk.co.real_logic.artio.Constants;
import uk.co.real_logic.artio.FixDictionaryImpl;
import uk.co.real_logic.artio.Reply;
import uk.co.real_logic.artio.builder.AbstractTestRequestEncoder;
import uk.co.real_logic.artio.dictionary.FixDictionary;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.engine.LowResourceEngineScheduler;
import uk.co.real_logic.artio.engine.framer.LibraryInfo;
import uk.co.real_logic.artio.engine.logger.FixArchiveScanner;
import uk.co.real_logic.artio.engine.logger.FixMessageConsumer;
import uk.co.real_logic.artio.engine.logger.ReplayIndexExtractor;
import uk.co.real_logic.artio.fixp.FixPMessageConsumer;
import uk.co.real_logic.artio.library.FixLibrary;
import uk.co.real_logic.artio.library.LibraryConfiguration;
import uk.co.real_logic.artio.library.SessionConfiguration;
import uk.co.real_logic.artio.messages.SessionReplyStatus;
import uk.co.real_logic.artio.session.Session;
import uk.co.real_logic.artio.validation.AuthenticationStrategy;
import uk.co.real_logic.artio.validation.MessageValidationStrategy;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

import static io.aeron.CommonContext.IPC_CHANNEL;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static uk.co.real_logic.artio.CommonConfiguration.*;
import static uk.co.real_logic.artio.GatewayProcess.NO_CONNECTION_ID;
import static uk.co.real_logic.artio.Reply.State.COMPLETED;
import static uk.co.real_logic.artio.Timing.DEFAULT_TIMEOUT_IN_MS;
import static uk.co.real_logic.artio.Timing.assertEventuallyTrue;
import static uk.co.real_logic.artio.engine.EngineConfiguration.DEFAULT_ARCHIVE_SCANNER_STREAM;
import static uk.co.real_logic.artio.engine.FixEngine.ENGINE_LIBRARY_ID;
import static uk.co.real_logic.artio.library.FixLibrary.NO_MESSAGE_REPLAY;
import static uk.co.real_logic.artio.messages.SessionState.ACTIVE;
import static uk.co.real_logic.artio.messages.SessionState.DISCONNECTED;

public final class SystemTestUtil
{
    public static final String ACCEPTOR_ID = "acceptor";
    public static final String INITIATOR_ID = "initiator";
    public static final String ACCEPTOR_LOGS = "acceptor-logs";

    static final IdleStrategy ADMIN_IDLE_STRATEGY = new YieldingIdleStrategy();
    static final String INITIATOR_ID2 = "initiator2";
    static final String INITIATOR_ID3 = "initiator3";
    public static final String CLIENT_LOGS = "client-logs";
    static final long TIMEOUT_IN_MS = 100;
    static final long AWAIT_TIMEOUT_IN_MS = 50 * TIMEOUT_IN_MS;
    static final int LIBRARY_LIMIT = 2;

    static final String USERNAME = "bob";
    static final String PASSWORD = "Uv1aegoh";

    private static final String HI_ID = "hi";

    public static final long TEST_REPLY_TIMEOUT_IN_MS = RUNNING_ON_WINDOWS ? 3_000 : 1_000;

    private static final int TEST_COMPACTION_SIZE = 1024 * 1024;

    static
    {
        final File parentDirectory = new File(optimalTmpDirName());
        //noinspection ConstantConditions
        for (final File directory : parentDirectory.listFiles((file) -> file.getName().startsWith("fix-library-")))
        {
            IoUtil.delete(directory, true);
        }
    }

    private static final AtomicLong TEST_REQ_COUNTER = new AtomicLong();

    static String testReqId()
    {
        return HI_ID + TEST_REQ_COUNTER.incrementAndGet();
    }

    static long assertTestRequestSentAndReceived(
        final Session sendingSession,
        final TestSystem testSystem,
        final FakeOtfAcceptor receivingHandler)
    {
        final String testReqID = testReqId();
        final long position = sendTestRequest(testSystem, sendingSession, testReqID);

        assertReceivedTestRequest(testSystem, receivingHandler, testReqID);

        return position;
    }

    public static long sendTestRequest(final TestSystem testSystem, final Session session, final String testReqID)
    {
        return sendTestRequest(testSystem, session, testReqID, new FixDictionaryImpl());
    }

    static long sendTestRequest(
        final TestSystem testSystem, final Session session, final String testReqID, final FixDictionary fixDictionary)
    {
        assertEventuallyTrue("Session not connected", session::isConnected);

        return alwaysSendTestRequest(testSystem, session, testReqID, fixDictionary);
    }

    static long alwaysSendTestRequest(
        final TestSystem testSystem, final Session session, final String testReqID, final FixDictionary fixDictionary)
    {
        final AbstractTestRequestEncoder testRequest = fixDictionary.makeTestRequestEncoder();
        testRequest.testReqID(testReqID);

        return testSystem.awaitSend(() -> session.trySend(testRequest));
    }

    static void assertReceivedTestRequest(
        final TestSystem testSystem, final FakeOtfAcceptor acceptor, final String testReqId)
    {
        assertEventuallyTrue("Failed to receive a test request message",
            () ->
            {
                testSystem.poll();
                return acceptor
                    .receivedMessage("1")
                    .anyMatch((msg) -> testReqId.equals(msg.testReqId()));
            });
    }

    public static void poll(final FixLibrary library1, final FixLibrary library2)
    {
        library1.poll(LIBRARY_LIMIT);
        if (library2 != null)
        {
            library2.poll(LIBRARY_LIMIT);
        }
    }

    static Reply<Session> initiate(
        final FixLibrary library,
        final int port,
        final String senderCompId,
        final String targetCompId)
    {
        return initiate(library, port, senderCompId, targetCompId, TEST_REPLY_TIMEOUT_IN_MS);
    }

    static Reply<Session> initiate(
        final FixLibrary library, final int port, final String senderCompId, final String targetCompId,
        final long timeoutInMs)
    {
        final SessionConfiguration config = SessionConfiguration.builder()
            .address("localhost", port)
            .credentials(USERNAME, PASSWORD)
            .senderCompId(senderCompId)
            .targetCompId(targetCompId)
            .timeoutInMs(timeoutInMs)
            .build();

        return library.initiate(config);
    }

    public static void awaitReply(final Reply<?> reply)
    {
        assertEventuallyTrue(
            "No reply from: " + reply,
            () -> !reply.isExecuting());
    }

    static void awaitLibraryReply(final FixLibrary library, final Reply<?> reply)
    {
        awaitLibraryReply(library, null, reply);
    }

    static void awaitLibraryReply(final FixLibrary library, final FixLibrary library2, final Reply<?> reply)
    {
        assertEventuallyTrue(
            "No reply from: " + reply,
            () ->
            {
                poll(library, library2);

                return !reply.isExecuting();
            });
    }

    static SessionReplyStatus releaseToEngine(
        final FixLibrary library, final Session session, final TestSystem testSystem)
    {
        final Reply<SessionReplyStatus> reply = testSystem.awaitReply(
            library.releaseToGateway(session, DEFAULT_REPLY_TIMEOUT_IN_MS));

        return reply.resultIfPresent();
    }

    static FixEngine launchInitiatingEngine(final int libraryAeronPort, final EpochNanoClock nanoClock)
    {
        return launchInitiatingEngine(libraryAeronPort, true, nanoClock);
    }

    static FixEngine launchInitiatingEngineWithSameLogs(final int libraryAeronPort, final EpochNanoClock nanoClock)
    {
        return launchInitiatingEngine(libraryAeronPort, false, nanoClock);
    }

    static FixEngine launchInitiatingEngine(
        final int libraryAeronPort, final boolean deleteDirOnStart, final EpochNanoClock nanoClock)
    {
        final EngineConfiguration initiatingConfig = initiatingConfig(libraryAeronPort, nanoClock);
        initiatingConfig.deleteLogFileDirOnStart(deleteDirOnStart);
        return FixEngine.launch(initiatingConfig);
    }

    static EngineConfiguration initiatingConfig(final int libraryAeronPort, final EpochNanoClock nanoClock)
    {
        final EngineConfiguration configuration = new EngineConfiguration()
            .libraryAeronChannel("aeron:udp?endpoint=localhost:" + libraryAeronPort)
            .monitoringFile(optimalTmpDirName() + File.separator + "fix-client" + File.separator + "engineCounters")
            .logFileDir(CLIENT_LOGS)
            .scheduler(new LowResourceEngineScheduler())
            .slowConsumerTimeoutInMs(TEST_REPLY_TIMEOUT_IN_MS)
            .replyTimeoutInMs(TEST_REPLY_TIMEOUT_IN_MS);
        configuration.epochNanoClock(nanoClock);
        configuration.agentNamePrefix("init-");

        return configuration;
    }

    public static void delete(final String dirPath)
    {
        final File dir = new File(dirPath);
        if (dir.exists())
        {
            IoUtil.delete(dir, false);
        }
    }

    static EngineConfiguration acceptingConfig(
        final int port,
        final String acceptorId,
        final String initiatorId,
        final EpochNanoClock nanoClock)
    {
        return acceptingConfig(port, acceptorId, initiatorId, ACCEPTOR_LOGS, nanoClock);
    }

    static EngineConfiguration acceptingConfig(
        final int port,
        final String acceptorId,
        final String initiatorId,
        final String acceptorLogs,
        final EpochNanoClock nanoClock)
    {
        final EngineConfiguration configuration = new EngineConfiguration();
        final MessageValidationStrategy validationStrategy = setupCommonConfig(
            acceptorId, initiatorId, nanoClock, configuration);
        final AuthenticationStrategy authenticationStrategy = AuthenticationStrategy.of(validationStrategy);
        configuration.authenticationStrategy(authenticationStrategy);

        return configuration
            .bindTo("localhost", port)
            .libraryAeronChannel(IPC_CHANNEL)
            .replayIndexFileRecordCapacity(33554432)
            .monitoringFile(acceptorMonitoringFile("engineCounters"))
            .logFileDir(acceptorLogs)
            .scheduler(new LowResourceEngineScheduler())
            .slowConsumerTimeoutInMs(TEST_REPLY_TIMEOUT_IN_MS)
            .replyTimeoutInMs(TEST_REPLY_TIMEOUT_IN_MS);
    }

    static String acceptorMonitoringFile(final String countersSuffix)
    {
        return optimalTmpDirName() + File.separator + "fix-acceptor" + File.separator + countersSuffix;
    }

    static LibraryConfiguration acceptingLibraryConfig(
        final FakeHandler sessionHandler, final EpochNanoClock nanoClock)
    {
        final LibraryConfiguration libraryConfiguration = new LibraryConfiguration();
        setupCommonConfig(ACCEPTOR_ID, INITIATOR_ID, nanoClock, libraryConfiguration);

        libraryConfiguration
            .sessionExistsHandler(sessionHandler)
            .sessionAcquireHandler(sessionHandler)
            .libraryAeronChannels(singletonList(IPC_CHANNEL))
            .libraryName("accepting")
            .replyTimeoutInMs(TEST_REPLY_TIMEOUT_IN_MS);

        return libraryConfiguration;
    }

    static MessageValidationStrategy setupCommonConfig(
        final String acceptorId,
        final String initiatorId,
        final EpochNanoClock nanoClock,
        final CommonConfiguration configuration)
    {
        final MessageValidationStrategy validationStrategy = MessageValidationStrategy.targetCompId(acceptorId)
            .and(MessageValidationStrategy.senderCompId(Arrays.asList(initiatorId,
            INITIATOR_ID2, INITIATOR_ID3, "initiator4", "initiator5")));

        configuration
            .messageValidationStrategy(validationStrategy)
            .epochNanoClock(nanoClock);

        return validationStrategy;
    }

    static SessionReplyStatus requestSession(
        final FixLibrary library,
        final long sessionId,
        final TestSystem testSystem)
    {
        return requestSession(library, sessionId, NO_MESSAGE_REPLAY, NO_MESSAGE_REPLAY, testSystem);
    }

    static SessionReplyStatus requestSession(
        final FixLibrary library,
        final long sessionId,
        final int lastReceivedMsgSeqNum,
        final int sequenceIndex,
        final TestSystem testSystem)
    {
        final Reply<SessionReplyStatus> reply = testSystem.awaitReply(library.requestSession(
            sessionId, lastReceivedMsgSeqNum, sequenceIndex, DEFAULT_REPLY_TIMEOUT_IN_MS));
        assertEquals(COMPLETED, reply.state());

        return reply.resultIfPresent();
    }

    static Session acquireSession(
        final FakeHandler sessionHandler, final FixLibrary library, final long sessionId, final TestSystem testSystem)
    {
        return acquireSession(sessionHandler, library, sessionId, testSystem, NO_MESSAGE_REPLAY, NO_MESSAGE_REPLAY);
    }

    static Session acquireSession(
        final FakeHandler sessionHandler,
        final FixLibrary library,
        final long sessionId,
        final TestSystem testSystem,
        final int lastReceivedSequenceNumber,
        final int sequenceIndex)
    {
        final SessionReplyStatus replyStatus = requestSession(
            library, sessionId, lastReceivedSequenceNumber, sequenceIndex, testSystem);

        assertEquals(SessionReplyStatus.OK, replyStatus);
        final Session session = sessionHandler.lastSession();
        sessionHandler.resetSession();

        return session;
    }

    static FixLibrary newInitiatingLibrary(
        final int libraryAeronPort, final FakeHandler sessionHandler, final EpochNanoClock nanoClock)
    {
        return connect(initiatingLibraryConfig(libraryAeronPort, sessionHandler, nanoClock));
    }

    static LibraryConfiguration initiatingLibraryConfig(
        final int libraryAeronPort, final FakeHandler sessionHandler, final EpochNanoClock nanoClock)
    {
        final LibraryConfiguration config = new LibraryConfiguration()
            .sessionAcquireHandler(sessionHandler)
            .sessionExistsHandler(sessionHandler)
            .libraryAeronChannels(singletonList("aeron:udp?endpoint=localhost:" + libraryAeronPort))
            .libraryName("initiating")
            .replyTimeoutInMs(TEST_REPLY_TIMEOUT_IN_MS);
        config.epochNanoClock(nanoClock);
        return config;
    }

    public static FixLibrary connect(final LibraryConfiguration configuration)
    {
        final FixLibrary library = FixLibrary.connect(configuration);
        assertEventuallyTrue(
            () -> "Unable to connect to engine",
            () ->
            {
                library.poll(LIBRARY_LIMIT);

                return library.isConnected();
            },
            DEFAULT_TIMEOUT_IN_MS,
            library::close);

        return library;
    }

    static FixLibrary newAcceptingLibrary(final FakeHandler sessionHandler, final EpochNanoClock nanoClock)
    {
        return connect(acceptingLibraryConfig(sessionHandler, nanoClock));
    }

    static void assertConnected(final Session session)
    {
        assertNotNull("Session is null", session);
        assertEquals("Session has failed to connect", ACTIVE, session.state());
    }

    static List<LibraryInfo> libraries(final FixEngine engine)
    {
        final Reply<List<LibraryInfo>> reply = engine.libraries();
        assertEventuallyTrue(
            "No reply from: " + reply,
            () -> !reply.isExecuting());

        assertEquals(COMPLETED, reply.state());

        return reply.resultIfPresent();
    }

    public static void awaitLibraryDisconnect(final FixEngine engine)
    {
        awaitLibraryDisconnect(engine, null);
    }

    public static void awaitLibraryDisconnect(final FixEngine engine, final TestSystem testSystem)
    {
        awaitLibraryCount(engine, testSystem, 1);
    }

    public static void awaitLibraryCount(final FixEngine engine, final TestSystem testSystem, final int count)
    {
        assertEventuallyTrue(
            () -> "libraries haven't disconnected yet",
            () -> libraries(engine, testSystem).size() == count,
            AWAIT_TIMEOUT_IN_MS,
            () ->
            {
            }
        );
    }

    static List<LibraryInfo> libraries(final FixEngine engine, final TestSystem testSystem)
    {
        final Reply<List<LibraryInfo>> reply = engine.libraries();
        assertEventuallyTrue(
            "No reply from: " + reply,
            () ->
            {
                if (testSystem != null)
                {
                    testSystem.poll();
                }

                return !reply.isExecuting();
            });

        assertEquals(COMPLETED, reply.state());

        return reply.resultIfPresent();
    }

    static Optional<LibraryInfo> libraryInfoById(final List<LibraryInfo> libraries, final int libraryId)
    {
        return libraries
            .stream()
            .filter((libraryInfo) -> libraryInfo.libraryId() == libraryId)
            .findFirst();
    }

    static LibraryInfo engineLibrary(final List<LibraryInfo> libraries)
    {
        return libraryInfoById(libraries, ENGINE_LIBRARY_ID).get(); // Error if not present
    }

    static void awaitLibraryConnect(final FixLibrary library)
    {
        assertEventuallyTrue(
            () -> "Library hasn't seen Engine",
            () ->
            {
                library.poll(5);
                return library.isConnected();
            },
            AWAIT_TIMEOUT_IN_MS,
            () -> {}
        );
    }

    static void assertReceivedSingleHeartbeat(
        final TestSystem testSystem, final FakeOtfAcceptor acceptor, final String testReqId)
    {
        assertEventuallyTrue("Failed to received heartbeat",
            () ->
            {
                testSystem.poll();

                return acceptor
                    .receivedMessage("0")
                    .anyMatch((message) -> testReqId.equals(message.get(Constants.TEST_REQ_ID)));
            });
    }

    static LibraryInfo gatewayLibraryInfo(final FixEngine engine)
    {
        return libraries(engine)
            .stream()
            .filter((libraryInfo) -> libraryInfo.libraryId() == ENGINE_LIBRARY_ID)
            .findAny()
            .orElseThrow(IllegalStateException::new);
    }

    @SafeVarargs
    static void assertEventuallyHasLibraries(
        final TestSystem testSystem,
        final FixEngine engine,
        final Matcher<LibraryInfo>... libraryMatchers)
    {
        assertEventuallyTrue("Could not find libraries: " + Arrays.toString(libraryMatchers),
            () ->
            {
                testSystem.poll();
                final List<LibraryInfo> libraries = libraries(engine);
                assertThat(libraries, containsInAnyOrder(libraryMatchers));
            });
    }

    public static void assertSessionDisconnected(final TestSystem testSystem, final Session session)
    {
        assertEventuallyTrue("Session is still connected",
            () ->
            {
                testSystem.poll();
                return session.state() == DISCONNECTED && session.connectionId() == NO_CONNECTION_ID;
            });
    }

    public static long logoutSession(final Session session)
    {
        final long position = session.startLogout();
        assertThat(position, greaterThan(0L));
        return position;
    }

    public static void validateReplayIndex(final FixEngine engine, final Session session)
    {
        if (session == null || engine == null)
        {
            return;
        }

        final EngineConfiguration config = engine.configuration();
        final long sessionId = session.id();
        if (config.logInboundMessages())
        {
            validateReplayIndex(config, sessionId, true);
        }
        if (config.logOutboundMessages())
        {
            validateReplayIndex(config, sessionId, false);
        }
    }

    private static void validateReplayIndex(
        final EngineConfiguration config, final long sessionId, final boolean inbound)
    {
        final ReplayIndexExtractor.ReplayIndexValidator validator = new ReplayIndexExtractor.ReplayIndexValidator();
        ReplayIndexExtractor.extract(config, sessionId, inbound, validator);
        assertThat(validator.errors(), hasSize(0));
    }

    public static void getMessagesFromArchive(
        final EngineConfiguration configuration,
        final IntHashSet queryStreamIds,
        final FixMessageConsumer fixMessageConsumer,
        final FixPMessageConsumer fixPConsumer,
        final boolean follow)
    {
        final FixArchiveScanner.Configuration context = new FixArchiveScanner.Configuration()
            .aeronDirectoryName(configuration.aeronContext().aeronDirectoryName())
            .idleStrategy(CommonConfiguration.backoffIdleStrategy())
            .compactionSize(TEST_COMPACTION_SIZE);

        try (FixArchiveScanner scanner = new FixArchiveScanner(context))
        {
            scanner.scan(
                configuration.libraryAeronChannel(),
                queryStreamIds,
                fixMessageConsumer,
                fixPConsumer,
                follow,
                DEFAULT_ARCHIVE_SCANNER_STREAM);
        }
    }
}
