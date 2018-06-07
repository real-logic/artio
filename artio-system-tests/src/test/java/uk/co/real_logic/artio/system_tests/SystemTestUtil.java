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
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.hamcrest.Matcher;
import uk.co.real_logic.artio.CommonConfiguration;
import uk.co.real_logic.artio.Reply;
import uk.co.real_logic.artio.builder.TestRequestEncoder;
import uk.co.real_logic.artio.Constants;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.engine.LowResourceEngineScheduler;
import uk.co.real_logic.artio.engine.framer.LibraryInfo;
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
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.*;
import static uk.co.real_logic.artio.CommonConfiguration.DEFAULT_REPLY_TIMEOUT_IN_MS;
import static uk.co.real_logic.artio.CommonConfiguration.optimalTmpDirName;
import static uk.co.real_logic.artio.Reply.State.COMPLETED;
import static uk.co.real_logic.artio.Timing.DEFAULT_TIMEOUT_IN_MS;
import static uk.co.real_logic.artio.Timing.assertEventuallyTrue;
import static uk.co.real_logic.artio.engine.FixEngine.ENGINE_LIBRARY_ID;
import static uk.co.real_logic.artio.library.FixLibrary.NO_MESSAGE_REPLAY;
import static uk.co.real_logic.artio.messages.SessionState.ACTIVE;

public final class SystemTestUtil
{
    public static final String ACCEPTOR_ID = "acceptor";
    public static final String INITIATOR_ID = "initiator";
    public static final String ACCEPTOR_LOGS = "acceptor-logs";

    static final IdleStrategy ADMIN_IDLE_STRATEGY = new YieldingIdleStrategy();
    static final String INITIATOR_ID2 = "initiator2";
    static final String CLIENT_LOGS = "client-logs";
    static final long TIMEOUT_IN_MS = 100;
    static final long AWAIT_TIMEOUT = 50 * TIMEOUT_IN_MS;
    static final int LIBRARY_LIMIT = 2;

    private static final String HI_ID = "hi";
    private static final String USERNAME = "bob";
    private static final String PASSWORD = "Uv1aegoh";
    private static final int MESSAGE_BUFFER_SIZE_IN_BYTES = 15000;

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
        final long position = sendTestRequest(sendingSession, testReqID);

        assertReceivedTestRequest(testSystem, receivingHandler, testReqID);

        return position;
    }

    static long sendTestRequest(final Session session, final String testReqID)
    {
        assertEventuallyTrue("Session not connected", session::isConnected);

        final TestRequestEncoder testRequest = new TestRequestEncoder();
        testRequest.testReqID(testReqID);

        final long position = session.send(testRequest);
        assertThat(position, greaterThan(0L));
        return position;
    }

    private static void assertReceivedTestRequest(
        final TestSystem testSystem, final FakeOtfAcceptor acceptor, final String testReqId)
    {
        assertEventuallyTrue("Failed to receive a test request message",
            () ->
            {
                testSystem.poll();
                return acceptor
                    .hasReceivedMessage("1")
                    .filter((msg) -> testReqId.equals(msg.getTestReqId()))
                    .count() > 0;
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
        final String initiatorId,
        final String acceptorId)
    {
        final SessionConfiguration config = SessionConfiguration.builder()
            .address("localhost", port)
            .credentials(USERNAME, PASSWORD)
            .senderCompId(initiatorId)
            .targetCompId(acceptorId)
            .build();

        return library.initiate(config);
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

    static SessionReplyStatus releaseToGateway(
        final FixLibrary library, final Session session, final TestSystem testSystem)
    {
        final Reply<SessionReplyStatus> reply = testSystem.awaitReply(
            library.releaseToGateway(session, DEFAULT_REPLY_TIMEOUT_IN_MS));

        return reply.resultIfPresent();
    }

    static FixEngine launchInitiatingEngine(final int libraryAeronPort)
    {
        delete(CLIENT_LOGS);
        return launchInitiatingEngineWithSameLogs(libraryAeronPort);
    }

    static FixEngine launchInitiatingEngineWithSameLogs(final int libraryAeronPort)
    {
        final EngineConfiguration initiatingConfig = initiatingConfig(libraryAeronPort);
        return FixEngine.launch(initiatingConfig);
    }

    static EngineConfiguration initiatingConfig(final int libraryAeronPort)
    {
        final EngineConfiguration configuration = new EngineConfiguration()
            .libraryAeronChannel("aeron:udp?endpoint=localhost:" + libraryAeronPort)
            .monitoringFile(optimalTmpDirName() + File.separator + "fix-client" + File.separator + "engineCounters")
            .logFileDir(CLIENT_LOGS)
            .scheduler(new LowResourceEngineScheduler());
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
        final String initiatorId)
    {
        return acceptingConfig(port, acceptorId, initiatorId, ACCEPTOR_LOGS);
    }

    private static EngineConfiguration acceptingConfig(
        final int port,
        final String acceptorId,
        final String initiatorId,
        final String acceptorLogs)
    {
        final EngineConfiguration configuration = new EngineConfiguration();
        setupCommonConfig(acceptorId, initiatorId, configuration);

        return configuration
            .bindTo("localhost", port)
            .libraryAeronChannel("aeron:ipc")
            .monitoringFile(acceptorMonitoringFile("engineCounters"))
            .logFileDir(acceptorLogs)
            .scheduler(new LowResourceEngineScheduler());
    }

    static String acceptorMonitoringFile(final String countersSuffix)
    {
        return optimalTmpDirName() + File.separator + "fix-acceptor" + File.separator + countersSuffix;
    }

    static LibraryConfiguration acceptingLibraryConfig(
        final FakeHandler sessionHandler)
    {
        final LibraryConfiguration libraryConfiguration = new LibraryConfiguration();
        setupCommonConfig(ACCEPTOR_ID, INITIATOR_ID, libraryConfiguration);

        libraryConfiguration
            .sessionExistsHandler(sessionHandler)
            .sessionAcquireHandler(sessionHandler)
            .sentPositionHandler(sessionHandler)
            .libraryAeronChannels(singletonList(IPC_CHANNEL));

        return libraryConfiguration;
    }

    static void setupCommonConfig(
        final String acceptorId, final String initiatorId, final CommonConfiguration configuration)
    {
        final MessageValidationStrategy validationStrategy = MessageValidationStrategy.targetCompId(acceptorId)
            .and(MessageValidationStrategy.senderCompId(Arrays.asList(initiatorId, INITIATOR_ID2)));

        final AuthenticationStrategy authenticationStrategy = AuthenticationStrategy.of(validationStrategy);

        configuration
            .authenticationStrategy(authenticationStrategy)
            .messageValidationStrategy(validationStrategy);
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

    static void sessionLogsOn(
        final TestSystem testSystem,
        final Session session,
        final long timeoutInMs)
    {
        assertEventuallyTrue("Session has failed to logon",
            () ->
            {
                testSystem.poll();
                testSystem.assertConnected();

                assertEquals(ACTIVE, session.state());
            },
            timeoutInMs);
    }

    static FixLibrary newInitiatingLibrary(final int libraryAeronPort, final FakeHandler sessionHandler)
    {
        return connect(initiatingLibraryConfig(libraryAeronPort, sessionHandler));
    }

    static LibraryConfiguration initiatingLibraryConfig(final int libraryAeronPort, final FakeHandler sessionHandler)
    {
        return new LibraryConfiguration()
            .sessionAcquireHandler(sessionHandler)
            .sentPositionHandler(sessionHandler)
            .sessionExistsHandler(sessionHandler)
            .libraryAeronChannels(singletonList("aeron:udp?endpoint=localhost:" + libraryAeronPort));
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

    static FixLibrary newAcceptingLibrary(final FakeHandler sessionHandler)
    {
        return connect(acceptingLibraryConfig(sessionHandler));
    }

    static void assertConnected(final Session session)
    {
        assertNotNull("Session is null", session);
        assertTrue("Session has failed to connect", session.isConnected());
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

    static List<LibraryInfo> libraries(final FixEngine engine, final TestSystem testSystem)
    {
        final Reply<List<LibraryInfo>> reply = engine.libraries();
        assertEventuallyTrue(
            "No reply from: " + reply,
            () ->
            {
                testSystem.poll();

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

    @SuppressWarnings("ConstantConditions")
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
            AWAIT_TIMEOUT,
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
                    .hasReceivedMessage("0")
                    .filter((message) -> testReqId.equals(message.get(Constants.TEST_REQ_ID)))
                    .count() > 0;
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

    static String largeTestReqId()
    {
        final char[] testReqIDChars = new char[MESSAGE_BUFFER_SIZE_IN_BYTES - 100];
        Arrays.fill(testReqIDChars, 'A');

        return new String(testReqIDChars);
    }
}
