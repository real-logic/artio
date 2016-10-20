/*
 * Copyright 2015-2016 Real Logic Ltd.
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
package uk.co.real_logic.fix_gateway.system_tests;

import org.agrona.IoUtil;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.YieldingIdleStrategy;
import org.hamcrest.Matcher;
import uk.co.real_logic.fix_gateway.CommonConfiguration;
import uk.co.real_logic.fix_gateway.Reply;
import uk.co.real_logic.fix_gateway.builder.TestRequestEncoder;
import uk.co.real_logic.fix_gateway.decoder.Constants;
import uk.co.real_logic.fix_gateway.engine.EngineConfiguration;
import uk.co.real_logic.fix_gateway.engine.FixEngine;
import uk.co.real_logic.fix_gateway.engine.framer.LibraryInfo;
import uk.co.real_logic.fix_gateway.library.FixLibrary;
import uk.co.real_logic.fix_gateway.library.LibraryConfiguration;
import uk.co.real_logic.fix_gateway.library.SessionConfiguration;
import uk.co.real_logic.fix_gateway.messages.SessionReplyStatus;
import uk.co.real_logic.fix_gateway.session.Session;
import uk.co.real_logic.fix_gateway.validation.AuthenticationStrategy;
import uk.co.real_logic.fix_gateway.validation.MessageValidationStrategy;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import static io.aeron.CommonContext.IPC_CHANNEL;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static uk.co.real_logic.fix_gateway.CommonConfiguration.backoffIdleStrategy;
import static uk.co.real_logic.fix_gateway.CommonConfiguration.optimalTmpDirName;
import static uk.co.real_logic.fix_gateway.Reply.State.COMPLETED;
import static uk.co.real_logic.fix_gateway.Timing.assertEventuallyTrue;
import static uk.co.real_logic.fix_gateway.engine.FixEngine.ENGINE_LIBRARY_ID;
import static uk.co.real_logic.fix_gateway.library.FixLibrary.NO_MESSAGE_REPLAY;
import static uk.co.real_logic.fix_gateway.messages.SessionState.ACTIVE;
import static uk.co.real_logic.fix_gateway.messages.SessionState.DISCONNECTED;

public final class SystemTestUtil
{
    public static final IdleStrategy ADMIN_IDLE_STRATEGY = new YieldingIdleStrategy();
    public static final String ACCEPTOR_ID = "acceptor";
    public static final String INITIATOR_ID = "initiator";
    public static final String INITIATOR_ID2 = "initiator2";
    public static final String CLIENT_LOGS = "client-logs";
    public static final String ACCEPTOR_LOGS = "acceptor-logs";
    public static final long TIMEOUT_IN_MS = 100;
    public static final long AWAIT_TIMEOUT = 50 * TIMEOUT_IN_MS;
    public static final String HI_ID = "hi";
    public static final int LIBRARY_LIMIT = 2;

    static
    {
        final File parentDirectory = new File(optimalTmpDirName());
        for (final File directory : parentDirectory.listFiles(file -> file.getName().startsWith("fix-library-")))
        {
            IoUtil.delete(directory, true);
        }
    }

    public static void assertSessionDisconnected(final FixLibrary library1, final Session session)
    {
        assertSessionDisconnected(library1, null, session);
    }

    public static void assertSessionDisconnected(
        final FixLibrary library1,
        final FixLibrary library2,
        final Session session)
    {
        assertEventuallyTrue("Session is still connected", () ->
        {
            poll(library1, library2);
            return session.state() == DISCONNECTED;
        });
    }

    public static long sendTestRequest(final Session session)
    {
        assertEventuallyTrue("Session not connected", session::isConnected);

        final TestRequestEncoder testRequest = new TestRequestEncoder();
        testRequest.testReqID(HI_ID);

        final long position = session.send(testRequest);
        assertThat(position, greaterThan(0L));
        return position;
    }

    public static void assertReceivedTestRequest(
        final FixLibrary library1, final FixLibrary library2, final FakeOtfAcceptor acceptor)
    {
        assertEventuallyTrue("Failed to receive a test request message", () ->
        {
            poll(library1, library2);
            return acceptor.hasReceivedMessage("1").isPresent();
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

    public static <T> Matcher<Iterable<? super T>> containsInitiator()
    {
        return containsLogon(ACCEPTOR_ID, INITIATOR_ID);
    }

    public static <T> Matcher<Iterable<? super T>> containsAcceptor()
    {
        return containsLogon(INITIATOR_ID, ACCEPTOR_ID);
    }

    private static <T> Matcher<Iterable<? super T>> containsLogon(final String senderCompId, final String targetCompId)
    {
        return hasItem(
            allOf(hasSenderCompId(senderCompId),
                hasTargetCompId(targetCompId)));
    }

    private static <T> Matcher<T> hasTargetCompId(final String targetCompId)
    {
        return hasProperty("targetCompID", equalTo(targetCompId));
    }

    private static <T> Matcher<T> hasSenderCompId(final String senderCompId)
    {
        return hasProperty("senderCompID", equalTo(senderCompId));
    }

    public static Reply<Session> initiateAndAwait(
        final FixLibrary library,
        final int port,
        final String initiatorId,
        final String acceptorId)
    {
        final Reply<Session> reply = initiate(library, port, initiatorId, acceptorId);

        awaitReply(library, reply);

        return reply;
    }

    public static Reply<Session> initiate(
        final FixLibrary library,
        final int port,
        final String initiatorId,
        final String acceptorId)
    {
        final SessionConfiguration config = SessionConfiguration.builder()
            .address("localhost", port)
            .credentials("bob", "Uv1aegoh")
            .senderCompId(initiatorId)
            .targetCompId(acceptorId)
            .build();

        return library.initiate(config);
    }

    public static void awaitReply(final FixLibrary library, final Reply<?> reply)
    {
        while (reply.isExecuting())
        {
            library.poll(1);
            ADMIN_IDLE_STRATEGY.idle();
        }
        ADMIN_IDLE_STRATEGY.reset();
    }

    public static SessionReplyStatus releaseToGateway(
        final FixLibrary library, final Session session)
    {
        final Reply<SessionReplyStatus> reply = library.releaseToGateway(session);
        awaitReply(library, reply);
        return reply.resultIfPresent();
    }

    public static FixEngine launchInitiatingGateway(final int libraryAeronPort)
    {
        delete(CLIENT_LOGS);
        return launchInitiatingGatewayWithSameLogs(libraryAeronPort);
    }

    public static FixEngine launchInitiatingGatewayWithSameLogs(final int libraryAeronPort)
    {
        final EngineConfiguration initiatingConfig = initiatingConfig(libraryAeronPort, "engineCounters");
        return FixEngine.launch(initiatingConfig);
    }

    public static EngineConfiguration initiatingConfig(
        final int libraryAeronPort,
        final String countersSuffix)
    {
        return new EngineConfiguration()
            .libraryAeronChannel("aeron:udp?endpoint=localhost:" + libraryAeronPort)
            .monitoringFile(optimalTmpDirName() + File.separator + "fix-client" + File.separator + countersSuffix)
            .logFileDir(CLIENT_LOGS);
    }

    public static void delete(final String dirPath)
    {
        final File dir = new File(dirPath);
        if (dir.exists())
        {
            IoUtil.delete(dir, false);
        }
    }

    public static EngineConfiguration acceptingConfig(
        final int port,
        final String countersSuffix,
        final String acceptorId,
        final String initiatorId)
    {
        return acceptingConfig(port, countersSuffix, acceptorId, initiatorId, ACCEPTOR_LOGS);
    }

    public static EngineConfiguration acceptingConfig(
        final int port,
        final String countersSuffix,
        final String acceptorId,
        final String initiatorId,
        final String acceptorLogs)
    {
        final EngineConfiguration configuration = new EngineConfiguration();
        setupAuthentication(acceptorId, initiatorId, configuration);
        return configuration
            .bindTo("localhost", port)
            .libraryAeronChannel("aeron:ipc")
            .monitoringFile(acceptorMonitoringFile(countersSuffix))
            .logFileDir(acceptorLogs);
    }

    public static String acceptorMonitoringFile(final String countersSuffix)
    {
        return optimalTmpDirName() + File.separator + "fix-acceptor" + File.separator + countersSuffix;
    }

    public static LibraryConfiguration acceptingLibraryConfig(
        final FakeHandler sessionHandler,
        final String acceptorId,
        final String initiatorId,
        final String libraryAeronChannel)
    {
        final LibraryConfiguration libraryConfiguration = new LibraryConfiguration();
        setupAuthentication(acceptorId, initiatorId, libraryConfiguration);

        libraryConfiguration
            .sessionExistsHandler(sessionHandler)
            .sessionAcquireHandler(sessionHandler)
            .sentPositionHandler(sessionHandler)
            .libraryAeronChannels(singletonList(libraryAeronChannel));

        return libraryConfiguration;
    }

    static void setupAuthentication(
        final String acceptorId,
        final String initiatorId,
        final CommonConfiguration configuration)
    {
        final MessageValidationStrategy validationStrategy = MessageValidationStrategy.targetCompId(acceptorId)
            .and(MessageValidationStrategy.senderCompId(Arrays.asList(initiatorId, INITIATOR_ID2)));

        final AuthenticationStrategy authenticationStrategy = AuthenticationStrategy.of(validationStrategy);

        configuration
            .authenticationStrategy(authenticationStrategy)
            .messageValidationStrategy(validationStrategy);
    }

    public static Session acquireSession(
        final FakeHandler sessionHandler,
        final FixLibrary library)
    {
        final long sessionId = sessionHandler.awaitSessionId(() -> library.poll(LIBRARY_LIMIT));
        return acquireSession(sessionHandler, library, sessionId);
    }

    public static Session acquireSession(
        final FakeHandler sessionHandler,
        final FixLibrary library,
        final long sessionId)
    {
        final SessionReplyStatus reply = getSessionStatus(library, sessionId, NO_MESSAGE_REPLAY);
        assertEquals(SessionReplyStatus.OK, reply);
        final Session session = sessionHandler.lastSession();
        sessionHandler.resetSession();
        return session;
    }

    public static SessionReplyStatus getSessionStatus(
        final FixLibrary library,
        final long sessionId,
        final int lastReceivedMsgSeqNum)
    {
        final Reply<SessionReplyStatus> reply = library.requestSession(sessionId, lastReceivedMsgSeqNum);
        awaitReply(library, reply);
        assertEquals(reply.state(), COMPLETED);
        return reply.resultIfPresent();
    }

    public static void sessionLogsOn(final FixLibrary library1,
        final FixLibrary library2,
        final Session session)
    {
        assertEventuallyTrue("Session has failed to logon", () ->
        {
            poll(library1, library2);
            assertEquals(ACTIVE, session.state());
        });
    }

    public static FixLibrary newInitiatingLibrary(
        final int libraryAeronPort,
        final FakeHandler sessionHandler)
    {
        final LibraryConfiguration configuration = new LibraryConfiguration()
            .sessionAcquireHandler(sessionHandler)
            .sentPositionHandler(sessionHandler)
            .libraryAeronChannels(singletonList("aeron:udp?endpoint=localhost:" + libraryAeronPort));

        return FixLibrary.connect(configuration);
    }

    public static FixLibrary newAcceptingLibrary(final FakeHandler sessionHandler)
    {
        return FixLibrary.connect(
            acceptingLibraryConfig(sessionHandler, ACCEPTOR_ID, INITIATOR_ID, IPC_CHANNEL));
    }

    public static void assertConnected(final Session session)
    {
        assertNotNull("Session is null", session);
        assertTrue("Session has failed to connect", session.isConnected());
    }

    public static void assertLibrariesDisconnect(final int count, final FixLibrary library, final FixEngine engine)
    {
        assertEventuallyTrue(
            "libraries haven't disconnected yet",
            () ->
            {
                if (library != null)
                {
                    library.poll(1);
                }
                return libraries(engine).size() == count + 1;
            },
            AWAIT_TIMEOUT,
            1);
    }

    public static List<LibraryInfo> libraries(final FixEngine engine)
    {
        final Reply<List<LibraryInfo>> reply = engine.libraries();
        while (reply.isExecuting())
        {
            ADMIN_IDLE_STRATEGY.idle();
        }
        ADMIN_IDLE_STRATEGY.reset();

        assertEquals(COMPLETED, reply.state());

        return reply.resultIfPresent();
    }

    public static void awaitLibraryConnect(final FixLibrary library)
    {
        assertEventuallyTrue(
            "Library hasn't seen Engine", () ->
            {
                library.poll(5);
                return library.isConnected();
            },
            AWAIT_TIMEOUT, 1);
    }

    public static void assertReceivedHeartbeat(final FixLibrary library,
        final FixLibrary library2,
        final FakeOtfAcceptor acceptor)
    {
        assertEventuallyTrue("Failed to received heartbeat", () ->
        {
            poll(library, library2);
            return acceptor
                .hasReceivedMessage("0")
                .filter(message -> HI_ID.equals(message.get(Constants.TEST_REQ_ID)))
                .isPresent();
        });
    }

    public static LibraryInfo gatewayLibraryInfo(final FixEngine engine)
    {
        return libraries(engine)
            .stream()
            .filter(libraryInfo -> libraryInfo.libraryId() == ENGINE_LIBRARY_ID)
            .findAny()
            .orElseThrow(IllegalStateException::new);
    }
}
