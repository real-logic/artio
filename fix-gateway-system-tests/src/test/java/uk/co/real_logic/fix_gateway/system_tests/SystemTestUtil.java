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

import org.hamcrest.Matcher;
import uk.co.real_logic.agrona.IoUtil;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.agrona.concurrent.SleepingIdleStrategy;
import uk.co.real_logic.fix_gateway.builder.TestRequestEncoder;
import uk.co.real_logic.fix_gateway.engine.EngineConfiguration;
import uk.co.real_logic.fix_gateway.engine.FixEngine;
import uk.co.real_logic.fix_gateway.library.FixLibrary;
import uk.co.real_logic.fix_gateway.library.LibraryConfiguration;
import uk.co.real_logic.fix_gateway.library.SessionConfiguration;
import uk.co.real_logic.fix_gateway.library.session.NewSessionHandler;
import uk.co.real_logic.fix_gateway.library.session.Session;
import uk.co.real_logic.fix_gateway.library.validation.AuthenticationStrategy;
import uk.co.real_logic.fix_gateway.library.validation.MessageValidationStrategy;
import uk.co.real_logic.fix_gateway.library.validation.SenderCompIdValidationStrategy;
import uk.co.real_logic.fix_gateway.library.validation.TargetCompIdValidationStrategy;

import java.io.File;
import java.util.Arrays;
import java.util.concurrent.locks.LockSupport;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static uk.co.real_logic.fix_gateway.CommonConfiguration.backoffIdleStrategy;
import static uk.co.real_logic.fix_gateway.Timing.assertEventuallyTrue;
import static uk.co.real_logic.fix_gateway.library.session.SessionState.ACTIVE;
import static uk.co.real_logic.fix_gateway.library.session.SessionState.DISCONNECTED;

public final class SystemTestUtil
{
    public static final IdleStrategy ADMIN_IDLE_STRATEGY = backoffIdleStrategy();
    public static final long CONNECTION_ID = 0L;
    public static final String ACCEPTOR_ID = "CCG";
    public static final String INITIATOR_ID = "LEH_LZJ02";
    public static final String INITIATOR_ID2 = "initiator2";
    public static final String CLIENT_LOGS = "client-logs";
    public static final String ACCEPTOR_LOGS = "acceptor-logs";
    public static final long TIMEOUT_IN_MS = 100;
    public static final long AWAIT_TIMEOUT = 50 * TIMEOUT_IN_MS;

    public static void assertDisconnected(final FixLibrary handlerLibrary,
                                          final FakeSessionHandler sessionHandler,
                                          final FixLibrary sessionLibrary,
                                          final Session session)
    {
        assertAcceptorDisconnected(handlerLibrary, sessionHandler);

        assertSessionDisconnected(handlerLibrary, sessionLibrary, session);
    }

    public static void assertAcceptorDisconnected(final FixLibrary library,
                                                  final FakeSessionHandler sessionHandler)
    {
        assertEventuallyTrue("Failed to requestDisconnect",
            () ->
            {
                library.poll(1);
                assertEquals(CONNECTION_ID, sessionHandler.connectionId());
            });
    }

    public static void assertSessionDisconnected(final FixLibrary library1, final Session session)
    {
        assertSessionDisconnected(library1, null, session);
    }

    public static void assertSessionDisconnected(final FixLibrary library1,
                                                 final FixLibrary library2,
                                                 final Session session)
    {
        assertEventuallyTrue("Session is still connected", () ->
        {
            poll(library1, library2);
            return session.state() == DISCONNECTED;
        });
    }

    public static void sendTestRequest(final Session session)
    {
        assertEventuallyTrue("Session not connected", session::isConnected);

        final TestRequestEncoder testRequest = new TestRequestEncoder();
        testRequest.testReqID("hi");

        session.send(testRequest);
    }

    public static void assertReceivedTestRequest(
        final FixLibrary library, final FakeOtfAcceptor acceptor)
    {
        assertReceivedTestRequest(library, null, acceptor);
    }

    public static void assertReceivedTestRequest(
        final FixLibrary library1, final FixLibrary library2, final FakeOtfAcceptor acceptor)
    {
        assertReceivedTestRequest(library1, library2, acceptor, 2);
    }

    public static void assertReceivedTestRequest(
        final FixLibrary library1,
        final FixLibrary library2,
        final FakeOtfAcceptor acceptor,
        final int messageCount)
    {
        assertEventuallyTrue("Failed to receive " + messageCount + " messages", () ->
        {
            poll(library1, library2);
            assertEquals(messageCount, acceptor.messages().size());
        });

        final FixMessage message = acceptor.messages().get(messageCount - 1);
        assertEquals("Not a test request message", "1", message.getMessageType());
    }

    public static void poll(final FixLibrary library1, final FixLibrary library2)
    {
        library1.poll(1);
        if (library2 != null)
        {
            library2.poll(1);
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

    public static Session initiate(
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

        return library.initiate(config, new SleepingIdleStrategy(10));
    }

    public static FixEngine launchInitiatingGateway(final int initAeronPort)
    {
        delete(CLIENT_LOGS);
        return launchInitiatingGatewayWithSameLogs(initAeronPort);
    }

    public static FixEngine launchInitiatingGatewayWithSameLogs(final int initAeronPort)
    {
        final EngineConfiguration initiatingConfig = initiatingConfig(initAeronPort, "engineCounters");
        return FixEngine.launch(initiatingConfig);
    }

    public static EngineConfiguration initiatingConfig(
        final int initAeronPort,
        final String countersSuffix)
    {
        return new EngineConfiguration()
            .aeronChannel("udp://localhost:" + initAeronPort)
            .monitoringFile(IoUtil.tmpDirName() + "fix-client" + File.separator + countersSuffix)
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

    public static FixEngine launchAcceptingGateway(final int port)
    {
        delete(ACCEPTOR_LOGS);
        return launchAcceptingGatewayWithSameLogs(port);
    }

    public static FixEngine launchAcceptingGatewayWithSameLogs(final int port)
    {
        final EngineConfiguration config = acceptingConfig(port, "engineCounters");
        return FixEngine.launch(config);
    }

    public static EngineConfiguration acceptingConfig(
        final int port,
        final String countersSuffix)
    {
        return new EngineConfiguration()
            .bindTo("localhost", port)
            .aeronChannel("aeron:ipc")
            .monitoringFile(IoUtil.tmpDirName() + "fix-acceptor" + File.separator + countersSuffix)
            .logFileDir(ACCEPTOR_LOGS);
    }

    public static LibraryConfiguration acceptingLibraryConfig(
        final NewSessionHandler sessionHandler,
        final String acceptorId,
        final String initiatorId,
        final String monitorDir)
    {
        final MessageValidationStrategy validationStrategy = new TargetCompIdValidationStrategy(acceptorId)
            .and(new SenderCompIdValidationStrategy(Arrays.asList(initiatorId, INITIATOR_ID2)));

        final AuthenticationStrategy authenticationStrategy = AuthenticationStrategy.of(validationStrategy);

        return new LibraryConfiguration()
            .isAcceptor(true)
            .authenticationStrategy(authenticationStrategy)
            .messageValidationStrategy(validationStrategy)
            .newSessionHandler(sessionHandler)
            .aeronChannel("aeron:ipc")
            .monitoringFile(IoUtil.tmpDirName() + monitorDir + File.separator + "accLibraryCounters");
    }

    public static Session acceptSession(final FakeSessionHandler acceptingSessionHandler,
                                        final FixLibrary acceptingLibrary)
    {
        Session session;
        while ((session = acceptingSessionHandler.latestSession()) == null)
        {
            acceptingLibrary.poll(1);
            LockSupport.parkNanos(10_000);
        }
        acceptingSessionHandler.resetSession();
        return session;
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

    public static void closeIfOpen(final AutoCloseable closable)
    {
        try
        {
            if (closable != null)
            {
                closable.close();
            }
        }
        catch (final Exception ex)
        {
            ex.printStackTrace();
        }
    }

    public static FixLibrary newInitiatingLibrary(
        final int initAeronPort,
        final NewSessionHandler sessionHandler,
        final int libraryId)
    {
        return FixLibrary.connect(
            new LibraryConfiguration()
                .libraryId(libraryId)
                .newSessionHandler(sessionHandler)
                .aeronChannel("udp://localhost:" + initAeronPort)
                .monitoringFile(IoUtil.tmpDirName() + "fix-client" + File.separator + "libraryCounters-" + libraryId));
    }

    public static FixLibrary newAcceptingLibrary(final NewSessionHandler sessionHandler)
    {
        return FixLibrary.connect(
            acceptingLibraryConfig(sessionHandler, ACCEPTOR_ID, INITIATOR_ID, "fix-acceptor"));
    }

    public static void assertConnected(final Session session)
    {
        assertTrue("Session has failed to connect", session.isConnected());
    }

    public static void assertLibrariesDisconnect(final int count, final FixLibrary library, final FixEngine engine)
    {
        assertEventuallyTrue(
            "libraries haven't disconnected yet",
            () -> {
                if (library != null)
                {
                    library.poll(1);
                }
                return engine.libraries(ADMIN_IDLE_STRATEGY).size() == count;
            },
            AWAIT_TIMEOUT,
            1);
    }

    public static void awaitLibraryConnect(final FixLibrary library)
    {
        assertEventuallyTrue(
            "Library hasn't seen Engine", () ->
            {
                library.poll(5);
                final boolean connected = library.isConnected();
                return connected;
            },
            AWAIT_TIMEOUT, 1);
    }
}
