/*
 * Copyright 2015 Real Logic Ltd.
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
import uk.co.real_logic.aeron.driver.MediaDriver;
import uk.co.real_logic.agrona.IoUtil;
import uk.co.real_logic.agrona.concurrent.SleepingIdleStrategy;
import uk.co.real_logic.fix_gateway.StaticConfiguration;
import uk.co.real_logic.fix_gateway.builder.TestRequestEncoder;
import uk.co.real_logic.fix_gateway.decoder.TestRequestDecoder;
import uk.co.real_logic.fix_gateway.engine.FixEngine;
import uk.co.real_logic.fix_gateway.library.FixLibrary;
import uk.co.real_logic.fix_gateway.library.SessionConfiguration;
import uk.co.real_logic.fix_gateway.library.auth.AuthenticationStrategy;
import uk.co.real_logic.fix_gateway.library.auth.SenderCompIdAuthenticationStrategy;
import uk.co.real_logic.fix_gateway.library.auth.TargetCompIdAuthenticationStrategy;
import uk.co.real_logic.fix_gateway.library.session.NewSessionHandler;
import uk.co.real_logic.fix_gateway.library.session.Session;

import java.io.File;
import java.util.Arrays;
import java.util.concurrent.locks.LockSupport;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static uk.co.real_logic.aeron.driver.ThreadingMode.SHARED;
import static uk.co.real_logic.fix_gateway.TestFixtures.unusedPort;
import static uk.co.real_logic.fix_gateway.Timing.assertEventuallyTrue;
import static uk.co.real_logic.fix_gateway.library.session.SessionState.ACTIVE;
import static uk.co.real_logic.fix_gateway.library.session.SessionState.DISCONNECTED;

public final class SystemTestUtil
{
    public static final long CONNECTION_ID = 0L;
    public static final String ACCEPTOR_ID = "CCG";
    public static final String INITIATOR_ID = "LEH_LZJ02";
    public static final String CLIENT_LOGS = "client-logs";
    public static final String ACCEPTOR_LOGS = "acceptor-logs";

    public static MediaDriver launchMediaDriver()
    {
        final MediaDriver.Context context = new MediaDriver.Context()
            .threadingMode(SHARED)
            .dirsDeleteOnStart(true);

        return MediaDriver.launch(context);
    }

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

    public static void assertReceivedMessage(
        final FixLibrary library, final FakeOtfAcceptor acceptor)
    {
        assertReceivedMessage(library, null, acceptor);
    }

    public static void assertReceivedMessage(
        final FixLibrary library1, final FixLibrary library2, final FakeOtfAcceptor acceptor)
    {
        assertEventuallyTrue("Failed to receive a logon and test request message", () ->
        {
            poll(library1, library2);
            assertEquals(2, acceptor.messageTypes().size());
            assertThat(acceptor.messageTypes(), hasItem(TestRequestDecoder.MESSAGE_TYPE));
        });
    }

    private static void poll(final FixLibrary library1, final FixLibrary library2)
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

    public static FixEngine launchInitiatingGateway(final NewSessionHandler sessionHandler, final int initAeronPort)
    {
        delete(CLIENT_LOGS);
        final StaticConfiguration initiatingConfig = initiatingConfig(sessionHandler, initAeronPort, "engineCounters");
        return FixEngine.launch(initiatingConfig);
    }

    public static StaticConfiguration initiatingConfig(
        final NewSessionHandler sessionHandler,
        final int initAeronPort,
        final String countersSuffix)
    {
        return new StaticConfiguration()
            .bind("localhost", unusedPort())
            .aeronChannel("udp://localhost:" + initAeronPort)
            .newSessionHandler(sessionHandler)
            .counterBuffersFile(IoUtil.tmpDirName() + "fix-client" + File.separator + countersSuffix)
            .logFileDir(CLIENT_LOGS);
    }

    private static void delete(final String dirPath)
    {
        final File dir = new File(dirPath);
        if (dir.exists())
        {
            IoUtil.delete(dir, false);
        }
    }

    public static FixEngine launchAcceptingGateway(
        final int port,
        final NewSessionHandler sessionHandler,
        final String acceptorId,
        final String initiatorId,
        final int acceptAeronPort)
    {
        delete(ACCEPTOR_LOGS);
        final StaticConfiguration config = acceptingConfig(
            port, sessionHandler, acceptorId, initiatorId, acceptAeronPort, "engineCounters");
        return FixEngine.launch(config);
    }

    public static StaticConfiguration acceptingConfig(
        final int port,
        final NewSessionHandler sessionHandler,
        final String acceptorId,
        final String initiatorId,
        final int acceptAeronPort,
        final String countersSuffix)
    {
        final AuthenticationStrategy authenticationStrategy = new TargetCompIdAuthenticationStrategy(acceptorId)
                .and(new SenderCompIdAuthenticationStrategy(Arrays.asList(initiatorId)));

        return new StaticConfiguration()
            .bind("localhost", port)
            .aeronChannel("udp://localhost:" + acceptAeronPort)
            .authenticationStrategy(authenticationStrategy)
            .newSessionHandler(sessionHandler)
            .counterBuffersFile(IoUtil.tmpDirName() + "fix-acceptor" + File.separator + countersSuffix)
            .logFileDir(ACCEPTOR_LOGS);
    }

    public static Session acceptSession(final FakeSessionHandler acceptingSessionHandler, final FixLibrary acceptingLibrary)
    {
        Session session;
        while ((session = acceptingSessionHandler.session()) == null)
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
}
