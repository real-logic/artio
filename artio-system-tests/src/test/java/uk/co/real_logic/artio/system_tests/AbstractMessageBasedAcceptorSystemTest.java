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

import io.aeron.archive.ArchivingMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.EpochNanoClock;
import org.agrona.concurrent.OffsetEpochNanoClock;
import org.junit.After;
import uk.co.real_logic.artio.CommonConfiguration;
import uk.co.real_logic.artio.MonitoringAgentFactory;
import uk.co.real_logic.artio.decoder.LogonDecoder;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.library.FixLibrary;
import uk.co.real_logic.artio.library.LibraryConfiguration;
import uk.co.real_logic.artio.messages.InitialAcceptedSessionOwner;
import uk.co.real_logic.artio.session.Session;
import uk.co.real_logic.artio.validation.AuthenticationStrategy;
import uk.co.real_logic.artio.validation.MessageValidationStrategy;

import static io.aeron.CommonContext.IPC_CHANNEL;
import static org.agrona.CloseHelper.close;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static uk.co.real_logic.artio.TestFixtures.*;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.*;
import static uk.co.real_logic.artio.validation.PersistenceLevel.PERSISTENT_SEQUENCE_NUMBERS;
import static uk.co.real_logic.artio.validation.PersistenceLevel.TRANSIENT_SEQUENCE_NUMBERS;

public class AbstractMessageBasedAcceptorSystemTest
{
    public static final int TEST_THROTTLE_WINDOW_IN_MS = 300;
    public static final int THROTTLE_MSG_LIMIT = 3;
    public static final int RESET_THROTTLE_MSG_LIMIT = 5;

    ErrorHandler errorHandler = mock(ErrorHandler.class);
    int port = unusedPort();

    final EpochNanoClock nanoClock = new OffsetEpochNanoClock();

    long reasonableTransmissionTimeInMs = CommonConfiguration.DEFAULT_REASONABLE_TRANSMISSION_TIME_IN_MS;

    AuthenticationStrategy optionalAuthStrategy;
    ArchivingMediaDriver mediaDriver;
    FixEngine engine;
    FakeOtfAcceptor otfAcceptor;
    FakeHandler handler;
    FixLibrary library;
    TestSystem testSystem;
    Session session;

    void setup(final boolean sequenceNumberReset, final boolean shouldBind)
    {
        setup(sequenceNumberReset, shouldBind, true);
    }

    void setupLibrary()
    {
        otfAcceptor = new FakeOtfAcceptor();
        handler = new FakeHandler(otfAcceptor);
        final LibraryConfiguration configuration = acceptingLibraryConfig(handler, nanoClock);
        configuration.messageValidationStrategy(MessageValidationStrategy.none());
        configuration.errorHandlerFactory(errorBuffer -> errorHandler);
        library = connect(configuration);
        testSystem = new TestSystem(library);
    }

    void setup(
        final boolean sequenceNumberReset,
        final boolean shouldBind,
        final boolean provideBindingAddress)
    {
        setup(sequenceNumberReset, shouldBind, provideBindingAddress, InitialAcceptedSessionOwner.ENGINE);
    }

    void setup(
        final boolean sequenceNumberReset,
        final boolean shouldBind,
        final boolean provideBindingAddress,
        final InitialAcceptedSessionOwner initialAcceptedSessionOwner)
    {
        setup(sequenceNumberReset, shouldBind, provideBindingAddress, initialAcceptedSessionOwner, false);
    }

    void setup(
        final boolean sequenceNumberReset,
        final boolean shouldBind,
        final boolean provideBindingAddress,
        final InitialAcceptedSessionOwner initialAcceptedSessionOwner,
        final boolean enableThrottle)
    {
        mediaDriver = launchMediaDriver();

        delete(ACCEPTOR_LOGS);
        final EngineConfiguration config = new EngineConfiguration()
            .libraryAeronChannel(IPC_CHANNEL)
            .monitoringFile(acceptorMonitoringFile("engineCounters"))
            .logFileDir(ACCEPTOR_LOGS)
            .initialAcceptedSessionOwner(initialAcceptedSessionOwner)
            .noLogonDisconnectTimeoutInMs(500)
            .slowConsumerTimeoutInMs(TEST_REPLY_TIMEOUT_IN_MS)
            .replyTimeoutInMs(TEST_REPLY_TIMEOUT_IN_MS)
            .reasonableTransmissionTimeInMs(reasonableTransmissionTimeInMs)
            .sessionPersistenceStrategy(logon ->
            sequenceNumberReset ? TRANSIENT_SEQUENCE_NUMBERS : PERSISTENT_SEQUENCE_NUMBERS);

        if (optionalAuthStrategy != null)
        {
            config.authenticationStrategy(optionalAuthStrategy);
        }

        if (enableThrottle)
        {
            config.enableMessageThrottle(TEST_THROTTLE_WINDOW_IN_MS, THROTTLE_MSG_LIMIT);
        }

        if (provideBindingAddress)
        {
            config.bindTo("localhost", port);
        }

        config.bindAtStartup(shouldBind);

        config
            .monitoringAgentFactory(MonitoringAgentFactory.none())
            .defaultHeartbeatIntervalInS(1);
        engine = FixEngine.launch(config);
    }

    void awaitedLogon(final FixConnection connection)
    {
        testSystem.awaitBlocking(() -> logon(connection));
    }

    void logon(final FixConnection connection)
    {
        connection.logon(true);

        final LogonDecoder logon = connection.readLogon();
        assertTrue(logon.resetSeqNumFlag());
    }

    Session acquireSession()
    {
        final long sessionId = handler.awaitSessionId(testSystem::poll);
        handler.clearSessionExistsInfos();
        final Session session = SystemTestUtil.acquireSession(handler, library, sessionId, testSystem);
        assertNotNull(session);
        return session;
    }

    @After
    public void tearDown()
    {
        if (testSystem == null)
        {
            close(engine);
        }
        else
        {
            testSystem.awaitBlocking(() -> CloseHelper.close(engine));
        }

        close(library);

        cleanupMediaDriver(mediaDriver);

        verifyNoMoreInteractions(errorHandler);
    }
}
