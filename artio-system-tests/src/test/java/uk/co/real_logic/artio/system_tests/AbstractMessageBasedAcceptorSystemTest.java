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
import org.junit.After;
import uk.co.real_logic.artio.decoder.LogonDecoder;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.library.FixLibrary;
import uk.co.real_logic.artio.messages.InitialAcceptedSessionOwner;

import static io.aeron.CommonContext.IPC_CHANNEL;
import static org.agrona.CloseHelper.close;
import static org.junit.Assert.assertTrue;
import static uk.co.real_logic.artio.TestFixtures.*;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.*;
import static uk.co.real_logic.artio.validation.PersistenceLevel.PERSISTENT_SEQUENCE_NUMBERS;
import static uk.co.real_logic.artio.validation.PersistenceLevel.TRANSIENT_SEQUENCE_NUMBERS;

public class AbstractMessageBasedAcceptorSystemTest
{
    int port = unusedPort();

    ArchivingMediaDriver mediaDriver;
    FixEngine engine;
    FakeOtfAcceptor otfAcceptor;
    FakeHandler handler;
    FixLibrary library;
    TestSystem testSystem;

    void setup(final boolean sequenceNumberReset, final boolean shouldBind)
    {
        setup(sequenceNumberReset, shouldBind, true);
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
        mediaDriver = launchMediaDriver();

        delete(ACCEPTOR_LOGS);
        final EngineConfiguration config = new EngineConfiguration()
            .libraryAeronChannel(IPC_CHANNEL)
            .monitoringFile(acceptorMonitoringFile("engineCounters"))
            .logFileDir(ACCEPTOR_LOGS)
            .initialAcceptedSessionOwner(initialAcceptedSessionOwner)
            .noLogonDisconnectTimeoutInMs(500)
            .replyTimeoutInMs(TEST_REPLY_TIMEOUT_IN_MS)
            .sessionPersistenceStrategy(logon ->
            sequenceNumberReset ? TRANSIENT_SEQUENCE_NUMBERS : PERSISTENT_SEQUENCE_NUMBERS);

        if (provideBindingAddress)
        {
            config.bindTo("localhost", port);
        }

        config.bindAtStartup(shouldBind);

        config
            .printErrorMessages(false)
            .defaultHeartbeatIntervalInS(1);
        engine = FixEngine.launch(config);
    }

    void logon(final FixConnection connection)
    {
        connection.logon(true);

        final LogonDecoder logon = connection.readLogonReply();
        assertTrue(logon.resetSeqNumFlag());
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
    }
}
