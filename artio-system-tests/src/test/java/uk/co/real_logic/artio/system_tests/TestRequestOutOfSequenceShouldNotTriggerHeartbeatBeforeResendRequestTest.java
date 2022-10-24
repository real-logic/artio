/*
 * Copyright 2019 Adaptive Financial Consulting Ltd.
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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.validation.SessionPersistenceStrategy;

import java.io.IOException;
import java.util.concurrent.locks.LockSupport;

import static io.aeron.CommonContext.IPC_CHANNEL;
import static org.agrona.CloseHelper.close;
import static org.junit.Assert.assertEquals;
import static uk.co.real_logic.artio.TestFixtures.*;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.*;

public class TestRequestOutOfSequenceShouldNotTriggerHeartbeatBeforeResendRequestTest
{
    private final int port = unusedPort();

    private ArchivingMediaDriver mediaDriver;
    private FixEngine engine;

    @Before
    public void setUp()
    {
        mediaDriver = launchMediaDriver();

        delete(ACCEPTOR_LOGS);
        final EngineConfiguration config = new EngineConfiguration()
            .bindTo("localhost", port)
            .libraryAeronChannel(IPC_CHANNEL)
            .monitoringFile(acceptorMonitoringFile("engineCounters"))
            .logFileDir(ACCEPTOR_LOGS)
            .sessionPersistenceStrategy(SessionPersistenceStrategy.alwaysPersistent());
        configureAeronArchive(config.aeronArchiveContext());
        engine = FixEngine.launch(config);
    }

    @Test
    public void testRequestWithIncorrectSeqNumShouldNotTriggerHeartbeat() throws IOException
    {
        try (FixConnection connection = FixConnection.initiate(port))
        {
            connection.logon(true);
            connection.readLogon(1);

            connection.msgSeqNum(3);
            connection.sendTestRequest("firstRequest");

            // await resend request
            connection.readResendRequest(2, 0);

            LockSupport.parkNanos(500);

            assertEquals(0, connection.pollData());

            connection.logoutAndAwaitReply();
        }
    }

    @After
    public void tearDown()
    {
        close(engine);
        cleanupMediaDriver(mediaDriver);
    }
}
