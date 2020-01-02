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
import org.junit.Test;
import uk.co.real_logic.artio.TestFixtures;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.FixEngine;

import static uk.co.real_logic.artio.system_tests.SystemTestUtil.*;

public class EngineRestartTest
{
    @Test
    public void shouldRestartWithoutSessions()
    {
        ArchivingMediaDriver mediaDriver = null;
        try
        {
            mediaDriver = TestFixtures.launchMediaDriver();
            final int port = TestFixtures.unusedPort();
            try (FixEngine ignore = SystemTestUtil.launchInitiatingEngine(port))
            {
            }

            try (FixEngine ignore = launchInitiatingEngineWithSameLogs(port))
            {
            }
        }
        finally
        {
            TestFixtures.cleanupMediaDriver(mediaDriver);
        }
    }

    // This is a way to test the scenario that the engine has been shutdown improperly without notifying the AMD.
    @Test
    public void shouldRestartWhenStopRecordingFails()
    {
        ArchivingMediaDriver mediaDriver = null;
        try
        {
            mediaDriver = TestFixtures.launchMediaDriver();
            final int port = TestFixtures.unusedPort();
            delete(SystemTestUtil.CLIENT_LOGS);

            final EngineConfiguration firstInitiatingConfig = initiatingConfig(port);
            try (FixEngine ignore = FixEngine.launch(firstInitiatingConfig))
            {
                firstInitiatingConfig.logInboundMessages(false).logOutboundMessages(false);
            }

            final EngineConfiguration secondInitiatingConfig = initiatingConfig(port)
                .printStartupWarnings(false);
            try (FixEngine ignore = FixEngine.launch(secondInitiatingConfig))
            {
            }
        }
        finally
        {
            TestFixtures.cleanupMediaDriver(mediaDriver);
        }
    }
}
