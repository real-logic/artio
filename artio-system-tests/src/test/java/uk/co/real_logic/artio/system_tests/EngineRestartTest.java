/*
 * Copyright 2015-2018 Real Logic Ltd, Adaptive Financial Consulting Ltd.
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

import io.aeron.archive.ArchivingMediaDriver;
import org.junit.Test;
import uk.co.real_logic.artio.TestFixtures;
import uk.co.real_logic.artio.engine.FixEngine;

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
            try (FixEngine engine = SystemTestUtil.launchInitiatingEngine(port))
            {
            }

            try (FixEngine engine = SystemTestUtil.launchInitiatingEngineWithSameLogs(port))
            {
            }
        }
        finally
        {
            TestFixtures.cleanupMediaDriver(mediaDriver);
        }
    }
}
