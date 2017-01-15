package uk.co.real_logic.fix_gateway.system_tests;

import io.aeron.driver.MediaDriver;
import org.junit.Test;
import uk.co.real_logic.fix_gateway.TestFixtures;
import uk.co.real_logic.fix_gateway.engine.FixEngine;

public class EngineRestartTest
{
    @Test
    public void shouldRestartWithoutSessions() throws Exception
    {
        MediaDriver mediaDriver = null;
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
