package uk.co.real_logic.fix_gateway.system_tests;

import uk.co.real_logic.aeron.driver.MediaDriver;

import static uk.co.real_logic.aeron.driver.ThreadingMode.SHARED;

public final class SystemTestUtil
{
    public static MediaDriver launchMediaDriver()
    {
        return MediaDriver.launch(new MediaDriver.Context().threadingMode(SHARED));
    }
}
