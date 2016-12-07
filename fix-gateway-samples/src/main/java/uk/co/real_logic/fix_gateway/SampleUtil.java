package uk.co.real_logic.fix_gateway;

import uk.co.real_logic.fix_gateway.library.FixLibrary;
import uk.co.real_logic.fix_gateway.library.LibraryConfiguration;

public final class SampleUtil
{
    public static FixLibrary blockingConnect(final LibraryConfiguration configuration)
    {
        final FixLibrary library = FixLibrary.connect(configuration);
        while (!library.isConnected())
        {
            library.poll(1);
            Thread.yield();
        }
        return library;
    }
}
