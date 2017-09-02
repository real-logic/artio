/*
 * Copyright 2015-2017 Real Logic Ltd.
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
package uk.co.real_logic.fix_gateway;

import io.aeron.driver.MediaDriver;
import org.agrona.ErrorHandler;
import org.agrona.IoUtil;
import org.agrona.concurrent.YieldingIdleStrategy;

import java.io.File;

import static io.aeron.driver.ThreadingMode.SHARED;
import static org.mockito.Mockito.spy;

public final class TestFixtures
{
    private static final int LOW_PORT = 9999;
    private static final int HIGH_PORT = 99999;
    public static final int TERM_BUFFER_LENGTH = 4 * 1024 * 1024;

    private static int port = LOW_PORT;

    public static int unusedPort()
    {
        if (port < HIGH_PORT)
        {
            return port++;
        }

        throw new IllegalStateException("The test framework has run out of ports");
    }

    public static MediaDriver launchMediaDriver()
    {
        return launchMediaDriver(TERM_BUFFER_LENGTH);
    }

    public static MediaDriver launchMediaDriver(final int termBufferLength)
    {
        return launchMediaDriver(mediaDriverContext(termBufferLength, true));
    }

    public static MediaDriver launchMediaDriver(final MediaDriver.Context context)
    {
        final MediaDriver mediaDriver = MediaDriver.launch(context);
        final String aeronDirectoryName = context.aeronDirectoryName();
        CloseChecker.onOpen(aeronDirectoryName, mediaDriver);

        return mediaDriver;
    }

    public static MediaDriver.Context mediaDriverContext(
        final int termBufferLength, final boolean dirsDeleteOnStart)
    {
        return new MediaDriver.Context()
            .useWindowsHighResTimer(true)
            .threadingMode(SHARED)
            .sharedIdleStrategy(new YieldingIdleStrategy())
            .dirDeleteOnStart(dirsDeleteOnStart)
            .publicationTermBufferLength(termBufferLength)
            .ipcTermBufferLength(termBufferLength);
    }

    public static void cleanupMediaDriver(final MediaDriver mediaDriver)
    {
        if (mediaDriver != null)
        {
            final String aeronDirectoryName = closeMediaDriver(mediaDriver);

            final File directory = new File(aeronDirectoryName);
            if (directory.exists())
            {
                CloseChecker.validate(aeronDirectoryName);
                IoUtil.delete(directory, false);
            }
        }
    }

    public static String closeMediaDriver(final MediaDriver mediaDriver)
    {
        final String aeronDirectoryName = mediaDriver.aeronDirectoryName();
        CloseChecker.onClose(aeronDirectoryName, mediaDriver);
        mediaDriver.close();
        return aeronDirectoryName;
    }

    public static String clusteredAeronChannel()
    {
        return "aeron:udp?endpoint=224.0.1.1:" + unusedPort();
    }

    public static ErrorHandler printingMockErrorHandler()
    {
        return spy(Throwable::printStackTrace);
    }
}
