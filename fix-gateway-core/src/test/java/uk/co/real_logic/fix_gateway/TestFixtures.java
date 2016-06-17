/*
 * Copyright 2015-2016 Real Logic Ltd.
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
import org.agrona.IoUtil;

import java.io.File;

import static io.aeron.driver.ThreadingMode.SHARED;

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

    public static MediaDriver launchMediaDriver(int termBufferLength)
    {
        final MediaDriver.Context context = mediaDriverContext(termBufferLength);

        return MediaDriver.launch(context);
    }

    public static MediaDriver.Context mediaDriverContext(final int termBufferLength)
    {
        return new MediaDriver.Context()
            .threadingMode(SHARED)
            .dirsDeleteOnStart(true)
            .publicationTermBufferLength(termBufferLength)
            .ipcTermBufferLength(termBufferLength);
    }

    public static void cleanupDirectory(final MediaDriver mediaDriver)
    {
        if (mediaDriver != null)
        {
            IoUtil.delete(new File(mediaDriver.aeronDirectoryName()), false);
        }
    }

    public static String clusteredAeronChannel()
    {
        return "aeron:udp?group=224.0.1.1:" + unusedPort();
    }
}
