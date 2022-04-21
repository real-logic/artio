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
package uk.co.real_logic.artio;

import io.aeron.archive.Archive;
import io.aeron.archive.ArchiveThreadingMode;
import io.aeron.archive.ArchivingMediaDriver;
import io.aeron.driver.MediaDriver;
import org.agrona.IoUtil;
import org.agrona.concurrent.YieldingIdleStrategy;

import java.io.File;
import java.io.IOException;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.nio.channels.AlreadyBoundException;
import java.nio.channels.DatagramChannel;
import java.nio.channels.ServerSocketChannel;
import java.util.Arrays;

import static io.aeron.driver.ThreadingMode.SHARED;

public final class TestFixtures
{
    private static final int LOW_PORT = 9999;
    private static final int HIGH_PORT = 99999;

    public static final int MESSAGE_BUFFER_SIZE_IN_BYTES = 15000;
    public static final int TERM_BUFFER_LENGTH = 16 * 1024 * 1024;

    private static int port = LOW_PORT;

    public static synchronized int unusedPort()
    {
        while (port < HIGH_PORT)
        {
            port++;

            if (portIsUnbound())
            {
                return port;
            }
        }

        throw new IllegalStateException("The test framework has run out of ports");
    }

    private static boolean portIsUnbound()
    {
        try
        {
            ServerSocketChannel
                .open()
                .bind(new InetSocketAddress("localhost", port))
                .close();

            DatagramChannel.open()
                .bind(new InetSocketAddress("localhost", port))
                .close();
            return true;
        }
        catch (final AlreadyBoundException | BindException e)
        {
            // not an error, deliberately blank
            return false;
        }
        catch (final IOException e)
        {
            e.printStackTrace();
            return false;
        }
    }

    public static ArchivingMediaDriver launchMediaDriver()
    {
        return launchMediaDriver(TERM_BUFFER_LENGTH);
    }

    public static MediaDriver launchJustMediaDriver()
    {
        return MediaDriver.launch(mediaDriverContext(TERM_BUFFER_LENGTH, true));
    }

    public static ArchivingMediaDriver launchMediaDriver(final int termBufferLength)
    {
        return launchMediaDriver(mediaDriverContext(termBufferLength, true));
    }

    public static ArchivingMediaDriver launchMediaDriverWithDirs()
    {
        return launchMediaDriver(mediaDriverContext(TERM_BUFFER_LENGTH, false));
    }

    public static ArchivingMediaDriver launchMediaDriver(final MediaDriver.Context context)
    {
        final Archive.Context archiveCtx = new Archive.Context()
            .deleteArchiveOnStart(context.dirDeleteOnStart())
            .segmentFileLength(context.ipcTermBufferLength());

        final ArchivingMediaDriver mediaDriver = ArchivingMediaDriver.launch(context, archiveCtx);
        archiveCtx.threadingMode(ArchiveThreadingMode.INVOKER);
        final String aeronDirectoryName = context.aeronDirectoryName();
        CloseChecker.onOpen(aeronDirectoryName, mediaDriver);

        return mediaDriver;
    }

    public static MediaDriver.Context mediaDriverContext(final int termBufferLength, final boolean dirsDeleteOnStart)
    {
        return new MediaDriver.Context()
            .useWindowsHighResTimer(true)
            .threadingMode(SHARED)
            .sharedIdleStrategy(new YieldingIdleStrategy())
            .dirDeleteOnStart(dirsDeleteOnStart)
            .warnIfDirectoryExists(false)
            .publicationTermBufferLength(termBufferLength)
            .ipcTermBufferLength(termBufferLength);
    }

    public static void cleanupMediaDriver(final ArchivingMediaDriver mediaDriver)
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

    public static String closeMediaDriver(final ArchivingMediaDriver archivingMediaDriver)
    {
        final String aeronDirectoryName = archivingMediaDriver.mediaDriver().aeronDirectoryName();
        CloseChecker.onClose(aeronDirectoryName, archivingMediaDriver);
        archivingMediaDriver.close();
        return aeronDirectoryName;
    }

    public static String largeTestReqId()
    {
        final char[] testReqIDChars = new char[MESSAGE_BUFFER_SIZE_IN_BYTES - 100];
        Arrays.fill(testReqIDChars, 'A');

        return new String(testReqIDChars);
    }
}
