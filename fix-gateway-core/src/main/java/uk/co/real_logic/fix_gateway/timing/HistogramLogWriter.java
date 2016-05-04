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
package uk.co.real_logic.fix_gateway.timing;

import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.LangUtil;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.EpochClock;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.util.List;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;

public class HistogramLogWriter implements Agent
{
    private static final int BUFFER_SIZE = 1024 * 1024;

    private final List<Timer> timers;
    private final FileChannel logFile;
    private final long intervalInMs;
    private final ErrorHandler errorHandler;
    private final EpochClock milliClock;
    private final ByteBuffer buffer = ByteBuffer.allocateDirect(BUFFER_SIZE);

    private long nextWriteTimeInMs = 0;

    public HistogramLogWriter(
        final List<Timer> timers,
        final String logFile,
        final long intervalInMs,
        final ErrorHandler errorHandler,
        final EpochClock milliClock)
    {
        this.timers = timers;
        this.intervalInMs = intervalInMs;
        this.errorHandler = errorHandler;
        this.milliClock = milliClock;
        this.logFile = open(logFile);
        buffer.putInt(timers.size());
        timers.forEach(timer -> timer.writeName(buffer));
        try
        {
            writeBuffer();
        }
        catch (IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }

    private void writeBuffer() throws IOException
    {
        buffer.flip();
        logFile.write(buffer);
        logFile.force(true);
    }

    private FileChannel open(final String logFile)
    {
        try
        {
            return FileChannel.open(Paths.get(logFile), WRITE, CREATE, TRUNCATE_EXISTING);
        }
        catch (IOException e)
        {
            LangUtil.rethrowUnchecked(e);
            return null;
        }
    }

    public int doWork() throws Exception
    {
        final long currentTimeInMs = milliClock.time();

        if (currentTimeInMs > nextWriteTimeInMs)
        {
            logHistograms(currentTimeInMs);

            nextWriteTimeInMs = currentTimeInMs + intervalInMs;
            return 1;
        }

        return 0;
    }

    private void logHistograms(final long currentTimeInMs) throws IOException
    {
        buffer.clear();
        buffer.putLong(currentTimeInMs);
        timers.forEach(timer -> timer.writeTimings(buffer));
        writeBuffer();
    }

    public String roleName()
    {
        return "HistogramLogger";
    }

    public void onClose()
    {
        try
        {
            logFile.force(true);
        }
        catch (IOException ex)
        {
            errorHandler.onError(ex);
        }

        CloseHelper.close(logFile);
    }
}
