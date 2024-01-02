/*
 * Copyright 2015-2024 Real Logic Limited.
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
package uk.co.real_logic.artio.benchmarks;

import org.HdrHistogram.Histogram;
import org.agrona.IoUtil;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;

public final class NetworkBenchmarkUtil
{
    public static final int MESSAGE_SIZE = 1024;
    public static final int PORT = 9999;
    public static final InetSocketAddress ADDRESS = new InetSocketAddress("localhost", PORT);
    public static final int BENCHMARKS = 10;
    public static final int ITERATIONS = 250_000;

    public static void writeChannel(
        final SocketChannel destination,
        final FileChannel source,
        final MappedByteBuffer buffer,
        final long time) throws IOException
    {
        if (null != buffer)
        {
            buffer.putLong(0, time);
        }

        long position = 0;
        while (position < MESSAGE_SIZE)
        {
            position += source.transferTo(position, MESSAGE_SIZE - position, destination);
        }
    }

    public static long readChannel(final SocketChannel channel, final FileChannel buffer) throws IOException
    {
        long position = 0;
        while (position < MESSAGE_SIZE)
        {
            position += buffer.transferFrom(channel, position, MESSAGE_SIZE - position);
        }

        // TODO?
        return 0;
    }

    public static FileChannel newFile(final String filename)
    {
        FileChannel fileChannel = null;
        try
        {
            fileChannel = IoUtil.createEmptyFile(new File("/dev/shm/" + filename), MESSAGE_SIZE, true);
        }
        catch (final Exception ex)
        {
            ex.printStackTrace();
            System.exit(1);
        }

        return fileChannel;
    }

    public static void writeByteBuffer(final SocketChannel channel, final ByteBuffer buffer, final long value)
        throws IOException
    {
        buffer.putLong(0, value);
        buffer.clear();

        int remaining = MESSAGE_SIZE;
        while (remaining > 0)
        {
            remaining -= channel.write(buffer);
        }
    }

    public static long readByteBuffer(final SocketChannel channel, final ByteBuffer buffer) throws IOException
    {
        buffer.clear();

        int remaining = MESSAGE_SIZE;
        while (remaining > 0)
        {
            remaining -= channel.read(buffer);
        }

        return buffer.getLong(0);
    }

    public static void checkEqual(final long time, final long result)
    {
        if (time != result)
        {
            throw new IllegalStateException(String.format("Expected %d, Actual: %d", time, result));
        }
    }

    public static void printStats(final Histogram histogram)
    {
        System.out.printf(
            "Max = %d, Mean = %f, 99.9%% = %d%n",
            histogram.getMaxValue(),
            histogram.getMean(),
            histogram.getValueAtPercentile(99.9));
    }
}
