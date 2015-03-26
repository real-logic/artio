/*
 * Copyright 2015 Real Logic Ltd.
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
package uk.co.real_logic.fix_gateway.benchmarks;

import org.HdrHistogram.Histogram;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

public abstract class AbstractPingPong
{
    private static final int PORT = 9999;
    private static final InetSocketAddress ADDRESS = new InetSocketAddress("localhost", PORT);
    private static final int TIMES = 1_000_000;

    protected static final int MESSAGE_SIZE = 40;

    private ServerSocketChannel serverSocket;

    public void benchmark() throws IOException
    {
        serverSocket = ServerSocketChannel.open().bind(ADDRESS);
        new Thread(() ->
        {
            pongs();
            System.out.println("Completed Warmup Run");

            pongs();
            System.out.println("Completed Benchmark Run");
        }).start();

        pings();
        pings();
    }

    private void pongs()
    {
        try (SocketChannel channel = serverSocket.accept())
        {
            channel.configureBlocking(false);

            for (int i = 0; i < TIMES; i++)
            {
                pong(channel);
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    private void pings()
    {
        try(SocketChannel channel = SocketChannel.open())
        {
            if (!channel.connect(ADDRESS))
            {
                System.err.println("Unable to connect");
            }
            channel.configureBlocking(false);

            final Histogram histogram = new Histogram(100_000_000, 2);
            for (int i = 0; i < TIMES; i++)
            {
                final long time = System.nanoTime();
                ping(channel);
                histogram.recordValue(System.nanoTime() - time);
            }
            histogram.outputPercentileDistribution(System.out, 1.0);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    protected abstract void ping(SocketChannel channel) throws IOException;

    protected abstract void pong(SocketChannel channel) throws IOException;

    protected void writeByteBuffer(final SocketChannel channel, final ByteBuffer buffer) throws IOException
    {
        buffer.position(0);
        int remaining = MESSAGE_SIZE;
        while (remaining > 0)
        {
            remaining -= channel.write(buffer);
        }
    }

    protected void readByteBuffer(final SocketChannel channel, final ByteBuffer buffer) throws IOException
    {
        int remaining = MESSAGE_SIZE;
        buffer.position(0);
        while (remaining > 0)
        {
            remaining -= channel.read(buffer);
        }
    }

    protected void writeChannel(final SocketChannel channel, final FileChannel buffer) throws IOException
    {
        int position = 0;
        while (position < MESSAGE_SIZE)
        {
            position += buffer.transferTo(position, MESSAGE_SIZE - position, channel);
        }
    }

    protected void readChannel(final SocketChannel channel, final FileChannel buffer) throws IOException
    {
        int position = 0;
        while (position < MESSAGE_SIZE)
        {
            position += buffer.transferFrom(channel, position, MESSAGE_SIZE - position);
        }
    }

    protected FileChannel newFileChannel(String filename)
    {
        try
        {
            RandomAccessFile file = new RandomAccessFile("/dev/shm/" + filename, "rw");
            file.write(new byte[MESSAGE_SIZE]);
            file.seek(0);
            return file.getChannel();
        }
        catch (IOException e)
        {
            e.printStackTrace();
            System.exit(1);
            return null;
        }
    }
}
