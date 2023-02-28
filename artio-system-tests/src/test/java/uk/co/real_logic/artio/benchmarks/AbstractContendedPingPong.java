/*
 * Copyright 2015-2023 Real Logic Limited.
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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import static uk.co.real_logic.artio.benchmarks.NetworkBenchmarkUtil.*;

public abstract class AbstractContendedPingPong
{
    private static final ByteBuffer PONG_BUFFER = ByteBuffer.allocateDirect(MESSAGE_SIZE);

    private ServerSocketChannel serverSocket;

    private volatile boolean running = true;

    public void benchmark() throws Exception
    {
        serverSocket = ServerSocketChannel.open().bind(ADDRESS);

        try (SocketChannel pingChannel = SocketChannel.open())
        {
            if (!pingChannel.connect(ADDRESS))
            {
                System.err.println("Unable to connect");
            }
            pingChannel.configureBlocking(false);

            final Thread echoThread = new Thread(() ->
            {
                try (SocketChannel channel = serverSocket.accept())
                {
                    channel.configureBlocking(false);
                    System.out.println("Accepted");

                    while (running)
                    {
                        pong(channel);
                    }
                }
                catch (final IOException ex)
                {
                    ex.printStackTrace();
                }
            });

            final Thread pingSendingThread = new Thread(() ->
            {
                for (int i = 0; i < BENCHMARKS; i++)
                {
                    sendPings(pingChannel);
                }
            });

            final Thread responseReadingThread = new Thread(() ->
            {
                for (int i = 0; i < BENCHMARKS; i++)
                {
                    readResponses(pingChannel);
                }

                running = false;
            });

            echoThread.start();
            responseReadingThread.start();
            pingSendingThread.start();

            echoThread.join();
        }
    }

    private void readResponses(final SocketChannel channel)
    {
        try
        {
            final Histogram histogram = new Histogram(1_000_000_000, 4);

            for (int i = 0; i < ITERATIONS; i++)
            {
                final long time = readResponse(channel);
                final long value = System.nanoTime() - time;
                histogram.recordValue(value);
            }
            printStats(histogram);
        }
        catch (final IOException ex)
        {
            ex.printStackTrace();
        }
    }

    private void sendPings(final SocketChannel channel)
    {
        try
        {
            for (int i = 0; i < ITERATIONS; i++)
            {
                sendPing(channel, System.nanoTime());
                //LockSupport.parkNanos(1_000_000);
            }
            System.out.println("Sent all pings");
        }
        catch (final IOException ex)
        {
            ex.printStackTrace();
        }
    }

    protected abstract void sendPing(SocketChannel channel, long time) throws IOException;

    protected abstract long readResponse(SocketChannel channel) throws IOException;

    protected void pong(final SocketChannel channel) throws IOException
    {
        PONG_BUFFER.clear();
        int amountRead = channel.read(PONG_BUFFER);
        if (amountRead > 0)
        {
            PONG_BUFFER.flip();

            while (amountRead > 0)
            {
                amountRead -= channel.write(PONG_BUFFER);
            }
        }
    }
}
