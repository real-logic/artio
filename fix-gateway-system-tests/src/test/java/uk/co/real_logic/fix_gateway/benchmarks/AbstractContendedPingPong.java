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
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import static uk.co.real_logic.fix_gateway.benchmarks.NetworkBenchmarkUtil.ADDRESS;
import static uk.co.real_logic.fix_gateway.benchmarks.NetworkBenchmarkUtil.TIMES;

public abstract class AbstractContendedPingPong
{

    private ServerSocketChannel serverSocket;

    public void benchmark() throws Exception
    {
        serverSocket = ServerSocketChannel.open().bind(ADDRESS);

        try(final SocketChannel pingChannel = SocketChannel.open())
        {
            if (!pingChannel.connect(ADDRESS))
            {
                System.err.println("Unable to connect");
            }
            pingChannel.configureBlocking(false);

            final Thread pingSendingThread = new Thread(() ->
            {
                sendPings(pingChannel);

                sendPings(pingChannel);
            });

            final Thread responseReadingThread = new Thread(() ->
            {
                readResponses(pingChannel);

                readResponses(pingChannel);
            });

            pingSendingThread.start();
            responseReadingThread.start();

            warmupAndPong();

            pingSendingThread.join();
            responseReadingThread.join();
        }
    }

    private void warmupAndPong()
    {
        try (SocketChannel channel = serverSocket.accept())
        {
            pongs(channel);
            System.out.println("Completed Warmup Run");

            pongs(channel);
            System.out.println("Completed Benchmark Run");
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    private void pongs(SocketChannel channel) throws IOException
    {
        for (int i = 0; i < TIMES; i++)
        {
            pong(channel);
        }
    }

    private void readResponses(final SocketChannel channel)
    {
        try
        {
            final Histogram histogram = new Histogram(100_000_000, 2);

            for (int i = 0; i < TIMES; i++)
            {
                final long time = readResponse(channel);
                histogram.recordValue(System.nanoTime() - time);
            }
            histogram.outputPercentileDistribution(System.out, 1.0);
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }

    }

    private void sendPings(final SocketChannel channel)
    {
        try
        {
            for (int i = 0; i < TIMES; i++)
            {
                sendPing(channel, System.nanoTime());
            }mosmo
            System.out.println("Sent all pings");
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    protected abstract void sendPing(SocketChannel channel, long time) throws IOException;

    protected abstract long readResponse(SocketChannel channel) throws IOException;

    protected abstract void pong(SocketChannel channel) throws IOException;

}
