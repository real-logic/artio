/*
 * Copyright 2015-2025 Real Logic Limited.
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
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.locks.LockSupport;

import static uk.co.real_logic.artio.benchmarks.NetworkBenchmarkUtil.BENCHMARKS;
import static uk.co.real_logic.artio.benchmarks.NetworkBenchmarkUtil.printStats;

public abstract class AbstractPingPong
{
    private ServerSocketChannel serverSocket;

    public void benchmark() throws IOException
    {
        serverSocket = ServerSocketChannel.open().bind(NetworkBenchmarkUtil.ADDRESS);
        new Thread(() ->
        {
            for (int i = 0; i < BENCHMARKS; i++)
            {
                System.gc();
                LockSupport.parkNanos(10 * 1000);
                pongs();
                System.out.println("Completed Run");
            }
        }).start();

        for (int i = 0; i < BENCHMARKS; i++)
        {
            pings();
        }
    }

    private void pongs()
    {
        try (SocketChannel channel = serverSocket.accept())
        {
            channel.configureBlocking(false);

            for (int i = 0; i < NetworkBenchmarkUtil.ITERATIONS; i++)
            {
                pong(channel);
            }
        }
        catch (final IOException ex)
        {
            ex.printStackTrace();
        }
    }

    private void pings()
    {
        try (SocketChannel channel = SocketChannel.open())
        {
            if (!channel.connect(NetworkBenchmarkUtil.ADDRESS))
            {
                System.err.println("Unable to connect");
            }
            channel.configureBlocking(false);

            final Histogram histogram = new Histogram(100_000_000, 2);
            for (int i = 0; i < NetworkBenchmarkUtil.ITERATIONS; i++)
            {
                final long time = System.nanoTime();
                ping(channel, time);
                histogram.recordValue(System.nanoTime() - time);
            }

            printStats(histogram);
        }
        catch (final IOException ex)
        {
            ex.printStackTrace();
        }
    }

    protected abstract void ping(SocketChannel channel, long time) throws IOException;

    protected abstract void pong(SocketChannel channel) throws IOException;
}
