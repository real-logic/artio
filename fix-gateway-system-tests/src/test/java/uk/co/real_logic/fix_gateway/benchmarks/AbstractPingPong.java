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

public abstract class AbstractPingPong
{

    private ServerSocketChannel serverSocket;

    public void benchmark() throws IOException
    {
        serverSocket = ServerSocketChannel.open().bind(NetworkBenchmarkUtil.ADDRESS);
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

            for (int i = 0; i < NetworkBenchmarkUtil.TIMES; i++)
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
            if (!channel.connect(NetworkBenchmarkUtil.ADDRESS))
            {
                System.err.println("Unable to connect");
            }
            channel.configureBlocking(false);

            final Histogram histogram = new Histogram(100_000_000, 2);
            for (int i = 0; i < NetworkBenchmarkUtil.TIMES; i++)
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

}
