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
import java.net.InetSocketAddress;
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
        new Thread(this::pongs).start();
        pings();
    }

    private void pongs()
    {
        try (SocketChannel channel = serverSocket.accept())
        {
            System.out.println("Accepted");
            final Histogram histogram = new Histogram(100_000_000, 2);
            for (int i = 0; i < TIMES; i++)
            {
                final long time = System.nanoTime();
                pong(channel);
                histogram.recordValue(System.nanoTime() - time);
            }
            System.out.println("Finished Ponging");
            histogram.outputPercentileDistribution(System.out, 1.0);
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

            for (int i = 0; i < TIMES; i++)
            {
                ping(channel);
            }
            System.out.println("Finished Pinging");
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    protected abstract void ping(SocketChannel channel) throws IOException;

    protected abstract void pong(SocketChannel channel) throws IOException;
}
