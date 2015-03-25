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
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

public class NioBufferPingPong
{
    private static final int PORT = 9999;
    public static final InetSocketAddress ADDRESS = new InetSocketAddress("localhost", PORT);
    private static final int TIMES = 1_000_000;
    public static final int MESSAGE_SIZE = 40;

    private static final ByteBuffer PING_BUFFER = ByteBuffer.allocate(MESSAGE_SIZE);
    private static final ByteBuffer PONG_BUFFER = ByteBuffer.allocate(MESSAGE_SIZE);

    private static ServerSocketChannel serverSocket;

    public static void main(String[] args) throws IOException
    {
        serverSocket = ServerSocketChannel.open().bind(ADDRESS);
        new Thread(NioBufferPingPong::pong).start();
        ping();
    }

    private static void pong()
    {
        try (SocketChannel channel = serverSocket.accept())
        {
            System.out.println("Accepted");
            final Histogram histogram = new Histogram(100_000_000, 2);
            for (int i = 0; i < TIMES; i++)
            {
                final long time = System.nanoTime();
                read(channel, PONG_BUFFER);

                write(channel, PONG_BUFFER);
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

    private static void ping()
    {
        try(SocketChannel channel = SocketChannel.open())
        {
            if (!channel.connect(ADDRESS))
            {
                System.err.println("Unable to connect");
            }
            //channel.configureBlocking(false);

            for (int i = 0; i < TIMES; i++)
            {
                write(channel, PING_BUFFER);

                read(channel, PING_BUFFER);
            }
            System.out.println("Finished Pinging");
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    private static void write(SocketChannel channel, ByteBuffer buffer) throws IOException
    {
        buffer.position(0);
        int remaining = MESSAGE_SIZE;
        while (remaining > 0)
        {
            remaining -= channel.write(buffer);
        }
    }

    private static void read(SocketChannel channel, ByteBuffer buffer) throws IOException
    {
        int remaining = MESSAGE_SIZE;
        buffer.position(0);
        while (remaining > 0)
        {
            remaining -= channel.read(buffer);
        }
    }
}
