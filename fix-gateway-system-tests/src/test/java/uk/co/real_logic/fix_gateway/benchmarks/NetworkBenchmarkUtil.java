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

import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;

public final class NetworkBenchmarkUtil
{
    public static final int MESSAGE_SIZE = 40;
    public static final int PORT = 9999;
    public static final InetSocketAddress ADDRESS = new InetSocketAddress("localhost", PORT);
    public static final int TIMES = 2;

    public static void writeChannel(final SocketChannel channel, final FileChannel buffer) throws IOException
    {
        int position = 0;
        while (position < MESSAGE_SIZE)
        {
            position += buffer.transferTo(position, MESSAGE_SIZE - position, channel);
        }
    }

    public static void readChannel(final SocketChannel channel, final FileChannel buffer) throws IOException
    {
        int position = 0;
        while (position < MESSAGE_SIZE)
        {
            position += buffer.transferFrom(channel, position, MESSAGE_SIZE - position);
        }
    }

    public static FileChannel newFileChannel(String filename)
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

    public static void writeByteBuffer(final SocketChannel channel, final ByteBuffer buffer) throws IOException
    {
        buffer.position(0);
        channel.write(buffer);
        int remaining = MESSAGE_SIZE;
        while (remaining > 0)
        {
            remaining -= channel.write(buffer);
            Thread.yield();
        }
    }

    public static void readByteBuffer(final SocketChannel channel, final ByteBuffer buffer) throws IOException
    {
        int remaining = MESSAGE_SIZE;
        buffer.position(0);
        buffer.limit(MESSAGE_SIZE);
        while (remaining > 0)
        {
            remaining -= channel.read(buffer);
            Thread.yield();
        }
    }
}
