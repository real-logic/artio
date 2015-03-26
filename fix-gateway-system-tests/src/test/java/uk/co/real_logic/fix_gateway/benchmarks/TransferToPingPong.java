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
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;

public class TransferToPingPong extends AbstractPingPong
{
    private final FileChannel PING_BUFFER = newFileChannel("ping");

    private final FileChannel PONG_BUFFER = newFileChannel("pong");

    public static void main(String[] args) throws IOException
    {

        new TransferToPingPong().benchmark();
    }

    protected void ping(SocketChannel channel) throws IOException
    {
        write(channel, PING_BUFFER);

        read(channel, PING_BUFFER);
    }

    protected void pong(SocketChannel channel) throws IOException
    {
        read(channel, PONG_BUFFER);

        write(channel, PONG_BUFFER);
    }

    private void write(final SocketChannel channel, final FileChannel buffer) throws IOException
    {
        int position = 0;
        while (position < MESSAGE_SIZE)
        {
            position += buffer.transferTo(position, MESSAGE_SIZE - position, channel);
        }
    }

    private void read(final SocketChannel channel, final FileChannel buffer) throws IOException
    {
        int position = 0;
        while (position < MESSAGE_SIZE)
        {
            position += buffer.transferFrom(channel, position, MESSAGE_SIZE - position);
        }
    }

    private FileChannel newFileChannel(String filename)
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
