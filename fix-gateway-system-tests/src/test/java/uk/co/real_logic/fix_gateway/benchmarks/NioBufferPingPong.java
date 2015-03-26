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
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

public class NioBufferPingPong extends AbstractPingPong
{
    private final ByteBuffer PING_BUFFER = ByteBuffer.allocate(MESSAGE_SIZE);
    private final ByteBuffer PONG_BUFFER = ByteBuffer.allocate(MESSAGE_SIZE);

    public static void main(String[] args) throws IOException
    {
        new NioBufferPingPong().benchmark();
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

    private void write(final SocketChannel channel, final ByteBuffer buffer) throws IOException
    {
        buffer.position(0);
        int remaining = MESSAGE_SIZE;
        while (remaining > 0)
        {
            remaining -= channel.write(buffer);
        }
    }

    private void read(final SocketChannel channel, final ByteBuffer buffer) throws IOException
    {
        int remaining = MESSAGE_SIZE;
        buffer.position(0);
        while (remaining > 0)
        {
            remaining -= channel.read(buffer);
        }
    }
}
