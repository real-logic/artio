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

public final class NioBufferPingPong extends AbstractPingPong
{
    private final ByteBuffer PING_BUFFER = ByteBuffer.allocate(MESSAGE_SIZE);
    private final ByteBuffer PONG_BUFFER = ByteBuffer.allocate(MESSAGE_SIZE);

    public static void main(String[] args) throws IOException
    {
        new NioBufferPingPong().benchmark();
    }

    protected void ping(SocketChannel channel) throws IOException
    {
        writeByteBuffer(channel, PING_BUFFER);

        readByteBuffer(channel, PING_BUFFER);
    }

    protected void pong(SocketChannel channel) throws IOException
    {
        readByteBuffer(channel, PONG_BUFFER);

        writeByteBuffer(channel, PONG_BUFFER);
    }
}
