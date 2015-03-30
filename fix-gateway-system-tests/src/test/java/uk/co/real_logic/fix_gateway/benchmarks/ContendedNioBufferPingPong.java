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

import static uk.co.real_logic.fix_gateway.benchmarks.NetworkBenchmarkUtil.MESSAGE_SIZE;
import static uk.co.real_logic.fix_gateway.benchmarks.NetworkBenchmarkUtil.readByteBuffer;
import static uk.co.real_logic.fix_gateway.benchmarks.NetworkBenchmarkUtil.writeByteBuffer;

public final class ContendedNioBufferPingPong extends AbstractContendedPingPong
{
    private final ByteBuffer SEND_PING_BUFFER = ByteBuffer.allocate(MESSAGE_SIZE);
    private final ByteBuffer READ_RESPONSE_BUFFER = ByteBuffer.allocate(MESSAGE_SIZE);
    private final ByteBuffer PONG_BUFFER = ByteBuffer.allocate(MESSAGE_SIZE);

    public static void main(String[] args) throws Exception
    {
        new ContendedNioBufferPingPong().benchmark();
    }

    protected synchronized void sendPing(final SocketChannel channel, final long time) throws IOException
    {
        System.out.println("WRITE");
        SEND_PING_BUFFER.putLong(0, time);
        writeByteBuffer(channel, SEND_PING_BUFFER);
        System.out.println("DONE");
    }

    protected synchronized long readResponse(final SocketChannel channel) throws IOException
    {
        System.out.println("READ");
        readByteBuffer(channel, READ_RESPONSE_BUFFER);
        return READ_RESPONSE_BUFFER.getLong(0);
    }

    protected void pong(final SocketChannel channel) throws IOException
    {
        readByteBuffer(channel, PONG_BUFFER);

        writeByteBuffer(channel, PONG_BUFFER);
    }
}
