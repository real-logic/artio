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
package uk.co.real_logic.fix_gateway.system_benchmarks;

import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.builder.HeaderEncoder;
import uk.co.real_logic.fix_gateway.builder.LogonEncoder;
import uk.co.real_logic.fix_gateway.util.MutableAsciiFlyweight;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import static java.net.StandardSocketOptions.TCP_NODELAY;

public final class FixBenchmarkClient
{
    private static final String HOST = System.getProperty("fix.benchmark.host", "localhost");
    private static final int PORT = Integer.getInteger("fix.benchmark.port", 9999);
    private static final ByteBuffer BUFFER = ByteBuffer.allocateDirect(16 * 1024);
    private static final UnsafeBuffer UNSAFE_BUFFER = new UnsafeBuffer(BUFFER);
    private static final MutableAsciiFlyweight ASCII_FLYWEIGHT = new MutableAsciiFlyweight(UNSAFE_BUFFER);

    public static void main(String[] args) throws IOException
    {
        final SocketChannel socketChannel = SocketChannel.open(new InetSocketAddress(HOST, PORT));
        socketChannel.setOption(TCP_NODELAY, true);
        socketChannel.configureBlocking(false);

        final LogonEncoder encoder = new LogonEncoder();
        final HeaderEncoder header = encoder.header();
        // header.senderCompID();
        final int length = encoder.encode(ASCII_FLYWEIGHT, 0);
        writeBuffer(socketChannel, length);
    }

    private static void writeBuffer(final SocketChannel socketChannel, final int amount) throws IOException
    {
        BUFFER.position(0);
        BUFFER.limit(amount);
        socketChannel.write(BUFFER);
    }
}
