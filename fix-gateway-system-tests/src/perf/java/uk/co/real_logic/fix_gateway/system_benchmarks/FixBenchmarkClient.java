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

import org.HdrHistogram.Histogram;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.builder.HeaderEncoder;
import uk.co.real_logic.fix_gateway.builder.LogonEncoder;
import uk.co.real_logic.fix_gateway.builder.TestRequestEncoder;
import uk.co.real_logic.fix_gateway.decoder.LogonDecoder;
import uk.co.real_logic.fix_gateway.util.MutableAsciiFlyweight;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.concurrent.locks.LockSupport;

import static java.net.StandardSocketOptions.TCP_NODELAY;
import static uk.co.real_logic.fix_gateway.system_benchmarks.Configuration.*;

public final class FixBenchmarkClient
{
    private static final String HOST = System.getProperty("fix.benchmark.host", "localhost");
    private static final int BUFFER_SIZE = 16 * 1024;

    private static final ByteBuffer WRITE_BUFFER = ByteBuffer.allocateDirect(BUFFER_SIZE);
    private static final MutableAsciiFlyweight WRITE_FLYWEIGHT =
        new MutableAsciiFlyweight(new UnsafeBuffer(WRITE_BUFFER));

    private static final ByteBuffer READ_BUFFER = ByteBuffer.allocateDirect(BUFFER_SIZE);
    private static final MutableAsciiFlyweight READ_FLYWEIGHT =
        new MutableAsciiFlyweight(new UnsafeBuffer(READ_BUFFER));

    public static void main(String[] args) throws IOException, InterruptedException
    {
        try (final SocketChannel socketChannel = SocketChannel.open(new InetSocketAddress(HOST, PORT)))
        {
            socketChannel.setOption(TCP_NODELAY, true);

            logon(socketChannel);

            final TestRequestEncoder testRequest = new TestRequestEncoder();
            final HeaderEncoder header = testRequest
                .header()
                .senderCompID(INITIATOR_ID)
                .targetCompID(ACCEPTOR_ID);

            testRequest.testReqID("a");

            final Histogram histogram = new Histogram(3);

            for (int i = 0; i < WARMUP_MESSAGES; i++)
            {
                exchangeMessage(socketChannel, testRequest, header, i, histogram);
            }

            LockSupport.parkNanos(1_000_000_000);
            histogram.reset();

            for (int i = 0; i < MESSAGES_EXCHANGED; i++)
            {
                exchangeMessage(socketChannel, testRequest, header, WARMUP_MESSAGES + i, histogram);
            }

            histogram.outputPercentileDistribution(System.out, 1000.0);
        }
    }

    private static void exchangeMessage(
        final SocketChannel socketChannel,
        final TestRequestEncoder testRequest,
        final HeaderEncoder header,
        final int i,
        final Histogram histogram)
        throws IOException
    {
        header.sendingTime(System.currentTimeMillis()).msgSeqNum(i + 2);

        int length = testRequest.encode(WRITE_FLYWEIGHT, 0);

        final long sendingTime = System.nanoTime();
        writeBuffer(socketChannel, length);

        length = read(socketChannel);
        final long returnTime = System.nanoTime();
        //System.out.println(READ_FLYWEIGHT.getAscii(0, length));
        histogram.recordValue(returnTime - sendingTime);
    }

    private static void logon(final SocketChannel socketChannel) throws IOException
    {
        final LogonEncoder logon = new LogonEncoder();
        logon.heartBtInt(10);
        logon
            .header()
            .sendingTime(System.currentTimeMillis())
            .senderCompID(INITIATOR_ID)
            .targetCompID(ACCEPTOR_ID)
            .msgSeqNum(1);

        writeBuffer(socketChannel, logon.encode(WRITE_FLYWEIGHT, 0));

        final int length = read(socketChannel);
        final LogonDecoder logonDecoder = new LogonDecoder();
        logonDecoder.decode(READ_FLYWEIGHT, 0, length);
        System.out.println("Authenticated: " + logonDecoder);
    }

    private static void writeBuffer(final SocketChannel socketChannel, final int amount) throws IOException
    {
        WRITE_BUFFER.position(0);
        WRITE_BUFFER.limit(amount);
        int remaining = amount;
        do
        {
            remaining -= socketChannel.write(WRITE_BUFFER);
        }
        while (remaining > 0);
    }

    private static int read(final SocketChannel socketChannel) throws IOException
    {
        READ_BUFFER.clear();
        int length;
        do
        {
            length = socketChannel.read(READ_BUFFER);
        }
        while (length == 0);
        //System.out.printf("Read Data: %d\n", length
        return length;
    }
}
