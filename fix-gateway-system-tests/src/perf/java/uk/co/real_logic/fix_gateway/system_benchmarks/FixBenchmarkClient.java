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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static java.net.StandardSocketOptions.TCP_NODELAY;
import static uk.co.real_logic.fix_gateway.system_benchmarks.Configuration.*;

public final class FixBenchmarkClient
{
    private static final byte START_BYTE = (byte) '8';
    private static final byte NEXT_BYTE = (byte) '=';

    private static final String HOST = System.getProperty("fix.benchmark.host", "localhost");
    private static final int BUFFER_SIZE = 16 * 1024;

    private static final ByteBuffer WRITE_BUFFER = ByteBuffer.allocateDirect(BUFFER_SIZE);
    private static final MutableAsciiFlyweight WRITE_FLYWEIGHT =
        new MutableAsciiFlyweight(new UnsafeBuffer(WRITE_BUFFER));

    private static final ByteBuffer READ_BUFFER = ByteBuffer.allocateDirect(BUFFER_SIZE);
    private static final MutableAsciiFlyweight READ_FLYWEIGHT =
        new MutableAsciiFlyweight(new UnsafeBuffer(READ_BUFFER));

    private static final long[] SENDING_TIMES = new long[MESSAGES_EXCHANGED];
    private static final long[] RETURN_TIMES = new long[MESSAGES_EXCHANGED];

    private static volatile boolean authenticated = false;

    public static void main(String[] args) throws IOException, InterruptedException
    {
        try (final SocketChannel socketChannel = SocketChannel.open(new InetSocketAddress(HOST, PORT)))
        {
            socketChannel.setOption(TCP_NODELAY, true);
            socketChannel.configureBlocking(false);

            final ReaderThread readerThread = new ReaderThread(socketChannel);
            readerThread.start();

            logon(socketChannel);

            while (!authenticated)
            {
                LockSupport.parkNanos(100);
            }

            sendMessages(socketChannel);

            readerThread.join();
        }

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
    }

    private static void sendMessages(final SocketChannel socketChannel) throws IOException
    {
        final TestRequestEncoder testRequest = new TestRequestEncoder();
        final HeaderEncoder header = testRequest
            .header()
            .senderCompID(INITIATOR_ID)
            .targetCompID(ACCEPTOR_ID);

        testRequest.testReqID("a");

        for (int i = 0; i < MESSAGES_EXCHANGED; i++)
        {
            header.sendingTime(System.currentTimeMillis()).msgSeqNum(i + 2);

            final int length = testRequest.encode(WRITE_FLYWEIGHT, 0);

            SENDING_TIMES[i] = System.nanoTime();
            writeBuffer(socketChannel, length);
        }
    }

    private static void writeBuffer(final SocketChannel socketChannel, final int amount) throws IOException
    {
        WRITE_BUFFER.position(0);
        WRITE_BUFFER.limit(amount);
        socketChannel.write(WRITE_BUFFER);
    }

    public static class ReaderThread extends Thread
    {
        private final Histogram histogram = new Histogram(TimeUnit.SECONDS.toNanos(10), 5);
        private final SocketChannel socketChannel;

        public ReaderThread(final SocketChannel socketChannel)
        {
            this.socketChannel = socketChannel;
        }

        public void run()
        {
            histogram.reset();
            int messageCount = 0;
            try
            {
                int length = read();

                final LogonDecoder logonDecoder = new LogonDecoder();
                logonDecoder.decode(READ_FLYWEIGHT, 0, length);

                System.out.println("Authenticated: " + logonDecoder);
                authenticated = true;

                while (messageCount < 100)
                {
                    length = read();
                    final long returnTime = System.nanoTime();

                    final int numberOfMessages = getNumberOfMessages(length);

                    for (int i = 0; i < numberOfMessages; i++)
                    {
                        RETURN_TIMES[messageCount + i] = returnTime;
                    }
                    messageCount += numberOfMessages;
                }
            }
            catch (Exception e)
            {
                e.printStackTrace(System.out);
            }
            finally
            {
                for (int i = WARMUP_MESSAGES; i < messageCount; i++)
                {
                    final long time = RETURN_TIMES[i] - SENDING_TIMES[i];
                    //System.out.println(time);
                    histogram.recordValue(time);
                }

                histogram.outputPercentileDistribution(System.out, 1000.0);
                System.out.println("Number of messages: " + messageCount);
            }
        }

        private int getNumberOfMessages(final int length)
        {
            int numberOfMessages = 0;
            final MutableAsciiFlyweight readFlyweight = READ_FLYWEIGHT;
            for (int i = 0; i < length; i++)
            {
                if (readFlyweight.getByte(i) == START_BYTE && readFlyweight.getByte(i) == NEXT_BYTE)
                {
                    numberOfMessages++;
                }
            }

            return numberOfMessages;
        }

        private int read() throws IOException
        {
            READ_BUFFER.clear();
            int length = 0;
            while (length == 0)
            {
                length = socketChannel.read(READ_BUFFER);
            }
            //System.out.printf("Read Data: %d\n", length
            return length;
        }
    }
}
