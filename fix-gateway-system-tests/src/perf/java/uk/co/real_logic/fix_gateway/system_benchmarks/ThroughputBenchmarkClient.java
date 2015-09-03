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

import uk.co.real_logic.fix_gateway.CommonConfiguration;
import uk.co.real_logic.fix_gateway.builder.HeaderEncoder;
import uk.co.real_logic.fix_gateway.builder.TestRequestEncoder;
import uk.co.real_logic.fix_gateway.util.MutableAsciiFlyweight;

import java.io.IOException;
import java.nio.channels.SocketChannel;

import static uk.co.real_logic.fix_gateway.util.AsciiFlyweight.UNKNOWN_INDEX;

public final class ThroughputBenchmarkClient extends AbstractBenchmarkClient
{
    private static final int MESSAGES_EXCHANGED = CommonConfiguration.MESSAGES_EXCHANGED;
    private static final byte EIGHT = (byte) '8';

    public static void main(String[] args) throws Exception
    {
        new ThroughputBenchmarkClient().runBenchmark();
    }

    private long endTime;

    private final class ReaderThread extends Thread
    {
        private final SocketChannel socketChannel;

        public ReaderThread(final SocketChannel socketChannel)
        {
            this.socketChannel = socketChannel;
        }

        public void run()
        {
            final SocketChannel socketChannel = this.socketChannel;
            final MutableAsciiFlyweight readFlyweight = ThroughputBenchmarkClient.this.readFlyweight;

            int messagesReceived = 0;
            do
            {
                try
                {
                    final int bytesReceived = read(socketChannel);
                    if (bytesReceived > 0)
                    {
                        int index = 0;
                        while (true)
                        {
                            index = readFlyweight.scan(index, bytesReceived - 1, EIGHT);

                            if (index == UNKNOWN_INDEX || (index + 1) >= bytesReceived)
                            {
                                break;
                            }

                            if (readFlyweight.getChar(index + 1) == '=')
                            {
                                messagesReceived++;
                            }

                            index += 2;
                        }

                        // System.out.println(readFlyweight.getAscii(0, bytesReceived));
                        // System.out.println(messagesReceived);
                    }
                }
                catch (IOException e)
                {
                    e.printStackTrace();
                    System.exit(-1);
                }
            }
            while (messagesReceived < MESSAGES_EXCHANGED);

            endTime = System.currentTimeMillis();
        }
    }

    public void runBenchmark() throws Exception
    {
        try (final SocketChannel socketChannel = open())
        {
            logon(socketChannel);

            final TestRequestEncoder testRequest = setupTestRequest();
            final HeaderEncoder header = testRequest.header();

            final ReaderThread readerThread = new ReaderThread(socketChannel);
            readerThread.start();

            final long startTime = System.currentTimeMillis();
            for (int seqNo = 2; seqNo < MESSAGES_EXCHANGED + 2; seqNo++)
            {
                final int length = encode(testRequest, header, seqNo);
                writeBuffer(socketChannel, length);
                // System.out.printf("Written: %d\n", length);
                // System.out.println(writeFlyweight.getAscii(0, length));
            }

            readerThread.join();

            final long duration = endTime - startTime;
            final double rate = (double) MESSAGES_EXCHANGED / duration;
            System.out.printf("%d messages in %d ms\n", MESSAGES_EXCHANGED, duration);
            System.out.printf("%G messages / ms\n", rate);
            System.out.printf("%G messages / s\n", rate * 1000.0);
        }
    }

    private int encode(final TestRequestEncoder testRequest, final HeaderEncoder header, final int seqNum)
    {
        header
            .sendingTime(System.currentTimeMillis())
            .msgSeqNum(seqNum);

        return testRequest.encode(writeFlyweight, 0);
    }

}
