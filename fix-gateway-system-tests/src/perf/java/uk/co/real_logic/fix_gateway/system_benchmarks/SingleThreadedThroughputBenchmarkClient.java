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

import uk.co.real_logic.fix_gateway.builder.HeaderEncoder;
import uk.co.real_logic.fix_gateway.builder.TestRequestEncoder;
import uk.co.real_logic.fix_gateway.util.MutableAsciiFlyweight;

import java.io.IOException;
import java.nio.channels.SocketChannel;

import static uk.co.real_logic.fix_gateway.util.AsciiFlyweight.UNKNOWN_INDEX;

public final class SingleThreadedThroughputBenchmarkClient extends AbstractBenchmarkClient
{
    private static final int MESSAGES_EXCHANGED = 20_000;
    private static final byte NINE = (byte) '9';

    public static void main(String[] args) throws Exception
    {
        new SingleThreadedThroughputBenchmarkClient().runBenchmark();
    }

    boolean lastWasSep;

    public void runBenchmark() throws Exception
    {
        try (final SocketChannel socketChannel = open())
        {
            logon(socketChannel);

            final TestRequestEncoder testRequest = setupTestRequest();
            final HeaderEncoder header = testRequest.header();

            int sequenceNumber = 2;
            while (true)
            {
                final long startTime = System.currentTimeMillis();

                lastWasSep = false;
                int messagesReceived = 0;

                do
                {
                    final int length = encode(testRequest, header, sequenceNumber);
                    write(socketChannel, length);
                    sequenceNumber++;

                    messagesReceived += greedyRead(socketChannel, readFlyweight);
                }
                while (messagesReceived < MESSAGES_EXCHANGED);

                printTimes(startTime);
            }
        }
    }

    private int greedyRead(
        final SocketChannel socketChannel,
        final MutableAsciiFlyweight readFlyweight)
    {
        int messagesReceived = 0;

        try
        {
            readBuffer.clear();
            final int length = socketChannel.read(readBuffer);
            if (length > 0)
            {
                int index = 0;

                while (index < length)
                {
                    index = readFlyweight.scan(index, length - 1, NINE);

                    if (index == UNKNOWN_INDEX)
                    {
                        break;
                    }

                    if (index == 0)
                    {
                        if (lastWasSep)
                        {
                            messagesReceived++;
                        }
                    }
                    else if (readFlyweight.getChar(index - 1) == '\001')
                    {
                        messagesReceived++;
                    }

                    index += 1;
                }

                lastWasSep = readFlyweight.getChar(length - 1) == '\001';
            }
        }
        catch (IOException e)
        {
            e.printStackTrace();
            System.exit(-1);
        }

        return messagesReceived;
    }

    private int encode(final TestRequestEncoder testRequest, final HeaderEncoder header, final int seqNum)
    {
        header
            .sendingTime(System.currentTimeMillis())
            .msgSeqNum(seqNum);

        return testRequest.encode(writeFlyweight, 0);
    }

    private void printTimes(final long startTime)
    {
        final long duration = System.currentTimeMillis() - startTime;
        final double rate = (double) MESSAGES_EXCHANGED / duration;
        System.out.printf("%d messages in %d ms\n", MESSAGES_EXCHANGED, duration);
        System.out.printf("%G messages / ms\n", rate);
        System.out.printf("%G messages / s\n", rate * 1000.0);
    }

}
