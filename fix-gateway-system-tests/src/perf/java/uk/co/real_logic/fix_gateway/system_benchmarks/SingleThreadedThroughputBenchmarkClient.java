/*
 * Copyright 2015-2016 Real Logic Ltd.
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
import uk.co.real_logic.fix_gateway.util.MutableAsciiBuffer;

import java.io.IOException;
import java.nio.channels.SocketChannel;

import static uk.co.real_logic.fix_gateway.system_benchmarks.BenchmarkConfiguration.MESSAGES_EXCHANGED;

public final class SingleThreadedThroughputBenchmarkClient extends AbstractBenchmarkClient
{
    public static void main(final String[] args) throws Exception
    {
        new SingleThreadedThroughputBenchmarkClient().runBenchmark();
    }

    public void runBenchmark() throws Exception
    {
        try (SocketChannel socketChannel = open())
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

    private int greedyRead(final SocketChannel socketChannel, final MutableAsciiBuffer readFlyweight)
    {
        int messagesReceived = 0;

        try
        {
            readBuffer.clear();
            final int length = socketChannel.read(readBuffer);
            messagesReceived += scanForReceivesMessages(readFlyweight, length);
        }
        catch (final IOException ex)
        {
            ex.printStackTrace();
            System.exit(-1);
        }

        return messagesReceived;
    }
}
