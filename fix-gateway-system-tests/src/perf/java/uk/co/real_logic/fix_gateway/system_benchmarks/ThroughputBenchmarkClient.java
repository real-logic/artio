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

import org.agrona.LangUtil;
import uk.co.real_logic.fix_gateway.builder.HeaderEncoder;
import uk.co.real_logic.fix_gateway.builder.TestRequestEncoder;
import uk.co.real_logic.fix_gateway.util.MutableAsciiBuffer;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

import static uk.co.real_logic.fix_gateway.system_benchmarks.BenchmarkConfiguration.MESSAGES_EXCHANGED;

public final class ThroughputBenchmarkClient extends AbstractBenchmarkClient
{
    public static void main(final String[] args) throws Exception
    {
        new ThroughputBenchmarkClient().runBenchmark();
    }

    private final CyclicBarrier barrier = new CyclicBarrier(2);

    private final class ReaderThread extends Thread
    {
        private final SocketChannel socketChannel;

        ReaderThread(final SocketChannel socketChannel)
        {
            this.socketChannel = socketChannel;
        }

        public void run()
        {
            final SocketChannel socketChannel = this.socketChannel;
            final MutableAsciiBuffer readFlyweight = ThroughputBenchmarkClient.this.readFlyweight;

            while (true)
            {
                final long startTime = System.currentTimeMillis();
                int messagesReceived = 0;
                do
                {
                    try
                    {
                        final int length = read(socketChannel);
                        messagesReceived += scanForReceivesMessages(readFlyweight, length);
                    }
                    catch (final IOException ex)
                    {
                        ex.printStackTrace();
                        System.exit(-1);
                    }
                }
                while (messagesReceived < MESSAGES_EXCHANGED);

                printTimes(startTime);

                await();
            }
        }
    }

    public void runBenchmark() throws Exception
    {
        try (SocketChannel socketChannel = open())
        {
            logon(socketChannel);

            final TestRequestEncoder testRequest = setupTestRequest();
            final HeaderEncoder header = testRequest.header();

            final ReaderThread readerThread = new ReaderThread(socketChannel);
            readerThread.start();

            int seqNo = 2;
            int max = 2;

            while (true)
            {
                max += MESSAGES_EXCHANGED;

                for (; seqNo < max; seqNo++)
                {
                    final int length = encode(testRequest, header, seqNo);
                    write(socketChannel, length);
                }

                await();
            }
        }
    }

    private void await()
    {
        try
        {
            barrier.await();
        }
        catch (InterruptedException | BrokenBarrierException e)
        {
            LangUtil.rethrowUnchecked(e);
        }
    }
}
