/*
 * Copyright 2015-2025 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.co.real_logic.artio.system_benchmarks;

import org.HdrHistogram.Histogram;
import org.agrona.LangUtil;
import uk.co.real_logic.artio.builder.HeaderEncoder;
import uk.co.real_logic.artio.builder.TestRequestEncoder;
import uk.co.real_logic.artio.timing.HistogramLogReader;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import static uk.co.real_logic.artio.system_benchmarks.BenchmarkConfiguration.MESSAGES_EXCHANGED;
import static uk.co.real_logic.artio.system_benchmarks.BenchmarkConfiguration.SEND_RATE_PER_SECOND;

public final class LatencyUnderLoadBenchmarkClient extends AbstractBenchmarkClient
{
    public static void main(final String[] args) throws Exception
    {
        new LatencyUnderLoadBenchmarkClient().runBenchmark();
    }

    private final CyclicBarrier barrier = new CyclicBarrier(2);
    private final long[] sendTimes = new long[MESSAGES_EXCHANGED];

    private final class ReaderThread extends Thread
    {
        private final SocketChannel socketChannel;

        ReaderThread(final SocketChannel socketChannel)
        {
            this.socketChannel = socketChannel;
        }

        public void run()
        {
            final Histogram histogram = new Histogram(3);
            final long scaleToMicros = TimeUnit.MICROSECONDS.toNanos(1);
            final SocketChannel socketChannel = this.socketChannel;
            final MutableAsciiBuffer readFlyweight = LatencyUnderLoadBenchmarkClient.this.readFlyweight;
            final long[] sendTimes = LatencyUnderLoadBenchmarkClient.this.sendTimes;

            while (true)
            {
                final long startTime = System.currentTimeMillis();
                int lastMessagesReceived = 0;
                while (lastMessagesReceived < MESSAGES_EXCHANGED)
                {
                    try
                    {
                        final int length = read(socketChannel);
                        final long time = System.nanoTime();
                        final int received = scanForReceivesMessages(readFlyweight, length);
                        for (int j = 0; j < received; j++)
                        {
                            final long duration = time - sendTimes[lastMessagesReceived + j];
                            histogram.recordValue(duration);
                        }
                        lastMessagesReceived += received;
                    }
                    catch (final IOException ex)
                    {
                        ex.printStackTrace();
                        System.exit(-1);
                    }
                }

                printThroughput(startTime, MESSAGES_EXCHANGED);
                HistogramLogReader.prettyPrint(
                    System.currentTimeMillis(), histogram, "Benchmark", scaleToMicros);

                histogram.reset();
                await();
            }
        }
    }

    public void runBenchmark() throws Exception
    {
        final long pauseInNs = getPauseInNs();
        System.out.println(pauseInNs);

        try (SocketChannel socketChannel = open())
        {
            final ReaderThread readerThread = new ReaderThread(socketChannel);
            readerThread.start();

            logon(socketChannel);

            final TestRequestEncoder testRequest = setupTestRequest();
            final HeaderEncoder header = testRequest.header();
            final long[] sendTimes = this.sendTimes;

            int seqNo = 2;

            while (true)
            {
                for (int i = 0; i < MESSAGES_EXCHANGED; i++)
                {
                    final long result = encode(testRequest, header, seqNo);
                    sendTimes[i] = System.nanoTime();
                    write(socketChannel, result);
                    seqNo++;

                    LockSupport.parkNanos(pauseInNs);
                }

                await();
            }
        }
    }

    private long getPauseInNs()
    {
        final double nsPerSecond = TimeUnit.SECONDS.toNanos(1);
        return (long)(nsPerSecond / (double)SEND_RATE_PER_SECOND);
    }

    private void await()
    {
        try
        {
            barrier.await();
        }
        catch (final InterruptedException | BrokenBarrierException ex)
        {
            LangUtil.rethrowUnchecked(ex);
        }
    }
}
