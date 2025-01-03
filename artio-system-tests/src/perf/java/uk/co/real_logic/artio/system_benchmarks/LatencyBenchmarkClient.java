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
import uk.co.real_logic.artio.builder.HeaderEncoder;
import uk.co.real_logic.artio.builder.TestRequestEncoder;
import uk.co.real_logic.artio.timing.HistogramLogReader;

import java.io.IOException;
import java.nio.channels.SocketChannel;

import static uk.co.real_logic.artio.system_benchmarks.BenchmarkConfiguration.MESSAGES_EXCHANGED;
import static uk.co.real_logic.artio.system_benchmarks.BenchmarkConfiguration.WARMUP_MESSAGES;

public final class LatencyBenchmarkClient extends AbstractBenchmarkClient
{
    public static void main(final String[] args) throws IOException
    {
        new LatencyBenchmarkClient().runBenchmark();
    }

    public void runBenchmark() throws IOException
    {
        while (true)
        {
            try (SocketChannel socketChannel = open())
            {
                logon(socketChannel);

                final TestRequestEncoder testRequest = setupTestRequest();
                final HeaderEncoder header = testRequest.header();
                final Histogram histogram = new Histogram(3);

                runWarmup(socketChannel, testRequest, header, histogram);

                parkAfterWarmup();

                runTimedRuns(socketChannel, testRequest, header, histogram);
            }
        }
    }

    private void runWarmup(
        final SocketChannel socketChannel,
        final TestRequestEncoder testRequest,
        final HeaderEncoder header,
        final Histogram histogram) throws IOException
    {
        for (int i = 0; i < WARMUP_MESSAGES; i++)
        {
            exchangeMessage(socketChannel, testRequest, header, i, histogram);
        }
        System.out.println("Warmup Complete");
    }

    private void runTimedRuns(
        final SocketChannel socketChannel,
        final TestRequestEncoder testRequest,
        final HeaderEncoder header,
        final Histogram histogram)
        throws IOException
    {
        histogram.reset();

        for (int i = 0; i < MESSAGES_EXCHANGED; i++)
        {
            exchangeMessage(socketChannel, testRequest, header, WARMUP_MESSAGES + i, histogram);
        }

        HistogramLogReader.prettyPrint(
            System.currentTimeMillis(), histogram, "Client in Micros", 1000);
    }

    private void exchangeMessage(
        final SocketChannel socketChannel,
        final TestRequestEncoder testRequest,
        final HeaderEncoder header,
        final int index,
        final Histogram histogram)
        throws IOException
    {
        header.msgSeqNum(index + 2);
        timestampEncoder.encode(System.currentTimeMillis());

        final long result = testRequest.encode(writeFlyweight, 0);

        final long sendingTime = System.nanoTime();
        write(socketChannel, result);

        read(socketChannel);
        final long returnTime = System.nanoTime();
        histogram.recordValue(returnTime - sendingTime);
    }
}
