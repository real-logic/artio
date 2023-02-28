/*
 * Copyright 2015-2023 Real Logic Limited.
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

import uk.co.real_logic.artio.builder.HeaderEncoder;
import uk.co.real_logic.artio.builder.TestRequestEncoder;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import static uk.co.real_logic.artio.system_benchmarks.BenchmarkConfiguration.*;

public final class ThroughputBenchmarkClient extends AbstractBenchmarkClient
{

    public static final int INITIAL_SEQ_NO = 2;
    public static final int TOTAL_MESSAGES = NUMBER_OF_SESSIONS * MESSAGES_EXCHANGED;

    public static void main(final String[] args) throws Exception
    {
        new ThroughputBenchmarkClient().runBenchmark();
    }

    private final BenchmarkSession[] sessions = new BenchmarkSession[NUMBER_OF_SESSIONS];
    private final CyclicBarrier barrier = new CyclicBarrier(2);

    private final class ReaderThread extends Thread
    {
        ReaderThread()
        {
            super("ReaderThread");
        }

        public void run()
        {
            final BenchmarkSession[] sessions = ThroughputBenchmarkClient.this.sessions;

            while (true)
            {
                final long startTime = System.currentTimeMillis();
                int remainingMessages = TOTAL_MESSAGES;
                while (remainingMessages > 0)
                {
                    for (int i = 0; i < NUMBER_OF_SESSIONS; i++)
                    {
                        try
                        {
                            remainingMessages -= sessions[i].attemptRead();
                        }
                        catch (final IOException ex)
                        {
                            ex.printStackTrace();
                            System.exit(-1);
                        }
                    }
                }

                printThroughput(startTime, TOTAL_MESSAGES);
            }
        }
    }

    public void runBenchmark() throws Exception
    {
        final BenchmarkSession[] sessions = this.sessions;
        for (int i = 0; i < NUMBER_OF_SESSIONS; i++)
        {
            sessions[i] = new BenchmarkSession(i);
        }

        final ReaderThread readerThread = new ReaderThread();
        readerThread.start();

        while (true)
        {
            int remainingMessages = TOTAL_MESSAGES;
            while (remainingMessages > 0)
            {
                for (int i = 0; i < NUMBER_OF_SESSIONS; i++)
                {
                    remainingMessages -= sessions[i].attemptWrite();
                }
            }
        }
    }

    private final class BenchmarkSession implements AutoCloseable
    {
        private final AtomicInteger totalMessagesReceived = new AtomicInteger(INITIAL_SEQ_NO);
        private final SocketChannel socketChannel;
        private final TestRequestEncoder testRequest;
        private final HeaderEncoder header;

        private final int seqNo = INITIAL_SEQ_NO;
        private int senderLimit = senderLimit();

        private BenchmarkSession(final int i) throws IOException
        {
            socketChannel = open();
            final String initiatorId = INITIATOR_ID + i;
            testRequest = setupTestRequest(initiatorId);
            header = testRequest.header();
            logon(socketChannel, initiatorId, 10);
        }

        private int attemptWrite() throws IOException
        {
            if (seqNo > senderLimit)
            {
                if (seqNo > (senderLimit = senderLimit()))
                {
                    return 0;
                }
            }

            write(socketChannel, encode(testRequest, header, seqNo, 0));
            return 1;
        }

        private int attemptRead() throws IOException
        {
            readBuffer.clear();
            final int length = socketChannel.read(readBuffer);
            if (length == 0)
            {
                return 0;
            }

            if (length < 0)
            {
                System.err.println("Disconnected by server");
                System.exit(-1);
            }

            final int receivedMessages = scanForReceivesMessages(readFlyweight, length);
            // System.out.println("Read: " + readFlyweight.getAscii(0, length));

            if (receivedMessages > 0)
            {
                final int i = totalMessagesReceived.addAndGet(receivedMessages);
                // System.out.println("receivedMessages = " + receivedMessages);
                // System.out.println("i = " + i);
            }
            return receivedMessages;
        }

        private int senderLimit()
        {
            return totalMessagesReceived.get() + MAX_MESSAGES_IN_FLIGHT;
        }

        public void close() throws Exception
        {
            socketChannel.close();
        }
    }
}
