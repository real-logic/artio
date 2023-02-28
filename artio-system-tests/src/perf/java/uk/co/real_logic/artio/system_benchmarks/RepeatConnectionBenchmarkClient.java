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

import uk.co.real_logic.artio.builder.TestRequestEncoder;

import java.io.IOException;
import java.nio.channels.SocketChannel;

public final class RepeatConnectionBenchmarkClient extends AbstractBenchmarkClient
{
    public static void main(final String[] args) throws IOException
    {
        new RepeatConnectionBenchmarkClient().runBenchmark();
    }

    public static final int NUMBER_OF_CONNECTIONS = 50_000;

    public void runBenchmark() throws IOException
    {
        for (int i = 0; i < NUMBER_OF_CONNECTIONS; i++)
        {
            try (SocketChannel socketChannel = open())
            {
                logon(socketChannel);

                final TestRequestEncoder testRequest = setupTestRequest();
                testRequest.header().msgSeqNum(3);

                timestampEncoder.encode(System.currentTimeMillis());

                write(socketChannel, testRequest.encode(writeFlyweight, 0));

                read(socketChannel);
            }

            System.out.printf("Finished Connection: %d%n", i + 1);
        }
    }
}
