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
package uk.co.real_logic.artio;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import uk.co.real_logic.artio.builder.LogonEncoder;
import uk.co.real_logic.artio.fields.UtcTimestampEncoder;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
@Fork(1)
public class StubEncoderBenchmark
{
    private final UtcTimestampEncoder timestampEncoder = new UtcTimestampEncoder();
    private final LogonEncoder logonEncoder = new LogonEncoder();
    private final MutableAsciiBuffer buffer = new MutableAsciiBuffer(new byte[8 * 1024]);

    // deliberately not static/final
    private final int sequenceNumber = 10;
    private final char[] password = "password".toCharArray();
    private final char[] username = "username".toCharArray();

    @Setup
    public void setup()
    {
        logonEncoder
            .header()
            .senderCompID("ABC_DEFG01")
            .targetCompID("CCG");
    }

    @Benchmark
    public void encodeLogon(final Blackhole bh)
    {
        final UtcTimestampEncoder timestampEncoder = this.timestampEncoder;

        logonEncoder
            .password(password)
            .username(username)
            .maxMessageSize(512)
            .heartBtInt(10)
            .header()
            .msgSeqNum(sequenceNumber)
            .sendingTime(timestampEncoder.buffer(), timestampEncoder.encode(System.currentTimeMillis()));

        bh.consume(logonEncoder.encode(buffer, 0));
    }

}
