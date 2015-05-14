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
package uk.co.real_logic.fix_gateway;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.builder.LogonEncoder;
import uk.co.real_logic.fix_gateway.util.MutableAsciiFlyweight;

import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@Fork(1)
public class StubEncoderBenchmark
{
    private LogonEncoder logonEncoder = new LogonEncoder();
    private UnsafeBuffer buffer = new UnsafeBuffer(new byte[8 * 1024]);
    private MutableAsciiFlyweight asciiFlyweight = new MutableAsciiFlyweight(buffer);

    // deliberately not static/final
    private int sequenceNumber = 10;
    private char[] password = "password".toCharArray();
    private char[] username = "username".toCharArray();

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
        logonEncoder
            .header()
            .msgSeqNum(sequenceNumber)
            .sendingTime(System.currentTimeMillis());

        logonEncoder
            .password(password)
            .username(username)
            .maxMessageSize(512)
            .heartBtInt(10);

        bh.consume(logonEncoder.encode(asciiFlyweight, 0));
    }

}
