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
import uk.co.real_logic.fix_gateway.decoder.HeaderDecoder;
import uk.co.real_logic.fix_gateway.decoder.LogonDecoder;
import uk.co.real_logic.fix_gateway.util.AsciiFlyweight;

import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@Fork(1)
public class StubDecoderBenchmark
{
    private LogonDecoder logonDecoder = new LogonDecoder();
    private UnsafeBuffer buffer = TestData.LOGON;
    private AsciiFlyweight asciiFlyweight = new AsciiFlyweight(buffer);

    @Benchmark
    public void decodeLogon(final Blackhole bh)
    {
        bh.consume(logonDecoder.decode(asciiFlyweight, 0, buffer.capacity()));

        final HeaderDecoder header = logonDecoder.header();
        bh.consume(header.msgSeqNum());

        bh.consume(logonDecoder.hasPassword());
        bh.consume(logonDecoder.password());

        bh.consume(logonDecoder.hasUsername());
        bh.consume(logonDecoder.username());
    }

}
