/*
 * Copyright 2015-2024 Real Logic Limited.
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
import uk.co.real_logic.artio.builder.Validation;
import uk.co.real_logic.artio.decoder.HeaderDecoder;
import uk.co.real_logic.artio.decoder.LogonDecoder;
import uk.co.real_logic.artio.util.AsciiBuffer;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
@Fork(1)
public class StubValidatingDecoderBenchmark
{
    private final LogonDecoder logonDecoder = new LogonDecoder();
    private final AsciiBuffer asciiBuffer = new MutableAsciiBuffer(TestData.LOGON);

    @Setup
    public void setup()
    {
        if (!Validation.CODEC_VALIDATION_ENABLED)
        {
            throw new IllegalStateException("Benchmark cannot run without validation enabled");
        }
    }

    @Benchmark
    public void decodeLogon(final Blackhole bh)
    {
        bh.consume(logonDecoder.decode(asciiBuffer, 0, asciiBuffer.capacity()));

        final HeaderDecoder header = logonDecoder.header();
        bh.consume(header.msgSeqNum());

        bh.consume(logonDecoder.hasPassword());
        bh.consume(logonDecoder.password());

        bh.consume(logonDecoder.hasUsername());
        bh.consume(logonDecoder.username());

        bh.consume(logonDecoder.validate());

        bh.consume(logonDecoder.validate());
    }

    @Benchmark
    public void resetAndDecodeLogon(final Blackhole bh)
    {
        logonDecoder.reset();

        bh.consume(logonDecoder.decode(asciiBuffer, 0, asciiBuffer.capacity()));

        final HeaderDecoder header = logonDecoder.header();
        bh.consume(header.msgSeqNum());

        bh.consume(logonDecoder.hasPassword());
        bh.consume(logonDecoder.password());

        bh.consume(logonDecoder.hasUsername());
        bh.consume(logonDecoder.username());

        bh.consume(logonDecoder.validate());

        bh.consume(logonDecoder.validate());
    }

}
