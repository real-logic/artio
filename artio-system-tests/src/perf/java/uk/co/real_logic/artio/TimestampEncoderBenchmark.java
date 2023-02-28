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
import uk.co.real_logic.artio.fields.UtcTimestampEncoder;

import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
@Fork(1)
public class TimestampEncoderBenchmark
{
    private final UtcTimestampEncoder timestampEncoder = new UtcTimestampEncoder();

    @Setup
    public void initialiseTimestamp()
    {
        timestampEncoder.initialise(System.currentTimeMillis());
    }

    @Benchmark
    public void encodeTimestamp(final Blackhole bh)
    {
        final UtcTimestampEncoder timestampEncoder = this.timestampEncoder;

        bh.consume(timestampEncoder.encode(System.currentTimeMillis()));
        bh.consume(timestampEncoder.buffer());
    }

    @Benchmark
    public void updateTimestamp(final Blackhole bh)
    {
        final UtcTimestampEncoder timestampEncoder = this.timestampEncoder;

        bh.consume(timestampEncoder.update(System.currentTimeMillis()));
        bh.consume(timestampEncoder.buffer());
    }

    @Benchmark
    public void noise(final Blackhole bh)
    {
        bh.consume(this.timestampEncoder);

        bh.consume(System.currentTimeMillis());
    }

}
