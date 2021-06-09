/*
 * Copyright 2014-2021 Real Logic Limited.
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
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

import java.util.concurrent.TimeUnit;

/**
 * Benchmark for the {@link uk.co.real_logic.artio.util.MutableAsciiBuffer#putFloatAscii(int, long, int)} method.
 */
@Fork(1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
@State(Scope.Benchmark)
public class DecimalFloatEncoderBenchmark
{
    @Param({ "0", "-9182", "27085146", "-123456789", "2147483647" })
    private int value;
    @Param({ "0", "2", "5", "10" })
    private int scale;

    private final MutableAsciiBuffer buffer = new MutableAsciiBuffer(new byte[64]);

    /**
     * Benchmark  {@link uk.co.real_logic.artio.util.MutableAsciiBuffer#putFloatAscii(int, long, int)}  method
     *
     * @return length in bytes of the written value.
     */
    @Benchmark
    public int benchmark()
    {
        return buffer.putFloatAscii(0, value, scale);
    }
}
