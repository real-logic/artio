/*
 * Copyright 2015-2016 Real Logic Ltd.
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

import org.agrona.BufferUtil;
import org.openjdk.jmh.annotations.*;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.agrona.UnsafeAccess.UNSAFE;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 5)
@Measurement(iterations = 10)
@Fork(1)
public class ArraysFillBenchmark
{
    private static final int MISSING_VALUE = 0;
    private static final byte MISSING_BYTE = 0;

    @Param({"2", "8", "32", "128", "256", "512", "1024"})
    int size;
    int[] values;

    @Setup
    public void setup()
    {
        values = new int[size];
    }

    @Benchmark
    public int[] arraysFill()
    {
        Arrays.fill(values, MISSING_VALUE);
        return values;
    }

    @Benchmark
    public int[] memset()
    {
        UNSAFE.setMemory(values, BufferUtil.ARRAY_BASE_OFFSET, values.length, MISSING_BYTE);
        return values;
    }

    @Benchmark
    public int[] memsetLocal()
    {
        final int[] values = this.values;
        UNSAFE.setMemory(values, BufferUtil.ARRAY_BASE_OFFSET, values.length, MISSING_BYTE);
        return values;
    }

    @Benchmark
    public int[] offsetMemset()
    {
        final int[] values = this.values;
        UNSAFE.putByte(values, BufferUtil.ARRAY_BASE_OFFSET, MISSING_BYTE);
        UNSAFE.setMemory(values, BufferUtil.ARRAY_BASE_OFFSET + 1, values.length - 1, MISSING_BYTE);
        return values;
    }

}
