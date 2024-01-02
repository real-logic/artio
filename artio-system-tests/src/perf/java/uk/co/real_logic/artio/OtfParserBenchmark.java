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

import org.agrona.concurrent.UnsafeBuffer;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;
import uk.co.real_logic.artio.dictionary.LongDictionary;
import uk.co.real_logic.artio.otf.OtfParser;

import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@Fork(1)
public class OtfParserBenchmark
{
    // Deliberately not static/final
    private final UnsafeBuffer buffer = TestData.NEW_ORDER_SINGLE;

    private OtfParser blackholeParser;
    private OtfParser noBlackholeParser;
    private OtfParser inlinableParser;

    @Setup
    public void setup(final Blackhole bh)
    {
        blackholeParser = new OtfParser(new OtfBlackHoleAcceptor(bh), new LongDictionary());
        noBlackholeParser = new OtfParser(new OtfNoBlackHoleAcceptor(), new LongDictionary());
        inlinableParser = new OtfParser(new OtfInlineableAcceptor(), new LongDictionary());
    }

    @Benchmark
    public void newOrderSingleBlackhole()
    {
        blackholeParser.onMessage(buffer, 0, buffer.capacity());
    }

    @Benchmark
    public void newOrderSingleNoBlackHole()
    {
        noBlackholeParser.onMessage(buffer, 0, buffer.capacity());
    }

    @Benchmark
    public void newOrderSingleInlinable()
    {
        inlinableParser.onMessage(buffer, 0, buffer.capacity());
    }
}
