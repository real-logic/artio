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
import uk.co.real_logic.fix_gateway.dictionary.IntDictionary;
import uk.co.real_logic.fix_gateway.otf.OtfParser;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@Fork(1)
public class OtfParserBenchmark
{
    // Deliberately not static/final
    private UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.wrap(
        ("8=FIX.4.2\0019=145\00135=D\00134=4\00149=ABC_DEFG01\001" +
        "52=20090323-15:40:29\00156=CCG\001115=XYZ\00111=NF 0542/03232009\00154=1\00138=100\00155=CVS\00140=1" +
        "\00159=0\00147=A\00160=20090323-15:40:29\00121=1\001207=N\00110=194\001").getBytes(StandardCharsets.US_ASCII)));


    private OtfParser blackholeParser;
    private OtfParser noBlackholeParser;
    private OtfParser inlinableParser;

    @Setup
    public void setup(final Blackhole bh)
    {
        blackholeParser = new OtfParser(new OtfBlackHoleAcceptor(bh), new IntDictionary());
        noBlackholeParser = new OtfParser(new OtfNoBlackHoleAcceptor(), new IntDictionary());
        inlinableParser = new OtfParser(new OtfInlineableAcceptor(), new IntDictionary());
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
