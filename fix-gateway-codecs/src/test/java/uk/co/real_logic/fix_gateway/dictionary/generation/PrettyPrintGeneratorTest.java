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
package uk.co.real_logic.fix_gateway.dictionary.generation;

import org.junit.BeforeClass;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.agrona.generation.StringWriterOutputManager;
import uk.co.real_logic.fix_gateway.util.MutableAsciiFlyweight;

import java.util.Map;

import static uk.co.real_logic.agrona.generation.CompilerUtil.compileInMemory;
import static uk.co.real_logic.fix_gateway.dictionary.ExampleDictionary.*;

public class PrettyPrintGeneratorTest
{
    private static StringWriterOutputManager outputManager = new StringWriterOutputManager();
    private static PrettyPrintGenerator prettyPrintGenerator =
        new PrettyPrintGenerator(MESSAGE_EXAMPLE, TEST_PACKAGE, outputManager);
    private static Class<?> heartbeat;
    private static Class<?> headerClass;

    private MutableAsciiFlyweight buffer = new MutableAsciiFlyweight(new UnsafeBuffer(new byte[8 * 1024]));

    @BeforeClass
    public static void generate() throws Exception
    {
        prettyPrintGenerator.generate();
        final Map<String, CharSequence> sources = outputManager.getSources();
        System.out.println(sources);
        heartbeat = compileInMemory(HEARTBEAT_DECODER, sources);
        headerClass = compileInMemory(HEADER_DECODER, sources);
    }


}
