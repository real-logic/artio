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

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import uk.co.real_logic.agrona.generation.CompilerUtil;
import uk.co.real_logic.agrona.generation.StringWriterOutputManager;

import static uk.co.real_logic.fix_gateway.dictionary.ExampleDictionary.HEARTBEAT;
import static uk.co.real_logic.fix_gateway.dictionary.ExampleDictionary.SINGLE_MESSAGE_EXAMPLE;

public class EncoderGeneratorTest
{
    private StringWriterOutputManager outputManager = new StringWriterOutputManager();
    private EncoderGenerator encoderGenerator = new EncoderGenerator(SINGLE_MESSAGE_EXAMPLE, outputManager);

    @Before
    public void generate()
    {
        encoderGenerator.generate();
    }

    @Ignore
    @Test
    public void generatesEncoderClass()
    {

    }

    private Class<?> compileHeartbeat() throws Exception
    {
        //System.out.println(outputManager.getSources());
        return CompilerUtil.compileInMemory(HEARTBEAT, outputManager.getSources());
    }

}
