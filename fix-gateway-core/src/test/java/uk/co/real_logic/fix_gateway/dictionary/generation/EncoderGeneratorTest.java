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
import org.junit.Test;
import uk.co.real_logic.agrona.generation.StringWriterOutputManager;
import uk.co.real_logic.fix_gateway.builder.Encoder;

import java.lang.reflect.Field;

import static java.lang.reflect.Modifier.isAbstract;
import static java.lang.reflect.Modifier.isPublic;
import static org.junit.Assert.*;
import static uk.co.real_logic.agrona.generation.CompilerUtil.compileInMemory;
import static uk.co.real_logic.fix_gateway.dictionary.ExampleDictionary.HEARTBEAT;
import static uk.co.real_logic.fix_gateway.dictionary.ExampleDictionary.MESSAGE_EXAMPLE;

public class EncoderGeneratorTest
{
    private StringWriterOutputManager outputManager = new StringWriterOutputManager();
    private EncoderGenerator encoderGenerator = new EncoderGenerator(MESSAGE_EXAMPLE, outputManager);

    private Class<?> clazz;

    @Before
    public void generate() throws Exception
    {
        encoderGenerator.generate();
        //System.out.println(outputManager.getSources());
        clazz = compileInMemory(HEARTBEAT, outputManager.getSources());
    }

    @Test
    public void generatesEncoderClass() throws Exception
    {
        assertNotNull("Not generated anything", clazz);
        assertTrue(Encoder.class.isAssignableFrom(clazz));

        final int modifiers = clazz.getModifiers();
        assertFalse("Not instantiable", isAbstract(modifiers));
        assertTrue("Not public", isPublic(modifiers));
    }

    @Test
    public void generatesSetters() throws Exception
    {
        final Object encoder = clazz.newInstance();

        clazz.getMethod("onBehalfOfCompID", String.class);

        final String value = "abc";
        final String testReqID = "testReqID";

        clazz.getMethod(testReqID, String.class)
             .invoke(encoder, value);

        final Field field = clazz.getDeclaredField(testReqID);
        field.setAccessible(true);
        assertEquals(value, field.get(encoder));
    }

    // optional fields
    // clazz.getField("hasTestReqID");

}
