/*
 * Copyright 2013 Real Logic Ltd.
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
import uk.co.real_logic.agrona.generation.CompilerUtil;
import uk.co.real_logic.agrona.generation.StringWriterOutputManager;

import java.lang.reflect.Method;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static uk.co.real_logic.fix_gateway.dictionary.ExampleDictionary.EG_ENUM;
import static uk.co.real_logic.fix_gateway.dictionary.ExampleDictionary.FIELD_EXAMPLE;

// TODO: support enums whose values has multiple characters
public class EnumGeneratorTest
{
    private StringWriterOutputManager outputManager = new StringWriterOutputManager();
    private EnumGenerator enumGenerator = new EnumGenerator(FIELD_EXAMPLE, outputManager);

    @Before
    public void generate()
    {
        enumGenerator.generate();
    }

    @Test
    public void generatesEnumClass() throws Exception
    {
        final Class<?> clazz = compileEgEnum();

        assertNotNull("Failed to generate a class", clazz);
        assertTrue("Generated class isn't an enum", clazz.isEnum());
    }

    @Test
    public void generatesEnumConstants() throws Exception
    {
        final Class<?> clazz = compileEgEnum();
        final Enum[] values = (Enum[])clazz.getEnumConstants();

        assertThat(values, arrayWithSize(2));

        assertEquals("AnEntry", values[0].name());
        assertRepresentation('a', values[0]);
        assertEquals("AnotherEntry", values[1].name());
        assertRepresentation('b', values[1]);
    }

    @Test
    public void generatesLookupTable() throws Exception
    {
        final Class<?> clazz = compileEgEnum();
        final Enum[] values = (Enum[])clazz.getEnumConstants();

        final Method valueOf = clazz.getMethod("valueOf", int.class);

        assertEquals(values[0], valueOf.invoke(null, 'a'));
        assertEquals(values[1], valueOf.invoke(null, 'b'));
    }

    @Test
    public void doesNotGenerateClassForNonEnumFields()
    {
        assertThat(outputManager.getSources(), not(hasKey("EgNotEnum")));
    }

    private Class<?> compileEgEnum() throws Exception
    {
        return CompilerUtil.compileInMemory(EG_ENUM, outputManager.getSources());
    }

    private void assertRepresentation(final int expected, final Enum<?> enumElement) throws Exception
    {
        final int representation =
            (int)enumElement
                .getDeclaringClass()
                .getMethod("representation")
                .invoke(enumElement);

        assertEquals(expected, representation);
    }
}
