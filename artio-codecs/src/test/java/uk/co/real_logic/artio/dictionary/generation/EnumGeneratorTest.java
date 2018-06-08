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
package uk.co.real_logic.artio.dictionary.generation;

import java.lang.reflect.Method;

import org.agrona.generation.CompilerUtil;
import org.agrona.generation.StringWriterOutputManager;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;


import static uk.co.real_logic.artio.dictionary.ExampleDictionary.EG_ENUM;
import static uk.co.real_logic.artio.dictionary.ExampleDictionary.FIELD_EXAMPLE;
import static uk.co.real_logic.artio.dictionary.ExampleDictionary.MULTI_STRING_VALUE_ENUM;
import static uk.co.real_logic.artio.dictionary.ExampleDictionary.OTHER_ENUM;
import static uk.co.real_logic.artio.dictionary.ExampleDictionary.STRING_ENUM;
import static uk.co.real_logic.artio.dictionary.generation.GenerationUtil.PARENT_PACKAGE;

public class EnumGeneratorTest
{
    private StringWriterOutputManager outputManager = new StringWriterOutputManager();
    private EnumGenerator enumGenerator = new EnumGenerator(FIELD_EXAMPLE, PARENT_PACKAGE, outputManager);

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

        final Method decode = decode(clazz);

        assertEquals(values[0], decode.invoke(null, 'a'));
        assertEquals(values[1], decode.invoke(null, 'b'));
    }

    private Method decode(final Class<?> clazz) throws NoSuchMethodException
    {
        return clazz.getMethod("decode", int.class);
    }

    @Test
    public void doesNotGenerateClassForNonEnumFields()
    {
        assertThat(outputManager.getSources(), not(hasKey("EgNotEnum")));
    }

    @Test
    public void generatesIntBasedEnumField() throws Exception
    {
        final Class<?> clazz = compile(OTHER_ENUM);
        final Enum[] values = (Enum[])clazz.getEnumConstants();

        final Method decode = decode(clazz);

        assertEquals(values[0], decode.invoke(null, 1));
        assertEquals(values[1], decode.invoke(null, 12));
    }

    @Test
    public void generatesStringBasedEnumField() throws Exception
    {
        final Class<?> clazz = compile(STRING_ENUM);
        final Enum[] values = (Enum[])clazz.getEnumConstants();

        final Method decode = stringDecode(clazz);

        assertEquals(values[0], decode.invoke(null, "0"));
        assertEquals(values[1], decode.invoke(null, "A"));
        assertEquals(values[2], decode.invoke(null, "AA"));
    }

    @Test
    public void generatesCharArrayBasedDecode() throws Exception
    {
        final Class<?> clazz = compile(STRING_ENUM);
        final Enum[] values = (Enum[])clazz.getEnumConstants();

        final Method decode = clazz.getMethod("decode", char[].class, int.class);

        assertEquals(values[0], decode.invoke(null, "0".toCharArray(), 1));
        assertEquals(values[1], decode.invoke(null, "A".toCharArray(), 1));
        assertEquals(values[2], decode.invoke(null, "AA ".toCharArray(), 2));
    }

    @Test
    public void generateMultiStringValueValidation() throws Exception
    {
        final Class<?> clazz = compile(MULTI_STRING_VALUE_ENUM);

        final Method isValid = clazz.getMethod("isValid", char[].class, int.class);

        final char[] validArr = "0 AA".toCharArray();
        assertTrue((boolean)isValid.invoke(null, validArr, validArr.length));
        final char[] invalidArr = "0 AA B".toCharArray();
        assertFalse((boolean)isValid.invoke(null, invalidArr, invalidArr.length));
    }

    private Method stringDecode(final Class<?> clazz) throws NoSuchMethodException
    {
        return clazz.getMethod("decode", String.class);
    }

    private Class<?> compileEgEnum() throws Exception
    {
        return compile(EG_ENUM);
    }

    private Class<?> compile(final String className) throws ClassNotFoundException
    {
        //System.out.println(outputManager.getSources());
        return CompilerUtil.compileInMemory(className, outputManager.getSources());
    }

    private void assertRepresentation(final int expected, final Enum<?> enumElement) throws Exception
    {
        final int representation = (int)enumElement
            .getDeclaringClass()
            .getMethod("representation")
            .invoke(enumElement);

        assertEquals(expected, representation);
    }

    private void assertRepresentation(final char expected, final Enum<?> enumElement) throws Exception
    {
        final char representation = (char)enumElement
            .getDeclaringClass()
            .getMethod("representation")
            .invoke(enumElement);

        assertEquals(expected, representation);
    }

}
