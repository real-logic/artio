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

import org.agrona.generation.CompilerUtil;
import org.agrona.generation.StringWriterOutputManager;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static uk.co.real_logic.artio.dictionary.ExampleDictionary.*;
import static uk.co.real_logic.artio.dictionary.generation.GenerationUtil.PARENT_PACKAGE;

public class EnumGeneratorTest
{

    private Map<String, CharSequence> sources;
    private Map<String, CharSequence> sourcesWithSentinelValue;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void generate()
    {
        sources = generateEnums(false);
        sourcesWithSentinelValue = generateEnums(true);
    }

    @Test
    public void generatesEnumClass() throws Exception
    {
        final Class<?> clazz = compileEgEnum(sources);

        assertNotNull("Failed to generate a class", clazz);
        assertTrue("Generated class isn't an enum", clazz.isEnum());
    }

    @Test
    public void generatesEnumConstants() throws Exception
    {
        final Class<?> clazz = compileEgEnum(sources);
        final Enum[] values = (Enum[])clazz.getEnumConstants();

        assertThat(values, arrayWithSize(3));

        assertEquals("AnEntry", values[0].name());
        assertRepresentation('a', values[0]);
        assertEquals("AnotherEntry", values[1].name());
        assertRepresentation('b', values[1]);
        assertEquals("UNKNOWN_REPRESENTATION", values[2].name());
        assertRepresentation('\u0000', values[2]);
    }

    @Test
    public void generatesLookupTable() throws Exception
    {
        final Class<?> clazz = compileEgEnum(sources);
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
        assertThat(sources, not(hasKey("EgNotEnum")));
    }

    @Test
    public void generatesIntBasedEnumField() throws Exception
    {
        final Class<?> clazz = compile(OTHER_ENUM, sources);
        final Enum[] values = (Enum[])clazz.getEnumConstants();

        final Method decode = decode(clazz);

        assertEquals(values[0], decode.invoke(null, 1));
        assertEquals(values[1], decode.invoke(null, 12));
    }

    @Test
    public void generatesStringBasedEnumField() throws Exception
    {
        final Class<?> clazz = compile(STRING_ENUM, sources);
        final Enum[] values = (Enum[])clazz.getEnumConstants();

        final Method decode = stringDecode(clazz);

        assertEquals(values[0], decode.invoke(null, "0"));
        assertEquals(values[1], decode.invoke(null, "A"));
        assertEquals(values[2], decode.invoke(null, "AA"));
    }

    @Test
    public void generatesCharArrayBasedDecode() throws Exception
    {
        final Class<?> clazz = compile(STRING_ENUM, sources);
        final Enum[] values = (Enum[])clazz.getEnumConstants();

        final Method decode = clazz.getMethod("decode", char[].class, int.class);

        assertEquals(values[0], decode.invoke(null, "0".toCharArray(), 1));
        assertEquals(values[1], decode.invoke(null, "A".toCharArray(), 1));
        assertEquals(values[2], decode.invoke(null, "AA ".toCharArray(), 2));
    }

    @Test
    public void shouldThrowWhenDecodingUnknownRepresentationCharArray() throws Throwable
    {
        final Class<?> clazz = compile(STRING_ENUM, sources);
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Unknown: UnknownRepresentation");

        final Method decode = clazz.getMethod("decode", char[].class, int.class);

        final char[] unknownRepresentation = "UnknownRepresentation".toCharArray();
        invoke(decode, unknownRepresentation, unknownRepresentation.length);
    }

    @Test
    public void shouldThrowWhenDecodingUnknownRepresentationString() throws Throwable
    {
        final Class<?> clazz = compile(STRING_ENUM, sources);
        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("Unknown: UnknownRepresentation");

        final Method decode = clazz.getMethod("decode", String.class);

        invoke(decode, "UnknownRepresentation");
    }

    @Test
    public void shouldReturnSentinelValueWhenDecodingUnknownRepresentation() throws Exception
    {
        final Class<?> clazz = compile(STRING_ENUM, sourcesWithSentinelValue);
        final Enum[] values = (Enum[])clazz.getEnumConstants();

        final Method decodeCharArray = clazz.getMethod("decode", char[].class, int.class);
        final Method decodeString = clazz.getMethod("decode", String.class);

        final String unknownRepresentation = "UnknownRepresentation";
        assertEquals(values[values.length - 1], decodeCharArray.invoke(null, unknownRepresentation.toCharArray(),
            unknownRepresentation.length()));
        assertEquals(values[values.length - 1], decodeString.invoke(null, unknownRepresentation));
    }

    private Method stringDecode(final Class<?> clazz) throws NoSuchMethodException
    {
        return clazz.getMethod("decode", String.class);
    }

    private Class<?> compileEgEnum(final Map<String, CharSequence> sources) throws Exception
    {
        return compile(EG_ENUM, sources);
    }

    private Class<?> compile(final String className, final Map<String, CharSequence> sources)
        throws ClassNotFoundException
    {
        return CompilerUtil.compileInMemory(className, sources);
    }

    private void assertRepresentation(final char expected, final Enum<?> enumElement) throws Exception
    {
        final char representation = (char)enumElement
            .getDeclaringClass()
            .getMethod("representation")
            .invoke(enumElement);

        assertEquals(expected, representation);
    }

    private void invoke(final Method method, final Object... argument) throws Throwable
    {
        try
        {
            method.invoke(null, argument);
        }
        catch (final InvocationTargetException e)
        {
            throw e.getCause();
        }
    }

    private Map<String, CharSequence> generateEnums(final boolean useEnumSentinelValue)
    {
        final Class<?> sentinelValueClass = useEnumSentinelValue ? EnumSentinelOn.class : EnumSentinelOff.class;
        final StringWriterOutputManager outputManager = new StringWriterOutputManager();
        final EnumGenerator enumGenerator = new EnumGenerator(FIELD_EXAMPLE, PARENT_PACKAGE, outputManager,
            sentinelValueClass);
        enumGenerator.generate();
        return outputManager.getSources();
    }
}
