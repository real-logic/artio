/*
 * Copyright 2013 Real Logic Limited.
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
package uk.co.real_logic.artio.dictionary.generation;

import org.agrona.generation.CompilerUtil;
import org.agrona.generation.StringWriterOutputManager;
import org.junit.BeforeClass;
import org.junit.Test;
import uk.co.real_logic.artio.dictionary.CharArrayWrapper;

import java.lang.reflect.Method;
import java.util.Map;

import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static uk.co.real_logic.artio.dictionary.ExampleDictionary.*;
import static uk.co.real_logic.artio.dictionary.generation.AbstractDecoderGeneratorTest.CODEC_LOGGING;
import static uk.co.real_logic.artio.dictionary.generation.CodecConfiguration.DEFAULT_PARENT_PACKAGE;
import static uk.co.real_logic.artio.dictionary.generation.CodecUtil.ENUM_MISSING_CHAR;
import static uk.co.real_logic.artio.dictionary.generation.CodecUtil.ENUM_UNKNOWN_CHAR;
import static uk.co.real_logic.artio.dictionary.generation.EnumGenerator.NULL_VAL_NAME;
import static uk.co.real_logic.artio.dictionary.generation.EnumGenerator.UNKNOWN_NAME;

public class EnumGeneratorTest
{
    private static Map<String, CharSequence> sources;
    private static Class<?> egEnumClass;
    private static Class<?> otherEnumClass;
    private static Class<?> stringEnumClass;
    private static Class<?> currencyEnumClass;

    @BeforeClass
    public static void generate() throws Exception
    {
        sources = generateEnums();
        if (CODEC_LOGGING)
        {
            System.err.println(sources);
        }
        egEnumClass = compileEgEnum(sources);
        otherEnumClass = compile(OTHER_ENUM, sources);
        stringEnumClass = compile(STRING_ENUM, sources);
        currencyEnumClass = compile(CURRENCY_ENUM, sources);
    }

    @Test
    public void generatesEnumClass()
    {
        assertNotNull("Failed to generate a class", egEnumClass);
        assertTrue("Generated class isn't an enum", egEnumClass.isEnum());

        assertNotNull(currencyEnumClass);
    }

    @Test
    public void generatesEnumConstants() throws Exception
    {
        final Enum<?>[] values = egEnumConstants();

        assertThat(values, arrayWithSize(4));

        assertEquals("AnEntry", values[0].name());
        assertRepresentation('a', values[0]);
        assertEquals("AnotherEntry", values[1].name());
        assertRepresentation('b', values[1]);
        assertEquals(NULL_VAL_NAME, values[2].name());
        assertRepresentation(ENUM_MISSING_CHAR, values[2]);
        assertEquals(UNKNOWN_NAME, values[3].name());
        assertRepresentation(ENUM_UNKNOWN_CHAR, values[3]);
    }

    @Test
    public void generatesLookupTable() throws Exception
    {
        final Enum<?>[] values = egEnumConstants();

        final Method decode = decode(egEnumClass);

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
        final Enum<?>[] values = (Enum<?>[])otherEnumClass.getEnumConstants();

        final Method decode = decode(otherEnumClass);

        assertEquals(values[0], decode.invoke(null, 1));
        assertEquals(values[1], decode.invoke(null, 12));
        assertEquals(values[2], decode.invoke(null, 99));
    }

    @Test
    public void generatesStringBasedEnumField() throws Exception
    {
        final Enum<?>[] values = getStringEnumConstants();

        final Method decode = stringDecode(stringEnumClass);

        assertEquals(values[0], decode.invoke(null, "0"));
        assertEquals(values[1], decode.invoke(null, "A"));
        assertEquals(values[2], decode.invoke(null, "AA"));
    }

    private Enum<?>[] getStringEnumConstants()
    {
        return (Enum<?>[])stringEnumClass.getEnumConstants();
    }

    @Test
    public void generatesCharArrayBasedDecode() throws Exception
    {
        final Enum<?>[] values = getStringEnumConstants();
        final CharArrayWrapper wrapper = new CharArrayWrapper();
        final Method decode = stringEnumClass.getMethod("decode", CharArrayWrapper.class);

        wrapper.wrap("0".toCharArray(), 1);
        assertEquals(values[0], decode.invoke(null, wrapper));

        wrapper.wrap("A".toCharArray(), 1);
        assertEquals(values[1], decode.invoke(null, wrapper));

        wrapper.wrap("AA ".toCharArray(), 2);
        assertEquals(values[2], decode.invoke(null, wrapper));
    }

    @Test
    public void shouldReturnSentinelValueWhenDecodingUnknownRepresentation() throws Exception
    {
        final Enum<?>[] values = getStringEnumConstants();

        final Method decodeCharArray = stringEnumClass.getMethod("decode", CharArrayWrapper.class);
        final Method decodeString = stringEnumClass.getMethod("decode", String.class);

        final String unknownRepresentation = "UnknownRepresentation";
        final CharArrayWrapper wrapper = new CharArrayWrapper();
        wrapper.wrap(unknownRepresentation.toCharArray(), unknownRepresentation.length());
        assertEquals(values[values.length - 1], decodeCharArray.invoke(null, wrapper));
        assertEquals(values[values.length - 1], decodeString.invoke(null, unknownRepresentation));
    }

    private Method stringDecode(final Class<?> clazz) throws NoSuchMethodException
    {
        return clazz.getMethod("decode", String.class);
    }

    private static Class<?> compileEgEnum(final Map<String, CharSequence> sources) throws Exception
    {
        return compile(EG_ENUM, sources);
    }

    private static Class<?> compile(final String className, final Map<String, CharSequence> sources)
        throws ClassNotFoundException
    {
        return CompilerUtil.compileInMemory(className, sources);
    }

    public static void assertRepresentation(final char expected, final Enum<?> enumElement) throws Exception
    {
        final char representation = (char)enumElement
            .getDeclaringClass()
            .getMethod("representation")
            .invoke(enumElement);

        assertEquals(expected, representation);
    }

    private static Map<String, CharSequence> generateEnums()
    {
        final StringWriterOutputManager outputManager = new StringWriterOutputManager();
        final EnumGenerator enumGenerator = new EnumGenerator(FIELD_EXAMPLE, DEFAULT_PARENT_PACKAGE, outputManager);
        enumGenerator.generate();
        return outputManager.getSources();
    }

    private Enum<?>[] egEnumConstants()
    {
        return (Enum<?>[])egEnumClass.getEnumConstants();
    }
}
