/*
 * Copyright 2015-2018 Real Logic Ltd, Adaptive Financial Consulting Ltd.
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

import org.agrona.generation.StringWriterOutputManager;
import org.junit.BeforeClass;
import org.junit.Test;
import uk.co.real_logic.artio.builder.Decoder;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;

import static org.agrona.generation.CompilerUtil.compileInMemory;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static uk.co.real_logic.artio.dictionary.ExampleDictionary.ENCODED_MESSAGE;
import static uk.co.real_logic.artio.dictionary.ExampleDictionary.HAS_TEST_REQ_ID;
import static uk.co.real_logic.artio.dictionary.ExampleDictionary.HEADER_DECODER;
import static uk.co.real_logic.artio.dictionary.ExampleDictionary.HEARTBEAT_DECODER;
import static uk.co.real_logic.artio.dictionary.ExampleDictionary.MESSAGE_EXAMPLE;
import static uk.co.real_logic.artio.dictionary.ExampleDictionary.TEST_PACKAGE;
import static uk.co.real_logic.artio.dictionary.ExampleDictionary.TEST_PARENT_PACKAGE;
import static uk.co.real_logic.artio.dictionary.generation.DecoderGenerator.CODEC_LOGGING;
import static uk.co.real_logic.artio.util.Reflection.get;
import static uk.co.real_logic.artio.util.Reflection.getField;

public class DecoderGeneratorStringFlyweightTest
{
    private static final char[] ABC = "abc".toCharArray();
    private static final String ON_BEHALF_OF_COMP_ID = "onBehalfOfCompID";

    private static Class<?> heartbeat;
    private MutableAsciiBuffer buffer = new MutableAsciiBuffer(new byte[8 * 1024]);

    @BeforeClass
    public static void generate() throws Exception
    {
        System.setProperty("FLYWEIGHT_STRINGS", "true");
        final Map<String, CharSequence> sourcesWithValidation = generateSources(true, false);
        final Map<String, CharSequence> sourcesWithoutValidation = generateSources(false, false);
        heartbeat = compileInMemory(HEARTBEAT_DECODER, sourcesWithValidation);
        if (heartbeat == null || CODEC_LOGGING)
        {
            System.out.println("sourcesWithValidation = " + sourcesWithValidation);
        }
        compileInMemory(HEADER_DECODER, sourcesWithValidation);

        final Class<?> heartbeatWithoutValidation = compileInMemory(HEARTBEAT_DECODER, sourcesWithoutValidation);
        if (heartbeatWithoutValidation == null || CODEC_LOGGING)
        {
            System.out.println("sourcesWithoutValidation = " + sourcesWithoutValidation);
        }
    }

    @Test
    public void generatesGetters() throws NoSuchMethodException
    {
        assertHasMethod(ON_BEHALF_OF_COMP_ID, char[].class, heartbeat);
    }

    @Test
    public void stringGettersReadFromFields() throws Exception
    {
        final Decoder decoder = decodeHeartbeat(ENCODED_MESSAGE);
        assertArrayEquals(ABC, getOnBehalfOfCompId(decoder));
    }

    @Test
    public void flagsForOptionalFieldsInitiallyUnset() throws Exception
    {
        final Object decoder = heartbeat.getConstructor().newInstance();
        assertFalse("hasTestReqId initially true", hasTestReqId(decoder));
    }


    private void assertHasMethod(final String name, final Class<?> expectedReturnType, final Class<?> component)
        throws NoSuchMethodException
    {
        final Method method = component.getMethod(name);
        assertEquals(expectedReturnType, method.getReturnType());
    }

    private boolean hasTestReqId(final Object encoder) throws Exception
    {
        return (boolean)getField(encoder, HAS_TEST_REQ_ID);
    }

    private char[] getOnBehalfOfCompId(final Decoder decoder) throws Exception
    {
        return getCharArray(decoder, ON_BEHALF_OF_COMP_ID);
    }

    private char[] getCharArray(final Decoder decoder, final String name) throws Exception
    {
        final char[] value = (char[])get(decoder, name);
        final int length = (int)get(decoder, name + "Length");
        return Arrays.copyOf(value, length);
    }

    private Decoder decodeHeartbeat(final String example) throws Exception
    {
        final Decoder decoder = (Decoder)heartbeat.getConstructor().newInstance();
        decode(example, decoder);
        return decoder;
    }

    private void decode(final String example, final Decoder decoder)
    {
        buffer.putAscii(1, example);
        decoder.decode(buffer, 1, example.length());
    }

    private static Map<String, CharSequence> generateSources(final boolean validation,
        final boolean rejectingUnknownFields)
    {
        final Class<?> validationClass = validation ? ValidationOn.class : ValidationOff.class;
        final Class<?> rejectUnknownField = rejectingUnknownFields ?
            RejectUnknownFieldOn.class : RejectUnknownFieldOff.class;
        final StringWriterOutputManager outputManager = new StringWriterOutputManager();
        final ConstantGenerator constantGenerator = new ConstantGenerator(
            MESSAGE_EXAMPLE, TEST_PACKAGE, outputManager);
        final EnumGenerator enumGenerator = new EnumGenerator(MESSAGE_EXAMPLE, TEST_PARENT_PACKAGE, outputManager);
        final DecoderGenerator decoderGenerator = new DecoderGenerator(
            MESSAGE_EXAMPLE, 1, TEST_PACKAGE, TEST_PARENT_PACKAGE, outputManager, validationClass,
            rejectUnknownField);

        constantGenerator.generate();
        enumGenerator.generate();
        decoderGenerator.generate();
        return outputManager.getSources();
    }
}
