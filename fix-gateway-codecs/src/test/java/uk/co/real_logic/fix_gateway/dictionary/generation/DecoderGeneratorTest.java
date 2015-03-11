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
import org.junit.Ignore;
import org.junit.Test;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.agrona.generation.StringWriterOutputManager;
import uk.co.real_logic.fix_gateway.builder.Decoder;
import uk.co.real_logic.fix_gateway.util.MutableAsciiFlyweight;
import uk.co.real_logic.fix_gateway.util.Reflection;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

import static java.lang.reflect.Modifier.isAbstract;
import static java.lang.reflect.Modifier.isPublic;
import static org.junit.Assert.*;
import static uk.co.real_logic.agrona.generation.CompilerUtil.compileInMemory;
import static uk.co.real_logic.fix_gateway.dictionary.ExampleDictionary.*;
import static uk.co.real_logic.fix_gateway.util.Reflection.*;

public class DecoderGeneratorTest
{

    public static final char[] ABC = "abc".toCharArray();
    public static final String ON_BEHALF_OF_COMP_ID = "onBehalfOfCompID";

    private static StringWriterOutputManager outputManager = new StringWriterOutputManager();
    private static DecoderGenerator decoderGenerator = new DecoderGenerator(MESSAGE_EXAMPLE, TEST_PACKAGE, 3, outputManager);
    private static Class<?> heartbeat;
    private static Class<?> headerClass;

    private MutableAsciiFlyweight buffer = new MutableAsciiFlyweight(new UnsafeBuffer(new byte[8 * 1024]));

    @BeforeClass
    public static void generate() throws Exception
    {
        decoderGenerator.generate();
        final Map<String, CharSequence> sources = outputManager.getSources();
        System.out.println(sources);
        heartbeat = compileInMemory(HEARTBEAT_DECODER, sources);
        headerClass = compileInMemory(HEADER_DECODER, sources);
    }

    @Test
    public void generatesDecoderClass() throws Exception
    {
        assertNotNull("Not generated anything", heartbeat);
        assertIsDecoder(heartbeat);

        final int modifiers = heartbeat.getModifiers();
        assertFalse("Not instantiable", isAbstract(modifiers));
        assertTrue("Not public", isPublic(modifiers));
    }

    @Test
    public void generatesGetters() throws NoSuchMethodException
    {
        Method onBehalfOfCompID = heartbeat.getMethod(ON_BEHALF_OF_COMP_ID);
        assertEquals(char[].class, onBehalfOfCompID.getReturnType());
    }

    @Test
    public void stringGettersReadFromFields() throws Exception
    {
        final Object decoder = heartbeat.newInstance();
        setField(decoder, ON_BEHALF_OF_COMP_ID, ABC);

        char[] value = (char[]) get(decoder, ON_BEHALF_OF_COMP_ID);

        assertArrayEquals(ABC, value);
    }

    @Test
    public void flagsForOptionalFieldsInitiallyUnset() throws Exception
    {
        final Object decoder = heartbeat.newInstance();
        assertFalse("hasTestReqId initially true", hasTestReqId(decoder));
    }

    @Test(expected = InvocationTargetException.class)
    public void missingOptionalFieldCausesGetterToThrow() throws Exception
    {
        final Object decoder = heartbeat.newInstance();

        Reflection.get(decoder, TEST_REQ_ID);
    }

    @Test
    public void decodesValues() throws Exception
    {
        final Decoder decoder = (Decoder) heartbeat.newInstance();
        final int length = DERIVED_FIELDS_EXAMPLE.length();
        buffer.putAscii(1, DERIVED_FIELDS_EXAMPLE);

        decoder.decode(buffer, 1, length);

        assertArrayEquals(ABC, (char[]) get(decoder, ON_BEHALF_OF_COMP_ID));
        assertEquals(2, get(decoder, INT_FIELD));
        // TODO:
        //assertEquals(new DecimalFloat(11, 1), get(decoder, FLOAT_FIELD));
    }

    // TODO: optional fields

    //assertEquals(ABC, get(decoder, TEST_REQ_ID));
    //assertEquals(true, get(decoder, BOOLEAN_FIELD));
    //assertEquals(new byte[]{'1', '2', '3'}, get(decoder, DATA_FIELD));

    @Ignore
    @Test
    public void ignoresMissingOptionalValues() throws Exception
    {
        // TODO
    }

    // TODO: compound types
    // TODO: groups (RefMsgType used in session management)
    // TODO: nested groups
    // TODO: validation

    private void assertIsDecoder(final Class<?> cls)
    {
        assertTrue("Isn't a decoder", Decoder.class.isAssignableFrom(cls));
    }

    private boolean hasTestReqId(final Object encoder) throws Exception
    {
        return (boolean) getField(encoder, HAS_TEST_REQ_ID);
    }

}
