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

import java.lang.reflect.Field;
import java.util.Map;

import static java.lang.reflect.Modifier.isAbstract;
import static java.lang.reflect.Modifier.isPublic;
import static org.junit.Assert.*;
import static uk.co.real_logic.agrona.generation.CompilerUtil.compileInMemory;
import static uk.co.real_logic.fix_gateway.dictionary.ExampleDictionary.*;

public class DecoderGeneratorTest
{

    private static StringWriterOutputManager outputManager = new StringWriterOutputManager();
    private static DecoderGenerator decoderGenerator = new DecoderGenerator(MESSAGE_EXAMPLE, TEST_PACKAGE, outputManager);
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
    public void generatesEncoderClass() throws Exception
    {
        assertNotNull("Not generated anything", heartbeat);
        assertIsDecoder(heartbeat);

        final int modifiers = heartbeat.getModifiers();
        assertFalse("Not instantiable", isAbstract(modifiers));
        assertTrue("Not public", isPublic(modifiers));
    }

    @Ignore
    @Test
    public void generatesGetters() throws NoSuchMethodException
    {
        heartbeat.getMethod("onBehalfOfCompID");
    }

    @Ignore
    @Test
    public void stringGettersReadFromFields() throws Exception
    {
        final Object decoder = heartbeat.newInstance();

        // TODO
        //setTestReqId(decoder);

        //assertTestReqIsValue(decoder);
    }

    @Ignore
    @Test
    public void flagsForOptionalFieldsInitiallyUnset() throws Exception
    {
        final Object decoder = heartbeat.newInstance();
        assertFalse("hasTestReqId initially true", hasTestReqId(decoder));
    }

    @Ignore
    @Test
    public void flagsForOptionalFieldsUpdated() throws Exception
    {
        final Object decoder = heartbeat.newInstance();

        // TODO

        assertTrue("hasTestReqId not updated", hasTestReqId(decoder));
    }

    @Ignore
    @Test
    public void decodesValues() throws Exception
    {
        // TODO
    }

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

    private Object getField(final Object encoder, final String fieldName) throws Exception
    {
        final Field field = heartbeat.getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(encoder);
    }

}
