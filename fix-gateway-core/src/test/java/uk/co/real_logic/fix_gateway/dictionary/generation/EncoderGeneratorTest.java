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
import uk.co.real_logic.agrona.MutableDirectBuffer;
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
    private static final String VALUE = "abc";
    private static final byte[] VALUE_IN_BYTES = {97, 98, 99};
    private static final String TEST_REQ_ID = "testReqID";
    private static final String TEST_REQ_ID_LENGTH = "testReqIDLength";
    private static final String HAS_TEST_REQ_ID = "hasTestReqID";

    private StringWriterOutputManager outputManager = new StringWriterOutputManager();
    private EncoderGenerator encoderGenerator = new EncoderGenerator(MESSAGE_EXAMPLE, 3, outputManager);

    private Class<?> clazz;

    @Before
    public void generate() throws Exception
    {
        encoderGenerator.generate();
        System.out.println(outputManager.getSources());
        clazz = compileInMemory(HEARTBEAT, outputManager.getSources());
    }

    class Bar implements Encoder
    {

        public int encode(final MutableDirectBuffer buffer, final int offset)
        {
            return 0;
        }
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
    public void generatesSetters() throws NoSuchMethodException
    {
        clazz.getMethod("onBehalfOfCompID", CharSequence.class);
    }

    @Test
    public void stringSettersWriteToFields() throws Exception
    {
        final Object encoder = clazz.newInstance();

        setTestReqId(encoder);

        assertTestReqIsValue(encoder);
    }

    @Test
    public void stringSettersResizeByteArray() throws Exception
    {
        final Object encoder = clazz.newInstance();

        setTestReqIdTo(encoder, "abcd");

        assertArrayEquals(new byte[]{97,98,99,100}, (byte[]) getField(encoder, TEST_REQ_ID));
        assertEquals(4, getField(encoder, TEST_REQ_ID_LENGTH));
    }

    @Test
    public void charArraySettersWriteToFields() throws Exception
    {
        final Object encoder = clazz.newInstance();

        final Object value = new char[] {'a', 'b', 'c'};
        clazz.getMethod(TEST_REQ_ID, char[].class)
             .invoke(encoder, value);

        assertTestReqIsValue(encoder);
    }

    @Test
    public void flagsForOptionalFieldsInitiallyUnset() throws Exception
    {
        final Object encoder = clazz.newInstance();
        assertFalse("hasTestReqId initially true", hasTestReqId(encoder));
    }

    @Test
    public void flagsForOptionalFieldsUpdated() throws Exception
    {
        final Object encoder = clazz.newInstance();

        setTestReqId(encoder);

        assertTrue("hasTestReqId not updated", hasTestReqId(encoder));
    }

    private void assertTestReqIsValue(final Object encoder) throws Exception
    {
        assertArrayEquals(VALUE_IN_BYTES, (byte[]) getField(encoder, TEST_REQ_ID));
        assertEquals(3, getField(encoder, TEST_REQ_ID_LENGTH));
    }

    private boolean hasTestReqId(final Object encoder) throws Exception
    {
        return (boolean) getField(encoder, HAS_TEST_REQ_ID);
    }

    private void setTestReqId(final Object encoder) throws Exception
    {
        setTestReqIdTo(encoder, VALUE);
    }

    private void setTestReqIdTo(final Object encoder, final String value) throws Exception
    {
        clazz.getMethod(TEST_REQ_ID, CharSequence.class)
                .invoke(encoder, value);
    }

    private Object getField(final Object encoder, final String fieldName) throws Exception
    {
        final Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(encoder);
    }

    // TODO: encode method
    // TODO: common header and footer
    // TODO: primitive fields
    // TODO: complex encoding data types - eg dates/float/etc
    // TODO: encoding Strings from direct buffers
    // TODO: encoding Strings from a char[]?
    // TODO: encoding Strings from an ascii flyweight?

}
