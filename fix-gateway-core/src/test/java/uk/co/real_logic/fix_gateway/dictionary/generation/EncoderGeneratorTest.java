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
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.agrona.generation.StringWriterOutputManager;
import uk.co.real_logic.fix_gateway.builder.Encoder;
import uk.co.real_logic.fix_gateway.fields.DecimalFloat;
import uk.co.real_logic.fix_gateway.util.MutableAsciiFlyweight;

import java.lang.reflect.Field;

import static java.lang.reflect.Modifier.isAbstract;
import static java.lang.reflect.Modifier.isPublic;
import static org.junit.Assert.*;
import static uk.co.real_logic.agrona.generation.CompilerUtil.compileInMemory;
import static uk.co.real_logic.fix_gateway.dictionary.ExampleDictionary.*;
import static uk.co.real_logic.fix_gateway.util.CustomMatchers.containsAscii;
import static uk.co.real_logic.fix_gateway.util.Reflection.*;

public class EncoderGeneratorTest
{
    private static final String VALUE = "abc";
    private static final byte[] VALUE_IN_BYTES = {97, 98, 99};
    private static final String TEST_REQ_ID = "testReqID";
    private static final String INT_FIELD = "intField";
    private static final String FLOAT_FIELD = "floatField";
    private static final String TEST_REQ_ID_LENGTH = "testReqIDLength";
    private static final String HAS_TEST_REQ_ID = "hasTestReqID";

    private StringWriterOutputManager outputManager = new StringWriterOutputManager();
    private EncoderGenerator encoderGenerator = new EncoderGenerator(MESSAGE_EXAMPLE, 3, outputManager);

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

        assertArrayEquals(new byte[]{97, 98, 99, 100}, (byte[]) getField(encoder, TEST_REQ_ID));
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
    public void intSettersWriteToFields() throws Exception
    {
        final Object encoder = clazz.newInstance();

        setInt(encoder, INT_FIELD, 1);

        assertEquals(1, getField(encoder, INT_FIELD));
    }

    @Test
    public void floatSettersWriteToFields() throws Exception
    {
        final Object encoder = clazz.newInstance();

        DecimalFloat value = new DecimalFloat(1, 2);

        setFloat(encoder, FLOAT_FIELD, value);

        assertEquals(value, getField(encoder, FLOAT_FIELD));
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

    @Test
    public void encodesValues() throws Exception
    {
        final int length = ENCODED_MESSAGE_EXAMPLE.length();

        final MutableAsciiFlyweight buffer = new MutableAsciiFlyweight(new UnsafeBuffer(new byte[8 * 1024]));
        final Encoder encoder = (Encoder) clazz.newInstance();

        setCharSequence(encoder, "onBehalfOfCompID", "abc");
        setTestReqIdTo(encoder, VALUE);
        setInt(encoder, INT_FIELD, 2);
        setFloat(encoder, FLOAT_FIELD, new DecimalFloat(11, 1));

        final int encodedLength = encoder.encode(buffer, 1);

        assertThat(buffer, containsAscii(ENCODED_MESSAGE_EXAMPLE, 1, length));
        assertEquals(length, encodedLength);
    }

    @Test
    public void ignoresMissingOptionalValues() throws Exception
    {
        final int length = NO_OPTIONAL_MESSAGE_EXAMPLE.length();

        final MutableAsciiFlyweight buffer = new MutableAsciiFlyweight(new UnsafeBuffer(new byte[8 * 1024]));
        final Encoder encoder = (Encoder) clazz.newInstance();

        setCharSequence(encoder, "onBehalfOfCompID", "abc");
        setInt(encoder, INT_FIELD, 2);
        setFloat(encoder, FLOAT_FIELD, new DecimalFloat(11, 1));

        final int encodedLength = encoder.encode(buffer, 1);

        assertThat(buffer, containsAscii(NO_OPTIONAL_MESSAGE_EXAMPLE, 1, length));
        assertEquals(length, encodedLength);
    }

    // TODO: common header and footer
    // TODO: checksum of encoded message
    // TODO: composite types
    // TODO: groups
    // TODO: nested groups

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
        setCharSequence(encoder, TEST_REQ_ID, value);
    }

    private Object getField(final Object encoder, final String fieldName) throws Exception
    {
        final Field field = clazz.getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(encoder);
    }

}
