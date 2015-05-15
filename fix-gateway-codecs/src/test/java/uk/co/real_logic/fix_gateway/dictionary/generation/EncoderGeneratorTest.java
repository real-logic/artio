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

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.agrona.generation.StringWriterOutputManager;
import uk.co.real_logic.fix_gateway.builder.Encoder;
import uk.co.real_logic.fix_gateway.builder.MessageEncoder;
import uk.co.real_logic.fix_gateway.fields.DecimalFloat;
import uk.co.real_logic.fix_gateway.util.MutableAsciiFlyweight;
import uk.co.real_logic.fix_gateway.util.Reflection;

import java.util.Map;

import static java.lang.reflect.Modifier.isAbstract;
import static java.lang.reflect.Modifier.isPublic;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static uk.co.real_logic.agrona.generation.CompilerUtil.compileInMemory;
import static uk.co.real_logic.fix_gateway.dictionary.ExampleDictionary.*;
import static uk.co.real_logic.fix_gateway.util.CustomMatchers.containsAscii;
import static uk.co.real_logic.fix_gateway.util.Reflection.*;

public class EncoderGeneratorTest
{
    private static final StringWriterOutputManager OUTPUT_MANAGER = new StringWriterOutputManager();
    private static final EncoderGenerator ENCODER_GENERATOR =
        new EncoderGenerator(MESSAGE_EXAMPLE, 1, TEST_PACKAGE, OUTPUT_MANAGER);

    private static Class<?> heartbeat;
    private static Class<?> headerClass;

    private MutableAsciiFlyweight buffer = new MutableAsciiFlyweight(new UnsafeBuffer(new byte[8 * 1024]));

    @BeforeClass
    public static void generate() throws Exception
    {
        ENCODER_GENERATOR.generate();
        final Map<String, CharSequence> sources = OUTPUT_MANAGER.getSources();
        //System.out.println(sources);
        heartbeat = compileInMemory(HEARTBEAT_ENCODER, sources);
        headerClass = compileInMemory(HEADER_ENCODER, sources);
    }

    @Test
    public void generatesEncoderClass() throws Exception
    {
        assertNotNull("Not generated anything", heartbeat);
        assertNotNull(heartbeat);
        assertTrue(MessageEncoder.class.isAssignableFrom(heartbeat));

        final int modifiers = heartbeat.getModifiers();
        assertFalse("Not instantiable", isAbstract(modifiers));
        assertTrue("Not public", isPublic(modifiers));
    }

    @Test
    public void generatesSetters() throws NoSuchMethodException
    {
        heartbeat.getMethod("onBehalfOfCompID", CharSequence.class);
    }

    @Test
    public void stringSettersWriteToFields() throws Exception
    {
        final Object encoder = heartbeat.newInstance();

        setTestReqId(encoder);

        assertTestReqIsValue(encoder);
    }

    @Test
    public void stringSettersResizeByteArray() throws Exception
    {
        final Object encoder = heartbeat.newInstance();

        setTestReqIdTo(encoder, "abcd");

        assertArrayEquals(new byte[]{97, 98, 99, 100}, (byte[]) getField(encoder, TEST_REQ_ID));
        assertEquals(4, getField(encoder, TEST_REQ_ID_LENGTH));
    }

    @Test
    public void charArraySettersWriteToFields() throws Exception
    {
        final Object encoder = heartbeat.newInstance();

        final Object value = new char[] {'a', 'b', 'c'};
        heartbeat.getMethod(TEST_REQ_ID, char[].class)
                 .invoke(encoder, value);

        assertTestReqIsValue(encoder);
    }

    @Test
    public void intSettersWriteToFields() throws Exception
    {
        final Object encoder = heartbeat.newInstance();

        setInt(encoder, INT_FIELD, 1);

        assertEquals(1, getField(encoder, INT_FIELD));
    }

    @Test
    public void floatSettersWriteToFields() throws Exception
    {
        final Object encoder = heartbeat.newInstance();

        final DecimalFloat value = new DecimalFloat(1, 2);

        setFloat(encoder, FLOAT_FIELD, value);

        Assert.assertEquals(value, getField(encoder, FLOAT_FIELD));
    }

    @Test
    public void flagsForOptionalFieldsInitiallyUnset() throws Exception
    {
        final Object encoder = heartbeat.newInstance();
        assertFalse("hasTestReqId initially true", hasTestReqId(encoder));
    }

    @Test
    public void flagsForOptionalFieldsUpdated() throws Exception
    {
        final Object encoder = heartbeat.newInstance();

        setTestReqId(encoder);

        assertTrue("hasTestReqId not updated", hasTestReqId(encoder));
    }

    @Test
    public void shouldGenerateHeader() throws Exception
    {
        assertNotNull(headerClass);
        assertTrue(Encoder.class.isAssignableFrom(headerClass));

        final Encoder header = (Encoder) headerClass.newInstance();

        setCharSequence(header, "beginString", "abc");
        setCharSequence(header, "msgType", "abc");

        assertEncodesTo(header, "8=abc\0019=0000\00135=abc\001");
    }

    @Test
    public void encodesValues() throws Exception
    {
        final Encoder encoder = (Encoder) heartbeat.newInstance();

        setRequiredFields(encoder);

        setOptionalFields(encoder);
        setupHeader(encoder);
        setupTrailer(encoder);

        assertEncodesTo(encoder, ENCODED_MESSAGE_EXAMPLE);
    }

    @Test
    public void ignoresMissingOptionalValues() throws Exception
    {
        final Encoder encoder = (Encoder) heartbeat.newInstance();

        setRequiredFields(encoder);
        setupHeader(encoder);
        setupTrailer(encoder);

        assertEncodesTo(encoder, NO_OPTIONAL_MESSAGE_EXAMPLE);
    }

    @Test
    public void automaticallyComputesDerivedHeaderAndTrailerFields() throws Exception
    {
        final Encoder encoder = (Encoder) heartbeat.newInstance();

        setRequiredFields(encoder);

        assertEncodesTo(encoder, DERIVED_FIELDS_EXAMPLE);
    }

    @Test
    public void shouldGenerateHumanReadableToString() throws Exception
    {
        final Encoder encoder = (Encoder) heartbeat.newInstance();

        setRequiredFields(encoder);

        assertThat(encoder.toString(), endsWith(STRING_NO_OPTIONAL_MESSAGE_SUFFIX));
    }

    @Test
    public void shouldIncludeOptionalFieldsInToString() throws Exception
    {
        final Encoder encoder = (Encoder) heartbeat.newInstance();

        setRequiredFields(encoder);
        setOptionalFields(encoder);

        assertThat(encoder.toString(), endsWith(STRING_ENCODED_MESSAGE_SUFFIX));
    }

    @Test
    public void shouldEncodeShorterStringsAfterLongerStrings() throws Exception
    {
        final Encoder encoder = (Encoder) heartbeat.newInstance();

        setRequiredFields(encoder);

        encoder.encode(buffer, 1);

        setCharSequence(encoder, "onBehalfOfCompID", "ab");

        assertEncodesTo(encoder, SHORTER_STRING_EXAMPLE);
    }

    @Test
    public void shouldToStringShorterStringsAfterLongerStrings() throws Exception
    {
        final Encoder encoder = (Encoder) heartbeat.newInstance();

        setRequiredFields(encoder);

        setCharSequence(encoder, "onBehalfOfCompID", "ab");

        assertThat(encoder.toString(), containsString("ab"));
        assertThat(encoder.toString(), not(containsString("abc")));
    }

    @Test
    public void shouldEncodeRepeatingGroups() throws Exception
    {
        final Encoder encoder = (Encoder) heartbeat.newInstance();
        setRequiredFields(encoder);

        Object group = get(encoder, "egGroup");
        setGroupField(group, 1);

        group = next(group);
        setGroupField(group, 2);

        assertEncodesTo(encoder, REPEATING_GROUP_EXAMPLE);
    }

    private void setGroupField(final Object tradingSessions, final int value) throws Exception
    {
        setInt(tradingSessions, "groupField", value);
    }

    private Object next(final Object tradingSessions) throws Exception
    {
        return call(tradingSessions, "next");
    }

    // TODO: lengths get resized down from initial buffer length being too high.
    // TODO: reset method
    // TODO: compound types
    // TODO: groups (RefMsgType used in session management)
    // TODO: nested groups

    private void setupHeader(final Encoder encoder) throws Exception
    {
        final Object header = Reflection.get(encoder, "header");
        setCharSequence(header, "beginString", "FIX.4.4");
        setCharSequence(header, "msgType", "0");
    }

    private void setRequiredFields(Encoder encoder) throws Exception
    {
        setCharSequence(encoder, "onBehalfOfCompID", "abc");
        setInt(encoder, INT_FIELD, 2);
        setFloat(encoder, FLOAT_FIELD, new DecimalFloat(11, 1));
    }

    private void setOptionalFields(Encoder encoder) throws Exception
    {
        setTestReqIdTo(encoder, ABC);
        setBoolean(encoder, BOOLEAN_FIELD, true);
        setByteArray(encoder, DATA_FIELD, new byte[]{'1', '2', '3'});
    }

    private void setupTrailer(final Encoder encoder) throws Exception
    {
        final Object trailer = Reflection.get(encoder, "trailer");
        setCharSequence(trailer, "checkSum", "12");
    }

    private void assertEncodesTo(final Encoder encoder, final String value)
    {
        final int length = encoder.encode(buffer, 1);
        assertThat(buffer, containsAscii(value, 1, value.length()));
        assertEquals(value.length(), length);
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
        setTestReqIdTo(encoder, ABC);
    }

    private void setTestReqIdTo(final Object encoder, final String value) throws Exception
    {
        setCharSequence(encoder, TEST_REQ_ID, value);
    }

}
