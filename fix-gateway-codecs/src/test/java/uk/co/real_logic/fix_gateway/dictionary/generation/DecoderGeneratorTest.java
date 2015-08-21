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
import org.junit.Test;
import uk.co.real_logic.agrona.collections.IntHashSet;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.agrona.generation.StringWriterOutputManager;
import uk.co.real_logic.fix_gateway.builder.Decoder;
import uk.co.real_logic.fix_gateway.fields.DecimalFloat;
import uk.co.real_logic.fix_gateway.util.MutableAsciiFlyweight;
import uk.co.real_logic.fix_gateway.util.Reflection;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import static java.lang.reflect.Modifier.isAbstract;
import static java.lang.reflect.Modifier.isPublic;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static uk.co.real_logic.agrona.generation.CompilerUtil.compileInMemory;
import static uk.co.real_logic.fix_gateway.builder.Decoder.NO_ERROR;
import static uk.co.real_logic.fix_gateway.dictionary.ExampleDictionary.*;
import static uk.co.real_logic.fix_gateway.dictionary.generation.CodecUtil.MISSING_INT;
import static uk.co.real_logic.fix_gateway.dictionary.generation.DecoderGenerator.*;
import static uk.co.real_logic.fix_gateway.util.Reflection.*;

public class DecoderGeneratorTest
{
    public static final char[] ABC = "abc".toCharArray();
    public static final char[] AB = "ab".toCharArray();
    public static final String ON_BEHALF_OF_COMP_ID = "onBehalfOfCompID";

    private static StringWriterOutputManager outputManager = new StringWriterOutputManager();
    private static ConstantGenerator constantGenerator = new ConstantGenerator(MESSAGE_EXAMPLE, TEST_PACKAGE, outputManager);
    private static DecoderGenerator decoderGenerator = new DecoderGenerator(MESSAGE_EXAMPLE, 1, TEST_PACKAGE, outputManager);
    private static Class<?> heartbeat;
    private static Class<?> component;

    private MutableAsciiFlyweight buffer = new MutableAsciiFlyweight(new UnsafeBuffer(new byte[8 * 1024]));

    @BeforeClass
    public static void generate() throws Exception
    {
        constantGenerator.generate();
        decoderGenerator.generate();
        final Map<String, CharSequence> sources = outputManager.getSources();
        heartbeat = compileInMemory(HEARTBEAT_DECODER, sources);
        if (heartbeat == null)
        {
            System.out.println(sources);
        }
        component = heartbeat.getClassLoader().loadClass(COMPONENT_DECODER);
        compileInMemory(HEADER_DECODER, sources);
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
        final Method onBehalfOfCompID = heartbeat.getMethod(ON_BEHALF_OF_COMP_ID);
        assertEquals(char[].class, onBehalfOfCompID.getReturnType());
    }

    @Test
    public void stringGettersReadFromFields() throws Exception
    {
        final Decoder decoder = (Decoder) heartbeat.newInstance();
        setField(decoder, ON_BEHALF_OF_COMP_ID, ABC);
        setField(decoder, ON_BEHALF_OF_COMP_ID + "Length", 3);

        assertArrayEquals(ABC, getOnBehalfOfCompId(decoder));
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
        final Decoder decoder = decodeHeartbeat(DERIVED_FIELDS_MESSAGE);

        assertArrayEquals(ABC, getOnBehalfOfCompId(decoder));
        assertEquals(2, getIntField(decoder));
        assertEquals(new DecimalFloat(11, 1), getFloatField(decoder));

        assertValid(decoder);
    }


    @Test
    public void ignoresMissingOptionalValues() throws Exception
    {
        final Decoder decoder = decodeHeartbeat(DERIVED_FIELDS_MESSAGE);

        assertFalse(hasTestReqId(decoder));
        assertFalse(hasBooleanField(decoder));
        assertFalse(hasDataField(decoder));

        assertValid(decoder);
    }

    @Test
    public void setsMissingOptionalValues() throws Exception
    {
        final Decoder decoder = decodeHeartbeat(ENCODED_MESSAGE);

        assertTrue(hasTestReqId(decoder));
        assertTrue(hasBooleanField(decoder));
        assertTrue(hasDataField(decoder));

        assertArrayEquals(ABC, getTestReqId(decoder));
        assertEquals(true, getBooleanField(decoder));
        assertArrayEquals(new byte[]{'1', '2', '3'}, getDataField(decoder));

        assertValid(decoder);
    }

    @Test
    public void hasMessageTypeFlag() throws Exception
    {
        final int messageType = (int) heartbeat.getField("MESSAGE_TYPE").get(null);

        assertEquals(HEARTBEAT_TYPE, messageType);
    }

    @Test
    public void decodesCommonComponents() throws Exception
    {
        final Decoder decoder = decodeHeartbeat(ENCODED_MESSAGE);

        final Decoder header = getHeader(decoder);

        assertEquals(49, getBodyLength(header));
    }

    @Test
    public void shouldResetFields() throws Exception
    {
        final Decoder decoder = decodeHeartbeat(ENCODED_MESSAGE);

        decoder.reset();

        assertFalse(hasTestReqId(decoder));
        assertFalse(hasBooleanField(decoder));
        assertFalse(hasDataField(decoder));

        assertEquals(MISSING_INT, getIntField(decoder));
    }

    @Test
    public void shouldGenerateHumanReadableToString() throws Exception
    {
        final Decoder decoder = decodeHeartbeat(NO_OPTIONAL_MESSAGE);

        assertThat(decoder.toString(), containsString(STRING_NO_OPTIONAL_MESSAGE_EXAMPLE));

        assertValid(decoder);
    }

    @Test
    public void shouldIncludeOptionalFieldsInToString() throws Exception
    {
        final Decoder decoder = decodeHeartbeat(ENCODED_MESSAGE);

        assertThat(decoder.toString(), containsString(STRING_ENCODED_MESSAGE_EXAMPLE));
    }

    @Test
    public void shouldDecodeShorterStringsAfterLongerStrings() throws Exception
    {
        final Decoder decoder = decodeHeartbeat(DERIVED_FIELDS_MESSAGE);

        assertArrayEquals(ABC, getOnBehalfOfCompId(decoder));

        decode(SHORTER_STRING_MESSAGE, decoder);

        assertArrayEquals(AB, getOnBehalfOfCompId(decoder));
    }

    @Test
    public void shouldDecodeRepeatingGroups() throws Exception
    {
        final Decoder decoder = decodeHeartbeat(REPEATING_GROUP_MESSAGE);

        assertEquals(2, getNoEgGroupGroupCounter(decoder));

        Object group = getEgGroup(decoder);
        assertEquals(1, getGroupField(group));

        group = next(group);
        assertEquals(2, getGroupField(group));
        assertNull(next(group));

        // TODO: assertValid(decoder);
    }

    @Test
    public void shouldDecodeNestedRepeatingGroups() throws Exception
    {
        final Decoder decoder = decodeHeartbeat(NESTED_GROUP_MESSAGE);

        assertEquals(1, getNoEgGroupGroupCounter(decoder));

        final Object group = getEgGroup(decoder);
        assertEquals(1, getGroupField(group));
        assertNull(next(group));

        final Object nestedGroup = getNestedGroup(group);
        assertEquals(1, get(nestedGroup, "nestedField"));
        assertNull(next(nestedGroup));

        // TODO: assertValid(decoder);
    }

    @Test
    public void shouldToStringRepeatingGroups() throws Exception
    {
        final Decoder decoder = decodeHeartbeat(REPEATING_GROUP_MESSAGE);

        assertThat(decoder, hasToString(containsString(STRING_FOR_GROUP)));
    }

    @Test
    public void shouldDecodeComponents() throws Exception
    {
        final Decoder decoder = decodeHeartbeat(COMPONENT_MESSAGE);

        assertEquals(2, get(decoder, "componentField"));

        assertValid(decoder);
    }

    @Test
    public void shouldGenerateComponentToString() throws Exception
    {
        final Decoder decoder = decodeHeartbeat(COMPONENT_MESSAGE);

        assertThat(decoder.toString(), containsString("  \"ComponentField\": \"2\""));
    }

    @Test
    public void shouldGenerateComponentInterface() throws NoSuchMethodException
    {
        assertTrue("heartbeat doesn't implement its component", component.isAssignableFrom(heartbeat));

        assertHasComponentFieldGetter();
    }

    @Test
    public void shouldGenerateRequiredFieldsDictionary() throws Exception
    {
        final Object allFieldsField = heartbeat.getField(REQUIRED_FIELDS).get(null);
        assertThat(allFieldsField, instanceOf(IntHashSet.class));

        @SuppressWarnings("unchecked")
        final Set<Integer> allFields = (Set<Integer>) allFieldsField;
        assertThat(allFields, hasItem(116));
        assertThat(allFields, not(hasItem(112)));
        assertThat(allFields, not(hasItem(999)));
    }

    @Test
    public void shouldValidateMissingRequiredFields() throws Exception
    {
        final Decoder decoder = decodeHeartbeat(MISSING_REQUIRED_FIELDS_MESSAGE);

        assertFalse("Passed validation with missing fields", decoder.validate());
        assertEquals("Wrong tag id", 116, decoder.invalidTagId());
        assertEquals("Wrong reject reason", REQUIRED_TAG_MISSING, decoder.rejectReason());
    }

    @Test
    public void shouldValidateTagNumbers() throws Exception
    {
        final Decoder decoder = decodeHeartbeat(INVALID_TAG_NUMBER_MESSAGE);

        assertFalse("Passed validation with invalid tag number", decoder.validate());
        assertEquals("Wrong tag id", 9999, decoder.invalidTagId());
        assertEquals("Wrong reject reason", INVALID_TAG_NUMBER, decoder.rejectReason());
    }

    @Test
    public void shouldValidateTagNumbersDefinedForThisMessage() throws Exception
    {
        final Decoder decoder = decodeHeartbeat(TAG_NOT_DEFINED_FOR_THIS_MESSAGE_TYPE_MESSAGE);

        assertFalse("Passed validation with invalid tag number", decoder.validate());
        assertEquals("Wrong tag id", 99, decoder.invalidTagId());
        assertEquals("Wrong reject reason", TAG_NOT_DEFINED_FOR_THIS_MESSAGE_TYPE, decoder.rejectReason());
    }

    @Test
    public void shouldValidateTagSpecifiedWithMissingValue() throws Exception
    {
        final Decoder decoder = decodeHeartbeat(TAG_SPECIFIED_WITHOUT_A_VALUE_MESSAGE);

        assertFalse("Passed validation with missing value", decoder.validate());
        assertEquals("Wrong tag id", 116, decoder.invalidTagId());
        assertEquals("Wrong reject reason", TAG_SPECIFIED_WITHOUT_A_VALUE, decoder.rejectReason());
    }

    @Test
    public void shouldValidateIntBasedEnum() throws Exception
    {
        final Decoder decoder = decodeHeartbeat(TAG_SPECIFIED_WHERE_INT_VALUE_IS_INCORRECT_MESSAGE);

        assertFalse("Passed validation with incorrect value", decoder.validate());
        assertEquals("Wrong tag id", 116, decoder.invalidTagId());
        assertEquals("Wrong reject reason", VALUE_IS_INCORRECT, decoder.rejectReason());
    }

    @Test
    public void shouldValidateStringBasedEnum() throws Exception
    {
        final Decoder decoder = decodeHeartbeat(TAG_SPECIFIED_WHERE_STRING_VALUE_IS_INCORRECT_MESSAGE);

        assertFalse("Passed validation with incorrect value", decoder.validate());
        assertEquals("Wrong tag id", 115, decoder.invalidTagId());
        assertEquals("Wrong reject reason", VALUE_IS_INCORRECT, decoder.rejectReason());
    }

    @Test
    public void shouldValidateTagsAppearingMoreThanOnce() throws Exception
    {
        final Decoder decoder = decodeHeartbeat(TAG_APPEARS_MORE_THAN_ONCE_MESSAGE);

        assertFalse("Passed validation with incorrect value", decoder.validate());
        assertEquals("Wrong tag id", 116, decoder.invalidTagId());
        assertEquals("Wrong reject reason", TAG_APPEARS_MORE_THAN_ONCE, decoder.rejectReason());
    }

    @Test
    public void shouldResetTheInvalidAccessors() throws Exception
    {
        final Decoder decoder = decodeHeartbeat(TAG_APPEARS_MORE_THAN_ONCE_MESSAGE);
        decoder.validate();

        decoder.reset();

        assertEquals("Failed to reset tag id", NO_ERROR, decoder.invalidTagId());
        assertEquals("Failed to reset reject reason", NO_ERROR, decoder.rejectReason());
    }

    @Test
    public void shouldValidateFirstThreeFieldsAreInOrder() throws Exception
    {
        final Decoder decoder = decodeHeartbeat(TAG_SPECIFIED_OUT_OF_REQUIRED_ORDER_MESSAGE);

        assertFalse("Passed validation with incorrect value", decoder.validate());
        assertEquals("Wrong tag id", 9, decoder.invalidTagId());
        assertEquals("Wrong reject reason", TAG_SPECIFIED_OUT_OF_REQUIRED_ORDER, decoder.rejectReason());
    }

    // TODO: validation for data fields
    // TODO: validation for groups

    private void assertHasComponentFieldGetter() throws NoSuchMethodException
    {
        final Method method = component.getMethod("componentField");
        assertEquals(int.class, method.getReturnType());
    }

    private Object getNoEgGroupGroupCounter(final Decoder decoder) throws Exception
    {
        return get(decoder, "noEgGroupGroupCounter");
    }

    private int getGroupField(final Object group) throws Exception
    {
        return (int) get(group, "groupField");
    }

    private int getBodyLength(final Decoder header) throws Exception
    {
        return (int) get(header, BODY_LENGTH);
    }

    private Decoder getHeader(final Decoder decoder) throws Exception
    {
        return (Decoder) get(decoder, "header");
    }

    private Decoder decodeHeartbeat(final String example) throws Exception
    {
        final Decoder decoder = (Decoder) heartbeat.newInstance();
        decode(example, decoder);
        return decoder;
    }

    private void decode(final String example, final Decoder decoder)
    {
        buffer.putAscii(1, example);
        decoder.decode(buffer, 1, example.length());
    }

    private void assertIsDecoder(final Class<?> cls)
    {
        assertTrue("Isn't a decoder", Decoder.class.isAssignableFrom(cls));
    }

    private boolean hasTestReqId(final Object encoder) throws Exception
    {
        return (boolean) getField(encoder, HAS_TEST_REQ_ID);
    }

    private boolean hasDataField(Decoder decoder) throws Exception
    {
        return (boolean) getField(decoder, HAS_DATA_FIELD);
    }

    private boolean hasBooleanField(Decoder decoder) throws Exception
    {
        return (boolean) getField(decoder, HAS_BOOLEAN_FIELD);
    }

    private Object getFloatField(Decoder decoder) throws Exception
    {
        return get(decoder, FLOAT_FIELD);
    }

    private Object getIntField(Object decoder) throws Exception
    {
        return get(decoder, INT_FIELD);
    }

    private char[] getOnBehalfOfCompId(Decoder decoder) throws Exception
    {
        return getCharArray(decoder, ON_BEHALF_OF_COMP_ID);
    }

    private byte[] getDataField(Decoder decoder) throws Exception
    {
        return (byte[]) get(decoder, DATA_FIELD);
    }

    private Object getBooleanField(Decoder decoder) throws Exception
    {
        return get(decoder, BOOLEAN_FIELD);
    }

    private char[] getTestReqId(Decoder decoder) throws Exception
    {
        return getCharArray(decoder, TEST_REQ_ID);
    }

    private char[] getCharArray(final Decoder decoder, final String name) throws Exception
    {
        final char[] value = (char[]) get(decoder, name);
        final int length = (int) get(decoder, name + "Length");
        return Arrays.copyOf(value, length);
    }

    private void assertValid(final Decoder decoder)
    {
        final boolean isValid = decoder.validate();
        assertTrue(String.format(
            "Decoder fails validation due to: %s for tag: %d", decoder.rejectReason(), decoder.invalidTagId()),
            isValid);
    }
}
