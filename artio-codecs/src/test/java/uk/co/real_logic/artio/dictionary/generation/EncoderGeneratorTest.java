/*
 * Copyright 2015-2017 Real Logic Ltd.
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
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import uk.co.real_logic.artio.EncodingException;
import uk.co.real_logic.artio.builder.Encoder;
import uk.co.real_logic.artio.fields.DecimalFloat;
import uk.co.real_logic.artio.fields.UtcTimestampEncoder;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;
import uk.co.real_logic.artio.util.Reflection;

import java.util.Map;

import static java.lang.reflect.Modifier.isAbstract;
import static java.lang.reflect.Modifier.isPublic;
import static org.agrona.generation.CompilerUtil.compileInMemory;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static uk.co.real_logic.artio.dictionary.ExampleDictionary.*;
import static uk.co.real_logic.artio.dictionary.generation.GenerationUtil.PARENT_PACKAGE;
import static uk.co.real_logic.artio.util.Reflection.*;

public class EncoderGeneratorTest
{
    private static Map<String, CharSequence> sources;
    private static Class<?> heartbeat;
    private static Class<?> otherMessage;
    private static Class<?> heartbeatWithoutValidation;

    private MutableAsciiBuffer buffer = new MutableAsciiBuffer(new byte[8 * 1024]);

    @BeforeClass
    public static void generate() throws Exception
    {
        sources = generateSources(true);
        heartbeat = compileInMemory(HEARTBEAT_ENCODER, sources);
        if (heartbeat == null)
        {
            System.out.println(sources);
        }
        otherMessage = compileInMemory(OTHER_MESSAGE_ENCODER, sources);

        final Map<String, CharSequence> sourcesWithoutValidation = generateSources(false);
        heartbeatWithoutValidation = compileInMemory(HEARTBEAT_ENCODER, sourcesWithoutValidation);
    }

    private static Map<String, CharSequence> generateSources(final boolean validation)
    {
        final Class<?> validationClass = validation ? ValidationOn.class : ValidationOff.class;
        final Class<?> rejectUnknownField = RejectUnknownFieldOff.class;
        final StringWriterOutputManager outputManager = new StringWriterOutputManager();
        final EnumGenerator enumGenerator = new EnumGenerator(MESSAGE_EXAMPLE, TEST_PARENT_PACKAGE, outputManager);
        final EncoderGenerator encoderGenerator =
            new EncoderGenerator(MESSAGE_EXAMPLE, 1, TEST_PACKAGE, TEST_PARENT_PACKAGE, outputManager, validationClass,
            rejectUnknownField);
        enumGenerator.generate();
        encoderGenerator.generate();
        return outputManager.getSources();
    }

    @Test
    public void generatesEncoderClass()
    {
        assertNotNull("Not generated anything", heartbeat);
        assertNotNull(heartbeat);
        assertTrue(Encoder.class.isAssignableFrom(heartbeat));

        final int modifiers = heartbeat.getModifiers();
        assertFalse("Not instantiable", isAbstract(modifiers));
        assertTrue("Not public", isPublic(modifiers));
    }

    @Test
    public void generatesSetters() throws Exception
    {
        heartbeat.getMethod(ON_BEHALF_OF_COMP_ID, CharSequence.class);
        final Class<?> stringEnumClass = heartbeat.getClassLoader().loadClass(PARENT_PACKAGE + ".OnBehalfOfCompID");
        heartbeat.getMethod(ON_BEHALF_OF_COMP_ID, stringEnumClass);
    }

    @Test
    public void stringSettersWriteToFields() throws Exception
    {
        final Object encoder = heartbeat.getConstructor().newInstance();

        setTestReqId(encoder);

        assertTestReqIsValue(encoder);
    }

    @Test
    public void stringSettersResizeByteArray() throws Exception
    {
        final Encoder encoder = newHeartbeat();

        setTestReqIdTo(encoder, "abcd");

        assertArrayEquals(LONG_VALUE_IN_BYTES, getTestReqIdBytes(encoder));
        assertTestReqIdLength(4, encoder);
        assertTestReqIdOffset(0, encoder);
    }

    @Test
    public void byteArraySettersWriteToFields() throws Exception
    {
        final Encoder encoder = newHeartbeat();

        heartbeat
            .getMethod(TEST_REQ_ID, byte[].class)
            .invoke(encoder, VALUE_IN_BYTES);

        assertTestReqIsValue(encoder);

        assertEncodesTestReqIdFully(encoder);
    }

    @Test
    public void offsetAndLengthbyteArraySettersWriteFields() throws Exception
    {
        final Encoder encoder = newHeartbeat();

        setTestReqIdBytes(encoder, 1, 3);

        assertArrayEquals(PREFIXED_VALUE_IN_BYTES, getTestReqIdBytes(encoder));
        assertTestReqIdOffset(1, encoder);
        assertTestReqIdLength(3, encoder);

        assertEncodesTestReqIdFully(encoder);
    }

    private void setTestReqIdBytes(
        final Object encoder, final int offset, final int length) throws Exception
    {
        heartbeat
            .getMethod(TEST_REQ_ID, byte[].class, int.class, int.class)
            .invoke(encoder, PREFIXED_VALUE_IN_BYTES, offset, length);
    }

    @Test
    public void charArraySettersWriteToFields() throws Exception
    {
        final Object encoder = heartbeat.getConstructor().newInstance();

        final Object value = new char[]{ 'a', 'b', 'c' };
        heartbeat.getMethod(TEST_REQ_ID, char[].class)
            .invoke(encoder, value);

        assertTestReqIsValue(encoder);
    }

    @Test
    public void stringSettersByEnumToFields() throws Exception
    {
        final Object encoder = heartbeat.getConstructor().newInstance();

        setEnumByRepresentation(encoder,
            ON_BEHALF_OF_COMP_ID,
            PARENT_PACKAGE + ".OnBehalfOfCompID",
            "abc");
        assertOnBehalfOfCompIDValue(encoder, "abc");
    }

    @Test
    public void intSettersWriteToFields() throws Exception
    {
        final Object encoder = heartbeat.getConstructor().newInstance();

        setInt(encoder, INT_FIELD, 1);

        assertEquals(1, getField(encoder, INT_FIELD));
    }

    @Test
    public void intSettersByEnumWriteToFields() throws Exception
    {
        final Object encoder = heartbeat.getConstructor().newInstance();
        setEnumByRepresentation(encoder, INT_FIELD, PARENT_PACKAGE + ".IntField", 1);

        assertEquals(1, getField(encoder, INT_FIELD));
    }

    @Test
    public void floatSettersWriteToFields() throws Exception
    {
        final Object encoder = heartbeat.getConstructor().newInstance();

        final DecimalFloat value = new DecimalFloat(1, 2);

        setFloat(encoder, FLOAT_FIELD, value);

        Assert.assertEquals(value, getField(encoder, FLOAT_FIELD));
    }

    @Test
    public void flagsForOptionalFieldsInitiallyUnset() throws Exception
    {
        final Object encoder = heartbeat.getConstructor().newInstance();
        assertFalse("hasTestReqId initially true", hasTestReqId(encoder));
    }

    @Test
    public void flagsForOptionalFieldsUpdated() throws Exception
    {
        final Object encoder = heartbeat.getConstructor().newInstance();

        setTestReqId(encoder);

        assertTrue("hasTestReqId not updated", hasTestReqId(encoder));
    }

    @Test
    public void encodesValues() throws Exception
    {
        final Encoder encoder = newHeartbeat();

        setRequiredFields(encoder);
        setupHeader(encoder);
        setupTrailer(encoder);

        setOptionalFields(encoder);
        assertEncodesTo(encoder, ENCODED_MESSAGE);
    }

    @Test
    public void ignoresMissingOptionalValues() throws Exception
    {
        final Encoder encoder = newHeartbeat();

        setRequiredFields(encoder);
        setupHeader(encoder);
        setupTrailer(encoder);

        assertEncodesTo(encoder, NO_OPTIONAL_MESSAGE);
    }

    @Test
    public void automaticallyComputesDerivedHeaderAndTrailerFields() throws Exception
    {
        final Encoder encoder = newHeartbeat();

        setRequiredFields(encoder);

        assertEncodesTo(encoder, DERIVED_FIELDS_MESSAGE);
    }

    @Test
    public void shouldGenerateHumanReadableToString() throws Exception
    {
        final Encoder encoder = newHeartbeat();

        setRequiredFields(encoder);

        assertThat(encoder.toString(), containsString(STRING_NO_OPTIONAL_MESSAGE_SUFFIX));
    }

    @Test
    public void shouldIncludeOptionalFieldsInToString() throws Exception
    {
        final Encoder encoder = newHeartbeat();

        setRequiredFields(encoder);
        setOptionalFields(encoder);

        assertThat(encoder.toString(), containsString(STRING_ENCODED_MESSAGE_SUFFIX));
    }

    @Test
    public void shouldEncodeShorterStringsAfterLongerStrings() throws Exception
    {
        final Encoder encoder = newHeartbeat();

        setRequiredFields(encoder);

        encoder.encode(buffer, 1);

        setCharSequence(encoder, ON_BEHALF_OF_COMP_ID, "ab");

        assertEncodesTo(encoder, SHORTER_STRING_MESSAGE);
    }

    @Test
    public void shouldToStringShorterStringsAfterLongerStrings() throws Exception
    {
        final Encoder encoder = newHeartbeat();

        setRequiredFields(encoder);

        setCharSequence(encoder, ON_BEHALF_OF_COMP_ID, "ab");

        assertThat(encoder.toString(), containsString("ab"));
        assertThat(encoder.toString(), not(containsString("abc")));
    }

    @Test
    public void shouldEncodeGroups() throws Exception
    {
        final Encoder encoder = newHeartbeat();
        setRequiredFields(encoder);
        setEgGroupToTwoElements(encoder);

        assertEncodesTo(encoder, REPEATING_GROUP_MESSAGE);
    }

    @Test
    public void shouldEncodeNestedGroups() throws Exception
    {
        final Encoder encoder = newHeartbeat();
        setRequiredFields(encoder);

        final Object group = getEgGroup(encoder, 1);
        setGroupField(group, 1);

        setNestedField(group);

        assertEncodesTo(encoder, NESTED_GROUP_MESSAGE);
    }

    @Test
    public void shouldResetOptionalFields() throws Exception
    {
        final Encoder encoder = newHeartbeat();
        setOptionalFields(encoder);

        reset(encoder);

        setRequiredFields(encoder);

        assertEncodesTo(encoder, NO_OPTIONAL_MESSAGE);
    }

    @Test(expected = EncodingException.class)
    public void shouldResetFlagForMissingRequiredIntFields() throws Exception
    {
        final Encoder encoder = newHeartbeat();

        setRequiredFields(encoder);

        encoder.reset();

        setOnBehalfOfCompID(encoder);
        setFloatField(encoder);
        setSomeTimeField(encoder, 1);

        encoder.encode(buffer, 1);
    }

    @Test
    public void shouldResetRequiredFields() throws Exception
    {
        final Encoder encoder = newHeartbeat();
        setRequiredFields(encoder);

        reset(encoder);

        assertFalse(hasOnBehalfOfCompID(encoder));
    }

    @Test
    public void shouldResetComponents() throws Exception
    {
        final Encoder encoder = newHeartbeat();
        setupComponent(encoder);

        reset(encoder);

        setRequiredFields(encoder);
        assertEncodesTo(encoder, NO_OPTIONAL_MESSAGE);
    }

    @Test
    public void shouldResetGroups() throws Exception
    {
        final Encoder encoder = newHeartbeat();

        final Object group = getEgGroup(encoder, 1);
        setGroupField(group, 1);
        setNestedField(group);

        reset(encoder);

        setRequiredFields(encoder);
        assertEncodesTo(encoder, NO_OPTIONAL_MESSAGE);
    }

    @Test
    public void shouldEncodeGroupsOfSizeZero() throws Exception
    {
        final Encoder encoder = newHeartbeat();

        setRequiredFields(encoder);

        final Object group = getEgGroup(encoder, 1);
        setGroupField(group, 1);
        setNestedField(group);

        getEgGroup(encoder, 0);

        assertEncodesTo(encoder, ZERO_REPEATING_GROUP_MESSAGE);
    }

    @Test
    public void shouldEncodeGroupsAfterReset() throws Exception
    {
        final Encoder encoder = newHeartbeat();

        setRequiredFields(encoder);
        setEgGroupToTwoElements(encoder);

        reset(encoder);

        setRequiredFields(encoder);
        setEgGroupToTwoElements(encoder);

        assertEncodesTo(encoder, REPEATING_GROUP_MESSAGE);
    }

    @Test
    public void shouldEncodeShorterGroups() throws Exception
    {
        final Encoder encoder = newHeartbeat();

        setRequiredFields(encoder);
        setEgGroupToTwoElements(encoder);

        setRequiredFields(encoder);
        setEgGroupToOneElement(encoder);

        assertEncodesTo(encoder, SINGLE_REPEATING_GROUP_MESSAGE);
    }

    @Test
    public void shouldEncodeShorterGroupsAfterReset() throws Exception
    {
        final Encoder encoder = newHeartbeat();

        setRequiredFields(encoder);
        setEgGroupToTwoElements(encoder);

        reset(encoder);

        setRequiredFields(encoder);
        setEgGroupToOneElement(encoder);

        assertEncodesTo(encoder, SINGLE_REPEATING_GROUP_MESSAGE);
    }

    @Test
    public void shouldGenerateToStringForShorterGroups() throws Exception
    {
        final Encoder encoder = newHeartbeat();
        setEgGroupToTwoElements(encoder);

        setRequiredFields(encoder);
        setEgGroupToOneElement(encoder);

        assertThat(encoder, hasToString(containsString(STRING_GROUP_ONE_ELEMENT)));
    }

    @Test
    public void shouldGenerateToStringForShorterGroupsAfterReset() throws Exception
    {
        final Encoder encoder = newHeartbeat();

        setRequiredFields(encoder);
        setEgGroupToTwoElements(encoder);

        reset(encoder);

        setRequiredFields(encoder);
        setEgGroupToOneElement(encoder);

        assertThat(encoder, hasToString(containsString(STRING_GROUP_ONE_ELEMENT)));
    }

    @Test
    public void shouldIgnoreUnnecessaryGroupNextCalls() throws Exception
    {
        final Encoder encoder = newHeartbeat();

        setRequiredFields(encoder);

        Object egGroup = setEgGroupToOneElement(encoder);

        egGroup = next(egGroup);
        egGroup = next(egGroup);
        next(egGroup);

        assertEncodesTo(encoder, SINGLE_REPEATING_GROUP_MESSAGE);
    }

    @Test(expected = EncodingException.class)
    public void shouldValidateMissingRequiredStringFields() throws Exception
    {
        final Encoder encoder = newHeartbeat();

        setFloatField(encoder);
        setSomeTimeField(encoder, 0);

        encoder.encode(buffer, 1);
    }

    @Test(expected = EncodingException.class)
    public void shouldValidateMissingRequiredFloatFields() throws Exception
    {
        final Encoder encoder = newHeartbeat();

        setOnBehalfOfCompID(encoder);
        setSomeTimeField(encoder, 0);

        encoder.encode(buffer, 1);
    }

    @Test(expected = EncodingException.class)
    public void shouldValidateMissingRequiredIntFields() throws Exception
    {
        final Encoder encoder = newHeartbeat();

        setOnBehalfOfCompID(encoder);
        setFloatField(encoder);
        setSomeTimeField(encoder, 1);

        encoder.encode(buffer, 1);
    }

    @Test(expected = EncodingException.class)
    public void shouldValidateMissingRequiredTemporalFields() throws Exception
    {
        final Encoder encoder = newHeartbeat();

        setOnBehalfOfCompID(encoder);
        setFloatField(encoder);

        encoder.encode(buffer, 1);
    }

    @Test
    public void canDisableRequiredStringFieldValidation() throws Exception
    {
        final Encoder encoder = (Encoder)heartbeatWithoutValidation.getConstructor().newInstance();

        setFloatField(encoder);
        setSomeTimeField(encoder, 0);

        encoder.encode(buffer, 1);
    }

    @Test
    public void canDisableRequiredFloatFieldValidation() throws Exception
    {
        final Encoder encoder = (Encoder)heartbeatWithoutValidation.getConstructor().newInstance();

        setOnBehalfOfCompID(encoder);
        setSomeTimeField(encoder, 0);

        encoder.encode(buffer, 1);
    }

    @Test
    public void canDisableRequiredTemporalFieldValidation() throws Exception
    {
        final Encoder encoder = (Encoder)heartbeatWithoutValidation.getConstructor().newInstance();

        setOnBehalfOfCompID(encoder);
        setFloatField(encoder);

        encoder.encode(buffer, 1);
    }

    @Test
    public void shouldEncodeShortTimestamp() throws Exception
    {
        final Encoder encoder = newHeartbeat();

        setRequiredFields(encoder, 0);

        assertEncodesTo(encoder, SHORT_TIMESTAMP_MESSAGE);
    }

    @Test
    public void shouldGenerateToStringForGroups() throws Exception
    {
        final Encoder encoder = newHeartbeat();
        setRequiredFields(encoder);
        setEgGroupToTwoElements(encoder);

        assertThat(encoder, hasToString(containsString(STRING_GROUP_TWO_ELEMENTS)));
    }

    @Test
    public void shouldGenerateComponentClass() throws Exception
    {
        final Class<?> component = compileInMemory(COMPONENT_ENCODER, sources);

        assertNotNull(component);
    }

    @Test
    public void shouldEncodeComponents() throws Exception
    {
        final Encoder encoder = newHeartbeat();
        setRequiredFields(encoder);

        setupComponent(encoder);

        assertEncodesTo(encoder, COMPONENT_MESSAGE);
    }

    @Test
    public void shouldBeAbleToToStringComponentValues() throws Exception
    {
        final Encoder encoder = newHeartbeat();
        setRequiredFields(encoder);

        setupComponent(encoder);

        assertThat(encoder.toString(), containsString(COMPONENT_TO_STRING));
    }

    @Test
    public void shouldGenerateHasMethodsForFields() throws Exception
    {
        final Encoder encoder = newHeartbeat();
        setRequiredFields(encoder);

        assertTrue(hasOnBehalfOfCompID(encoder));
        assertFalse(hasTestReqID(encoder));
    }

    @Test
    public void shouldGenerateTwoCharacterMessageTypes() throws Exception
    {
        final Encoder encoder = (Encoder)otherMessage.getConstructor().newInstance();

        assertThat(encoder.toString(), containsString("\"MsgType\": \"" + OTHER_MESSAGE_TYPE + "\""));
        assertEncodesTo(encoder, "8=FIX.4.4\0019=6\00135=AB\00110=247\001");
    }

    private void setNestedField(final Object group) throws Exception
    {
        final Object nestedGroup = getNestedGroup(group, 1);
        setInt(nestedGroup, "nestedField", 1);
    }

    private boolean hasOnBehalfOfCompID(final Encoder encoder) throws Exception
    {
        return (boolean)get(encoder, "hasOnBehalfOfCompID");
    }

    private boolean hasTestReqID(final Encoder encoder) throws Exception
    {
        return (boolean)get(encoder, "hasTestReqID");
    }

    private void setupComponent(final Encoder encoder) throws Exception
    {
        final Object egComponent = getEgComponent(encoder);
        setInt(egComponent, COMPONENT_FIELD, 2);

        Object componentGroup = getComponentGroup(egComponent, 2);
        setComponentGroupField(componentGroup, 1);

        componentGroup = next(componentGroup);
        setComponentGroupField(componentGroup, 2);
    }

    private void setEgGroupToTwoElements(final Encoder encoder) throws Exception
    {
        Object egGroup = getEgGroup(encoder, 2);
        setGroupField(egGroup, 1);

        egGroup = next(egGroup);
        setGroupField(egGroup, 2);
    }

    private Object setEgGroupToOneElement(final Encoder encoder) throws Exception
    {
        final Object egGroup = getEgGroup(encoder, 1);
        setGroupField(egGroup, 2);
        return egGroup;
    }


    private void setGroupField(final Object group, final int value) throws Exception
    {
        setInt(group, "groupField", value);
    }

    private void setComponentGroupField(final Object group, final int value) throws Exception
    {
        setInt(group, "componentGroupField", value);
    }

    private void setupHeader(final Encoder encoder) throws Exception
    {
        final Object header = Reflection.get(encoder, "header");
        setCharSequence(header, "beginString", "FIX.4.4");
        setCharSequence(header, MSG_TYPE, "0");
    }

    private void setRequiredFields(final Encoder encoder) throws Exception
    {
        setRequiredFields(encoder, 1);
    }

    private void setRequiredFields(final Encoder encoder, final int someTime) throws Exception
    {
        setOnBehalfOfCompID(encoder);
        setIntField(encoder);
        setFloatField(encoder);
        setSomeTimeField(encoder, someTime);
    }

    private void setOnBehalfOfCompID(final Encoder encoder) throws Exception
    {
        setCharSequence(encoder, ON_BEHALF_OF_COMP_ID, "abc");
    }

    private void setFloatField(final Encoder encoder) throws Exception
    {
        setFloat(encoder, FLOAT_FIELD, new DecimalFloat(11, 1));
    }

    private void setIntField(final Encoder encoder) throws Exception
    {
        setInt(encoder, INT_FIELD, 2);
    }

    private void setSomeTimeField(final Encoder encoder, final int someTime) throws Exception
    {
        final UtcTimestampEncoder utcTimestampEncoder = new UtcTimestampEncoder();
        final int length = utcTimestampEncoder.encode(someTime);
        encoder.getClass()
            .getMethod(SOME_TIME_FIELD, byte[].class, int.class)
            .invoke(encoder, utcTimestampEncoder.buffer(), length);
    }

    private void setOptionalFields(final Encoder encoder) throws Exception
    {
        setTestReqIdTo(encoder, ABC);
        setBoolean(encoder, BOOLEAN_FIELD, true);
        setByteArray(encoder, DATA_FIELD, new byte[]{ '1', '2', '3' });
    }

    private void setupTrailer(final Encoder encoder) throws Exception
    {
        final Object trailer = Reflection.get(encoder, "trailer");
        setCharSequence(trailer, "checkSum", "12");
    }

    private void assertEncodesTo(final Encoder encoder, final String expectedValue)
    {
        final long result = encoder.encode(buffer, 1);
        final int length = Encoder.length(result);
        final int offset = Encoder.offset(result);
        assertEquals(expectedValue, buffer.getAscii(offset, expectedValue.length()));
        assertEquals(expectedValue.length(), length);
    }

    private void assertTestReqIsValue(final Object encoder) throws Exception
    {
        assertArrayEquals(VALUE_IN_BYTES, getTestReqIdBytes(encoder));
        assertTestReqIdOffset(0, encoder);
        assertTestReqIdLength(3, encoder);
    }

    private byte[] getTestReqIdBytes(final Object encoder) throws Exception
    {
        return (byte[])getField(encoder, TEST_REQ_ID);
    }

    private void assertOnBehalfOfCompIDValue(final Object encoder, final String value) throws Exception
    {
        assertArrayEquals(value.getBytes(), (byte[])getField(encoder, ON_BEHALF_OF_COMP_ID));
        assertEquals(value.length(), getField(encoder, ON_BEHALF_OF_COMP_ID_LENGTH));
    }


    private boolean hasTestReqId(final Object encoder) throws Exception
    {
        return (boolean)get(encoder, HAS_TEST_REQ_ID);
    }

    private void setTestReqId(final Object encoder) throws Exception
    {
        setTestReqIdTo(encoder, ABC);
    }

    private void setTestReqIdTo(final Object encoder, final String value) throws Exception
    {
        setCharSequence(encoder, TEST_REQ_ID, value);
    }

    private Encoder newHeartbeat() throws Exception
    {
        return (Encoder)heartbeat.getConstructor().newInstance();
    }

    private void assertTestReqIdLength(final int expectedLength, final Object encoder) throws Exception
    {
        assertEquals(expectedLength, getField(encoder, TEST_REQ_ID_LENGTH));
    }

    private void assertTestReqIdOffset(final int expectedoffset, final Object encoder) throws Exception
    {
        assertEquals(expectedoffset, getField(encoder, TEST_REQ_ID_OFFSET));
    }

    private void assertEncodesTestReqIdFully(final Encoder encoder) throws Exception
    {
        setRequiredFields(encoder);
        assertThat(encoder.toString(), containsString(STRING_ONLY_TESTREQ_MESSAGE_SUFFIX));
        assertEncodesTo(encoder, ONLY_TESTREQ_ENCODED_MESSAGE);
    }
}
