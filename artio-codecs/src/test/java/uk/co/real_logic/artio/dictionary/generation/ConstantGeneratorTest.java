/*
 * Copyright 2015-2023 Real Logic Limited, Adaptive Financial Consulting Ltd.
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

import org.agrona.collections.IntHashSet;
import org.agrona.generation.StringWriterOutputManager;
import org.junit.BeforeClass;
import org.junit.Test;
import uk.co.real_logic.artio.util.Reflection;

import java.util.Map;
import java.util.Set;

import static org.agrona.generation.CompilerUtil.compileInMemory;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.assertEquals;
import static uk.co.real_logic.artio.dictionary.ExampleDictionary.*;
import static uk.co.real_logic.artio.util.Reflection.getField;

public class ConstantGeneratorTest
{
    private static final StringWriterOutputManager OUTPUT_MANAGER = new StringWriterOutputManager();
    private static final ConstantGenerator CONSTANT_GENERATOR = new ConstantGenerator(
        MESSAGE_EXAMPLE, TEST_PACKAGE, null, OUTPUT_MANAGER);

    private static Object constants;

    @BeforeClass
    public static void generate() throws Exception
    {
        CONSTANT_GENERATOR.generate();
        final Map<String, CharSequence> sources = OUTPUT_MANAGER.getSources();
        final Class<?> constantsClass = compileInMemory(TEST_PACKAGE + "." + ConstantGenerator.CLASS_NAME, sources);
        if (constantsClass == null)
        {
            System.out.println(sources);
        }
        else
        {
            constants = constantsClass.getConstructor().newInstance();
        }
    }

    @Test
    public void shouldContainNumericConstantsForFieldTags() throws Exception
    {
        assertEquals(TEST_REQ_ID_TAG, getField(constants, "TEST_REQ_ID"));
    }

    @Test
    public void shouldContainNumericConstantsForMessageTypes() throws Exception
    {
        assertEquals(HEARTBEAT_TYPE, getField(constants, "HEARTBEAT_MESSAGE"));
    }

    @Test
    public void shouldContainStringConstantsForMessageTypes() throws Exception
    {
        final String heartbeatString = String.valueOf((char)HEARTBEAT_TYPE);
        assertEquals(heartbeatString, getField(constants, "HEARTBEAT_MESSAGE_AS_STR"));
    }

    @Test
    public void shouldGenerateBeginString() throws Exception
    {
        final Object version = getField(constants, ConstantGenerator.VERSION);

        assertEquals("FIX.4.4", version);
    }

    @Test
    public void shouldGenerateAllFieldsSet() throws Exception
    {
        final Object allFieldsField = getField(constants, "ALL_FIELDS");
        assertThat(allFieldsField, instanceOf(IntHashSet.class));

        @SuppressWarnings("unchecked") final Set<Integer> allFields = (Set<Integer>)allFieldsField;
        assertThat(allFields, hasItem(123));
        assertThat(allFields, hasItem(124));
        assertThat(allFields, hasItem(35));
        assertThat(allFields, not(hasItem(999)));
    }

    @Test(expected = NoSuchFieldException.class)
    public void shouldNotGenerateUnnecessaryValuesOfMethods() throws Exception
    {
        Reflection.field(constants, "VALUES_OF_TestReqID");
    }
}
