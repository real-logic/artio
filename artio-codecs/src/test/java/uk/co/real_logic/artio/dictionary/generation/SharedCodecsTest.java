/*
 * Copyright 2021 Adaptive Financial Consulting Ltd.
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

import org.agrona.generation.StringWriterOutputManager;
import org.junit.BeforeClass;
import org.junit.Test;
import uk.co.real_logic.artio.dictionary.ExampleDictionary;

import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.*;

import static org.agrona.generation.CompilerUtil.compileInMemory;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

public class SharedCodecsTest
{
    private static final String DICT_1 = "shared.dictionary.1";
    private static final String DICT_2 = "shared_dictionary_2";
    private static final String DICT_3 = "shared_dictionary.3";

    private static final String DICT_1_NORM = "shared_dictionary_1";
    private static final String DICT_2_NORM = "shared_dictionary_2";
    private static final String DICT_3_NORM = "shared_dictionary_3";

    private static final List<StringWriterOutputManager> OUTPUT_MANAGERS = new ArrayList<>();

    private static CodecConfiguration config;
    private static Class<?> executionReportEncoderShared;
    private static ClassLoader classLoader;
    private static Class<?> executionReportEncoder1;
    private static Class<?> executionReportEncoder2;
    private static Class<?> executionReportEncoder3;

    private static Class<?> headerEncoder1;
    private static Class<?> headerEncoder2;
    private static Class<?> headerEncoderShared;

    @BeforeClass
    public static void generate() throws Exception
    {
        config = new CodecConfiguration()
            .outputPath("ignored")
            .outputManagerFactory((outputPath, parentPackage) ->
            {
                final StringWriterOutputManager outputManager = new StringWriterOutputManager();
                outputManager.setPackageName(parentPackage);
                OUTPUT_MANAGERS.add(outputManager);
                return outputManager;
            })
            .sharedCodecsEnabled(DICT_1, DICT_2, DICT_3)
            .fileStreams(dictionaryStream(DICT_1), dictionaryStream(DICT_2), dictionaryStream(DICT_3));

        CodecGenerator.generate(config);

        final Map<String, CharSequence> sources = new HashMap<>();
        for (StringWriterOutputManager outputManager : OUTPUT_MANAGERS)
        {
            sources.putAll(outputManager.getSources());
        }

        if (AbstractDecoderGeneratorTest.CODEC_LOGGING)
        {
//            System.out.println(sources);
            System.out.println("sources.toString().length() = " + sources.toString().length());
        }

        final String nosEncoderName = executionReportEncoder(config, DICT_1_NORM);
        executionReportEncoder1 = compileInMemory(nosEncoderName, sources);
        classLoader = executionReportEncoder1.getClassLoader();
        executionReportEncoder2 = loadClass(executionReportEncoder(config, DICT_2_NORM));
        executionReportEncoder3 = loadClass(executionReportEncoder(config, DICT_3_NORM));
        executionReportEncoderShared = loadClass(executionReportEncoder(config, null));

        headerEncoder1 = loadClass(headerEncoder(config, DICT_1_NORM));
        headerEncoder2 = loadClass(headerEncoder(config, DICT_2_NORM));
        headerEncoderShared = loadClass(headerEncoder(config, null));
    }

    private static String executionReportEncoder(final CodecConfiguration config, final String dictNorm)
    {
        return encoder(config, dictNorm, "ExecutionReport");
    }

    private static String headerEncoder(final CodecConfiguration config, final String dictNorm)
    {
        return encoder(config, dictNorm, "Header");
    }

    private static String newOrderSingleEncoder(final CodecConfiguration config, final String dictNorm)
    {
        return encoder(config, dictNorm, "NewOrderSingle");
    }

    private static String encoder(final CodecConfiguration config, final String dictNorm, final String messageName)
    {
        final String packagePrefix = dictNorm == null ? "" : "." + dictNorm;
        return config.parentPackage() + packagePrefix + ".builder." + messageName + "Encoder";
    }

    private static InputStream dictionaryStream(final String dict)
    {
        return ExampleDictionary.class.getResourceAsStream(dict + ".xml");
    }

    @Test
    public void shouldGenerateClassStructure() throws ClassNotFoundException
    {
        assertNotNull(executionReportEncoder2);
        assertNotNull(executionReportEncoder3);
        assertNotNull(executionReportEncoderShared);

//        assertTrue(executionReportEncoderShared.isAssignableFrom(executionReportEncoder1));
//        assertTrue(executionReportEncoderShared.isAssignableFrom(executionReportEncoder2));
//        assertTrue(executionReportEncoderShared.isAssignableFrom(executionReportEncoder3));
    }

    @Test
    public void shouldMakeRequiredFieldOptionalIfOptionalInSomeDictionaries() throws Exception
    {
        // OrdStatus optional in dict 2

        // TODO: better test, need to encode and not throw an exception
    }

    @Test
    public void shouldSupportFieldMissingInSomeDictionaries() throws Exception
    {
        // ER.SecondaryOrderID on 1 but not others
        final String hasSecondaryOrderID = "hasSecondaryOrderID";
        executionReportEncoder1.getDeclaredMethod(hasSecondaryOrderID);
        noMethod(executionReportEncoder2, hasSecondaryOrderID);
        noMethod(executionReportEncoderShared, hasSecondaryOrderID);

        // OnBehalfOfCompID on header 1
        final String hasOnBehalfOfCompID = "hasOnBehalfOfCompID";
        headerEncoder1.getDeclaredMethod(hasOnBehalfOfCompID);
        noMethod(headerEncoder2, hasOnBehalfOfCompID);
        noMethod(headerEncoderShared, hasOnBehalfOfCompID);
    }

    @Test
    public void shouldSupportMessagesMissingInSomeDictionaries() throws ClassNotFoundException
    {
        // dict 2 doesn't have NOS but shared does
        noClass(newOrderSingleEncoder(config, null));
        loadClass(newOrderSingleEncoder(config, DICT_1_NORM));
        noClass(newOrderSingleEncoder(config, DICT_2_NORM));
    }

    private static Class<?> loadClass(final String name) throws ClassNotFoundException
    {
        return classLoader.loadClass(name);
    }

    private void noClass(final String name)
    {
        try
        {
            loadClass(name);
            fail("Managed to load " + name + " which shouldn't exist");
        }
        catch (final ClassNotFoundException | NullPointerException e)
        {
            // Deliberately blank
        }
    }

    private void noMethod(final Class<?> cls, final String name, final Class<?> ... paramTypes)
    {
        try
        {
            final Method method = cls.getDeclaredMethod(name, paramTypes);
            fail("Found method: " + method + " which shouldn't exist");
        }
        catch (final NoSuchMethodException e)
        {
            // Deliberately blank
        }
    }
}
