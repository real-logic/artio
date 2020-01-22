/*
 * Copyright 2015-2020 Real Logic Limited.
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
import uk.co.real_logic.artio.builder.Printer;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;

import static org.agrona.generation.CompilerUtil.compileInMemory;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static uk.co.real_logic.artio.dictionary.ExampleDictionary.*;

public class PrinterGeneratorTest
{
    private static StringWriterOutputManager outputManager = new StringWriterOutputManager();
    private static ConstantGenerator constantGenerator =
        new ConstantGenerator(MESSAGE_EXAMPLE, TEST_PACKAGE, outputManager);
    private static EnumGenerator enumGenerator =
        new EnumGenerator(MESSAGE_EXAMPLE, TEST_PACKAGE, outputManager);

    private static DecoderGenerator decoderGenerator =
        new DecoderGenerator(MESSAGE_EXAMPLE, 1, TEST_PACKAGE, TEST_PARENT_PACKAGE, outputManager, ValidationOn.class,
        RejectUnknownFieldOff.class, RejectUnknownEnumValueOn.class, false,
        Generator.RUNTIME_REJECT_UNKNOWN_ENUM_VALUE_PROPERTY);
    private static PrinterGenerator printerGenerator =
        new PrinterGenerator(MESSAGE_EXAMPLE, TEST_PACKAGE, outputManager);
    private static Class<?> printer;

    private MutableAsciiBuffer buffer = new MutableAsciiBuffer(new byte[8 * 1024]);

    @BeforeClass
    public static void generate() throws Exception
    {
        constantGenerator.generate();
        enumGenerator.generate();
        decoderGenerator.generate();
        printerGenerator.generate();
        final Map<String, CharSequence> sources = outputManager.getSources();
        printer = compileInMemory(PRINTER, sources);
        if (printer == null)
        {
            System.out.println(sources);
        }
    }

    @Test
    public void shouldPrettyPrintAMessage() throws Exception
    {
        final Printer printer = printer();
        buffer.putAscii(1, ENCODED_MESSAGE);

        final String string = printer.toString(buffer, 1, ENCODED_MESSAGE.length(), HEARTBEAT_TYPE);

        assertThat(string, containsString(STRING_ENCODED_MESSAGE_EXAMPLE));
    }

    private Printer printer()
        throws InstantiationException, IllegalAccessException, NoSuchMethodException, InvocationTargetException
    {
        return (Printer)PrinterGeneratorTest.printer.getConstructor().newInstance();
    }
}
