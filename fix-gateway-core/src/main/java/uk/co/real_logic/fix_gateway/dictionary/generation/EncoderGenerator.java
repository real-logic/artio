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

import uk.co.real_logic.agrona.generation.StringWriterOutputManager;
import uk.co.real_logic.fix_gateway.dictionary.ir.DataDictionary;
import uk.co.real_logic.fix_gateway.dictionary.ir.Entry;
import uk.co.real_logic.fix_gateway.dictionary.ir.Field;
import uk.co.real_logic.fix_gateway.dictionary.ir.Message;
import uk.co.real_logic.sbe.generation.java.JavaUtil;

import java.io.IOException;
import java.io.Writer;
import java.util.List;

import static uk.co.real_logic.fix_gateway.dictionary.generation.GenerationUtil.BUILDER_PACKAGE;
import static uk.co.real_logic.fix_gateway.dictionary.generation.GenerationUtil.fileHeader;

public class EncoderGenerator
{
    private final DataDictionary dictionary;
    private final int initialArraySize;
    private final StringWriterOutputManager outputManager;

    public EncoderGenerator(
        final DataDictionary dictionary,
        final int initialArraySize,
        final StringWriterOutputManager outputManager)
    {
        this.dictionary = dictionary;
        this.initialArraySize = initialArraySize;
        this.outputManager = outputManager;
    }

    public void generate()
    {
        dictionary.messages()
                  .forEach(this::generateMessage);
    }

    private void generateMessage(final Message message)
    {

        final String className = message.name();

        try (final Writer out = outputManager.createOutput(className))
        {
            out.append(fileHeader(BUILDER_PACKAGE));
            out.append(generateClassDeclaration(className));
            generateSetters(out, className, message.entries());
            out.append(generateEncodeMethod());
            out.append("}\n");
        }
        catch (IOException e)
        {
            // TODO: logging
            e.printStackTrace();
        }

        message.category();
        message.type();
    }

    private void generateSetters(final Writer out, final String className, final List<Entry> entries) throws IOException
    {
        for (Entry entry : entries)
        {
            out.append(generateSetter(className, entry));
        }
    }

    private String generateSetter(final String className, final Entry entry)
    {
        final Field element = (Field) entry.element();
        final String name = element.name();
        final String fieldName = JavaUtil.formatPropertyName(name);

        String optionalField;
        String optionalAssign;

        if (entry.required())
        {
            optionalField = "";
            optionalAssign = "";
        }
        else
        {
            optionalField = String.format("    private boolean has%s;\n\n", name);
            optionalAssign = String.format("        has%s = true;\n", name);
        }

        switch (element.type())
        {
            // TODO: other string encoding cases: bytebuffer, etc.
            // TODO: other type cases
            // TODO: how do we reset optional fields - clear method?
            case STRING:

                return String.format(
                    "    private byte[] %s = new byte[%d];\n\n" +
                    "    private int %1$sLength = 0;\n\n" +
                    "%s" +
                    "    public %s %1$s(CharSequence value)\n" +
                    "    {\n" +
                    "        %1$s = toBytes(value, %1$s);\n" +
                    "        %1$sLength = value.length();\n" +
                    "%s" +
                    "        return this;\n" +
                    "    }\n" +
                    "\n" +
                    "    public %4$s %1$s(char[] value)\n" +
                    "    {\n" +
                    "        %1$s = toBytes(value, %1$s);\n" +
                    "        %1$sLength = value.length;\n" +
                    "%5$s" +
                    "        return this;\n" +
                    "    }\n\n",
                    fieldName,
                    initialArraySize,
                    optionalField,
                    className,
                    optionalAssign);
            default: throw new UnsupportedOperationException("Unknown type: " + element.type());
        }
    }

    private String generateEncodeMethod()
    {
        return String.format(
            "    public int encode(final MutableDirectBuffer buffer, final int offset)\n" +
            "    {\n" +
            "        return 0;\n" +
            "    }\n\n");
    }

    private String generateClassDeclaration(final String className)
    {
        return String.format(
            "import %s.Encoder;\n" +
            "import static uk.co.real_logic.fix_gateway.dictionary.generation.EncodingUtil.*;\n\n" +
            "import uk.co.real_logic.agrona.MutableDirectBuffer;\n\n" +
            "public final class %s implements Encoder\n" +
            "{\n\n",
            BUILDER_PACKAGE,
            className);
    }
}
