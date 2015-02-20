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
import uk.co.real_logic.fix_gateway.dictionary.ir.Message;

import java.io.IOException;
import java.io.Writer;

import static uk.co.real_logic.fix_gateway.dictionary.generation.GenerationUtil.BUILDER_PACKAGE;
import static uk.co.real_logic.fix_gateway.dictionary.generation.GenerationUtil.fileHeader;

public class EncoderGenerator
{
    private final DataDictionary dictionary;
    private final StringWriterOutputManager outputManager;

    public EncoderGenerator(final DataDictionary dictionary, final StringWriterOutputManager outputManager)
    {
        this.dictionary = dictionary;
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

    private String generateEncodeMethod()
    {
        return String.format(
            "    public int encode(final MutableDirectBuffer buffer, final int offset)\n" +
            "    {\n" +
            "        return 0;" +
            "    }\n\n");
    }

    private String generateClassDeclaration(final String className)
    {
        return String.format(
            "import %s.Encoder;\n" +
            "import uk.co.real_logic.agrona.MutableDirectBuffer;\n\n" +
            "public class %s implements Encoder\n" +
            "{\n\n",
            BUILDER_PACKAGE,
            className);
    }
}
