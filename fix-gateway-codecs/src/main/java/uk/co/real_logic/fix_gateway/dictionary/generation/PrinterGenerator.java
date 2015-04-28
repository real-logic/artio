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

import uk.co.real_logic.agrona.LangUtil;
import uk.co.real_logic.agrona.generation.OutputManager;
import uk.co.real_logic.fix_gateway.builder.Printer;
import uk.co.real_logic.fix_gateway.dictionary.ir.Aggregate;
import uk.co.real_logic.fix_gateway.dictionary.ir.DataDictionary;
import uk.co.real_logic.fix_gateway.dictionary.ir.Message;
import uk.co.real_logic.fix_gateway.util.AsciiFlyweight;
import uk.co.real_logic.sbe.generation.java.JavaUtil;

import java.io.IOException;
import java.io.Writer;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;
import static uk.co.real_logic.fix_gateway.dictionary.generation.DecoderGenerator.decoderClassName;
import static uk.co.real_logic.fix_gateway.dictionary.generation.GenerationUtil.fileHeader;
import static uk.co.real_logic.fix_gateway.dictionary.generation.GenerationUtil.importFor;

public class PrinterGenerator
{
    private static final String CLASS_NAME = "PrinterImpl";
    private static final String CLASS_DECLARATION =
        importFor(Printer.class) +
        importFor(AsciiFlyweight.class) +
        "\npublic class PrinterImpl implements Printer\n" +
        "{\n\n";

    private final DataDictionary dictionary;
    private final String builderPackage;
    private final OutputManager outputManager;

    public PrinterGenerator(
        final DataDictionary dictionary, final String builderPackage, final OutputManager outputManager)
    {
        this.dictionary = dictionary;
        this.builderPackage = builderPackage;
        this.outputManager = outputManager;
    }

    public void generate()
    {
        try (final Writer out = outputManager.createOutput(CLASS_NAME))
        {
            out.append(fileHeader(builderPackage));
            out.append(CLASS_DECLARATION);
            out.append(generateDecoderFields());
            out.append(generateToString());
            out.append("}\n");
        }
        catch (IOException e)
        {
            LangUtil.rethrowUnchecked(e);
        }
    }

    private String generateDecoderFields()
    {
        return messages()
              .map(this::generateDecoderField)
              .collect(joining()) + "\n";
    }

    private String generateDecoderField(final Aggregate aggregate)
    {
        return String.format(
            "    private final %s %s = new %1$s();\n",
            decoderClassName(aggregate),
            decoderFieldName(aggregate)
        );
    }

    private String decoderFieldName(final Aggregate aggregate)
    {
        return JavaUtil.formatPropertyName(aggregate.name());
    }

    private String generateToString()
    {
        final String cases =
            messages()
           .map(aggregate ->
           {
               return String.format(
                   "            case %s:\n" +
                   "            %s.decode(input, offset, length);\n" +
                   "            return %2$s.toString();\n\n",
                   aggregate.type(),
                   decoderFieldName(aggregate));
           })
           .collect(joining());

        return
            "    public String toString(\n" +
            "        final AsciiFlyweight input,\n" +
            "        final int offset,\n" +
            "        final int length,\n" +
            "        final int messageType)\n" +
            "    {\n" +
            "        switch(messageType)\n" +
            "        {\n" +
            cases +
            "            default:\n" +
            "            throw new IllegalArgumentException(\"Unknown Message Type: \" + messageType);" +
            "        }\n" +
            "    }\n\n";
    }

    private Stream<Message> messages()
    {
        return dictionary.messages().stream();
    }

}
