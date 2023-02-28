/*
 * Copyright 2015-2023 Real Logic Limited.
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

import org.agrona.generation.OutputManager;
import uk.co.real_logic.artio.builder.Printer;
import uk.co.real_logic.artio.dictionary.Generated;
import uk.co.real_logic.artio.dictionary.ir.Aggregate;
import uk.co.real_logic.artio.dictionary.ir.Dictionary;
import uk.co.real_logic.artio.dictionary.ir.Message;
import uk.co.real_logic.artio.util.AsciiBuffer;
import uk.co.real_logic.sbe.generation.java.JavaUtil;

import java.util.function.Function;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;
import static uk.co.real_logic.artio.dictionary.generation.DecoderGenerator.decoderClassName;
import static uk.co.real_logic.artio.dictionary.generation.GenerationUtil.GENERATED_ANNOTATION;
import static uk.co.real_logic.artio.dictionary.generation.GenerationUtil.fileHeader;
import static uk.co.real_logic.artio.dictionary.generation.GenerationUtil.importFor;

class PrinterGenerator
{
    private static final String CLASS_NAME = "PrinterImpl";
    private static final String CLASS_DECLARATION =
        importFor(Printer.class) +
        importFor(AsciiBuffer.class) +
        importFor(Generated.class) +
        "\n" +
        GENERATED_ANNOTATION +
        "public class PrinterImpl implements Printer\n" +
        "{\n\n";

    private final Dictionary dictionary;
    private final String builderPackage;
    private final OutputManager outputManager;

    PrinterGenerator(final Dictionary dictionary, final String builderPackage, final OutputManager outputManager)
    {
        this.dictionary = dictionary;
        this.builderPackage = builderPackage;
        this.outputManager = outputManager;
    }

    public void generate()
    {
        if (dictionary.shared())
        {
            return;
        }

        outputManager.withOutput(CLASS_NAME,
            (out) ->
            {
                out.append(fileHeader(builderPackage));
                out.append(CLASS_DECLARATION);
                out.append(generateDecoderFields());
                out.append(generateToString());
                out.append("}\n");
            });
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
        final Function<Message, String> mapper = (aggregate) -> String.format(
            "            if (messageType == %sL)\n" +
            "            {\n" +
            "                %s.decode(input, offset, length);\n" +
            "                return %2$s.toString();\n" +
            "            }\n\n",
            aggregate.packedType(),
            decoderFieldName(aggregate));

        final String cases = messages().map(mapper).collect(joining());

        return
            "    public String toString(\n" +
            "        final AsciiBuffer input,\n" +
            "        final int offset,\n" +
            "        final int length,\n" +
            "        final long messageType)\n" +
            "    {\n" +
            cases +
            "            else\n" +
            "            {\n" +
            "                throw new IllegalArgumentException(\"Unknown Message Type: \" + messageType);\n" +
            "            }\n" +
            "    }\n\n";
    }

    private Stream<Message> messages()
    {
        return dictionary.messages().stream();
    }
}
