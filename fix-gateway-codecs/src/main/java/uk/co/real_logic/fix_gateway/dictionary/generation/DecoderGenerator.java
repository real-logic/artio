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

import uk.co.real_logic.agrona.generation.OutputManager;
import uk.co.real_logic.fix_gateway.builder.Decoder;
import uk.co.real_logic.fix_gateway.dictionary.ir.Aggregate;
import uk.co.real_logic.fix_gateway.dictionary.ir.DataDictionary;
import uk.co.real_logic.fix_gateway.dictionary.ir.Entry;

import java.io.IOException;
import java.io.Writer;
import java.util.List;

import static java.util.stream.Collectors.joining;
import static uk.co.real_logic.fix_gateway.dictionary.generation.AggregateType.MESSAGE;
import static uk.co.real_logic.fix_gateway.dictionary.generation.GenerationUtil.fileHeader;

public class DecoderGenerator extends Generator
{
    public DecoderGenerator(
        final DataDictionary dictionary,
        final String builderPackage,
        final OutputManager outputManager)
    {
        super(dictionary, builderPackage, outputManager);
    }

    protected void generateAggregate(final Aggregate aggregate, final AggregateType type)
    {
        final String className = aggregate.name() + "Decoder";
        final boolean hasCommonCompounds = type == MESSAGE;

        try (final Writer out = outputManager.createOutput(className))
        {
            out.append(fileHeader(builderPackage));
            out.append(generateClassDeclaration(className, hasCommonCompounds, Decoder.class));
            out.append(generateDecodeMethod(aggregate.entries(), hasCommonCompounds));
            out.append(generateResetMethod(aggregate.entries()));
            out.append("}\n");
        }
        catch (IOException e)
        {
            // TODO: logging
            e.printStackTrace();
        }
    }

    private String generateDecodeMethod(final List<Entry> entries, final boolean hasCommonCompounds)
    {
        final String prefix =
            "    public void decode(final MutableAsciiFlyweight buffer, final int offset, final int length)\n" +
            "    {\n"+
            "        int position = offset;\n\n";
            //(hasCommonCompounds ? "        position += header.decode(buffer, position);\n" : "");

        final String body =
            entries.stream()
                   .map(this::decodeField)
                   .collect(joining("\n"));

        final String suffix =
            //(hasCommonCompounds ? "        position += trailer.encode(buffer, position, header.bodyLength);\n" : "") +
            "    }\n\n";

        return prefix + body + suffix;
    }

    private String decodeField(final Entry entry)
    {
        return "";
    }

}
