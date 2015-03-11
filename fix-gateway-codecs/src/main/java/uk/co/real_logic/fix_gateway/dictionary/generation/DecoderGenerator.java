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
import uk.co.real_logic.fix_gateway.dictionary.ir.Field;
import uk.co.real_logic.fix_gateway.dictionary.ir.Field.Type;
import uk.co.real_logic.sbe.generation.java.JavaUtil;

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
            generateGetters(out, className, aggregate.entries());
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

    private void generateGetters(final Writer out, final String className, final List<Entry> entries) throws IOException
    {
        for (Entry entry : entries)
        {
            out.append(generateGetter(className, entry));
        }
    }

    private String generateGetter(final String className, final Entry entry) throws IOException
    {
        final Field field = (Field) entry.element();
        final String name = entry.name();
        final String fieldName = JavaUtil.formatPropertyName(name);

        return String.format(
            "    private %s %s;\n\n" +
            "%s" +
            "    public %1$s %2$s()\n" +
            "    {\n" +
            "%s" +
            "        return %2$s;\n" +
            "    }\n\n" +
            "%s",
            javaTypeOf(field.type()),
            fieldName,
            optionalField(entry),
            optionalCheck(entry),
            optionalGetter(entry)
        );
    }

    private String optionalGetter(final Entry entry)
    {
        return entry.required() ? "" : String.format(
            "    public boolean has%s()\n" +
            "    {\n" +
            "        return has%1$s;\n" +
            "    }\n\n",
            entry.name());
    }

    private String optionalCheck(final Entry entry)
    {
        return entry.required() ? "" : String.format(
            "        if (!has%s)\n" +
            "        {\n" +
            "            throw new IllegalArgumentException(\"No value for optional field: %1$s\");\n" +
            "        }\n\n",
            entry.name());
    }

    private String javaTypeOf(final Type type)
    {
        switch (type)
        {
            case STRING:
                return "char[]";

            case BOOLEAN:
                return "boolean";

            case DATA:
                return "byte[]";

            case INT:
            case LENGTH:
            case SEQNUM:
            case LOCALMKTDATE:
                return "int";

            case UTCTIMESTAMP:
                return "long";

            case QTY:
            case PRICE:
            case PRICEOFFSET:
                return "DecimalFloat";

            default: throw new UnsupportedOperationException("Unknown type: " + type);
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
