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
import uk.co.real_logic.fix_gateway.dictionary.ir.*;
import uk.co.real_logic.fix_gateway.dictionary.ir.Field.Type;
import uk.co.real_logic.sbe.generation.java.JavaUtil;

import java.io.IOException;
import java.io.Writer;
import java.util.List;

import static java.util.stream.Collectors.joining;
import static uk.co.real_logic.fix_gateway.dictionary.generation.AggregateType.MESSAGE;
import static uk.co.real_logic.fix_gateway.dictionary.generation.GenerationUtil.fileHeader;

// TODO: optimisations
// skip decoding the msg type, since its known
// skip decoding the body string, since its known
// use ordering of fields to reduce branching
public class DecoderGenerator extends Generator
{
    public static String decoderClassName(final Aggregate aggregate)
    {
        return aggregate.name() + "Decoder";
    }

    private final int initialBufferSize;

    public DecoderGenerator(
        final DataDictionary dictionary,
        final int initialBufferSize,
        final String builderPackage,
        final OutputManager outputManager)
    {
        super(dictionary, builderPackage, outputManager);
        this.initialBufferSize = initialBufferSize;
    }

    protected void generateAggregate(final Aggregate aggregate, final AggregateType type)
    {
        final String className = decoderClassName(aggregate);
        final boolean isMessage = type == MESSAGE;

        try (final Writer out = outputManager.createOutput(className))
        {
            out.append(fileHeader(builderPackage));
            out.append(generateClassDeclaration(className, isMessage, Decoder.class));
            if (isMessage)
            {
                Message message = (Message) aggregate;
                out.append(generateMessageType(message.type()));
                out.append(commonCompoundImports("Decoder"));
            }
            generateGetters(out, className, aggregate.entries());
            out.append(generateDecodeMethod(aggregate.entries(), isMessage));
            out.append(generateResetMethod(aggregate.entries()));
            out.append(generateToString(aggregate));
            out.append("}\n");
        }
        catch (IOException e)
        {
            // TODO: logging
            e.printStackTrace();
        }
    }

    private String generateMessageType(final int type)
    {
        return String.format(
            "    public static final int MESSAGE_TYPE = %d;\n\n",
            type);
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
            "    private %s %s%s;\n\n" +
            "%s" +
            "    public %1$s %2$s()\n" +
            "    {\n" +
            "%s" +
            "        return %2$s;\n" +
            "    }\n\n" +
            "%s",
            javaTypeOf(field.type(), name),
            fieldName,
            fieldInitialisation(field.type(), name),
            optionalField(entry),
            optionalCheck(entry),
            optionalGetter(entry)
        );
    }

    private String fieldInitialisation(Type type, final String name)
    {
        switch (type)
        {
            case STRING:
                return String.format(" = new char[%d]", initialBufferSize);

            case DATA:
                return String.format(" = new byte[%d]", initialBufferSize);

            case QTY:
            case PRICE:
            case PRICEOFFSET:
                return " = new DecimalFloat()";

            case BOOLEAN:
            case INT:
            case LENGTH:
            case SEQNUM:
            case LOCALMKTDATE:
            case UTCTIMESTAMP:
                return "";

            default: throw new UnsupportedOperationException("Unknown type: " + type);
        }
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

    private String javaTypeOf(final Type type, final String name)
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
            "    public int decode(final AsciiFlyweight buffer, final int offset, final int length)\n" +
            "    {\n"+
            "        final int end = offset + length;\n" +
            "        int position = offset;\n" +
            (hasCommonCompounds ? "        position += header.decode(buffer, position, length);\n" : "") +
            "        int tag;\n\n" +
            "        while (position < end)\n" +
            "        {\n" +
            "            final int equalsPosition = buffer.scan(position, end, '=');\n" +
            "            tag = buffer.getInt(position, equalsPosition);\n" +
            "            final int valueOffset = equalsPosition + 1;\n" +
            "            final int endOfField = buffer.scan(valueOffset, end, START_OF_HEADER);\n" +
            "            final int valueLength = endOfField - valueOffset;\n" +
            "            switch (tag)\n" +
            "            {\n\n";

        final String body =
            entries.stream()
                   .map(this::decodeField)
                   .collect(joining("\n", "", "\n"));

        final String suffix =
            "            default:\n" +
            "                return position - offset;\n\n" +

            "            }\n\n" +
            "            position = endOfField + 1;\n" +
            "        }\n\n" +
            (hasCommonCompounds ? "        position += trailer.decode(buffer, position, end - position);\n" : "") +
            "        return position - offset;\n" +
            "    }\n\n";

        return prefix + body + suffix;
    }

    private String decodeField(final Entry entry)
    {
        // Uses variables from surrounding context:
        // int tag = the tag number of the field
        // int valueOffset = starting index of the value
        // int valueLength = the number of bytes for the value
        // int endOfField = the end index of the value

        final Field field = (Field) entry.element();
        final int tag = field.number();
        final String name = entry.name();
        final String fieldName = JavaUtil.formatPropertyName(name);

        return String.format(
            "            case %d:\n" +
            "%s" +
            "                %s = buffer.%s);\n" +
            "                break;\n",
            tag,
            optionalAssign(entry),
            fieldName,
            decodeMethod(field.type(), fieldName)
        );
    }

    private String optionalAssign(final Entry entry)
    {
        return entry.required() ? "" : String.format("                has%s = true;\n", entry.name());
    }

    private String decodeMethod(final Type type, String fieldName)
    {
        switch (type)
        {
            case STRING:
                return String.format("getChars(%s, valueOffset, valueLength", fieldName);

            case BOOLEAN:
                return "getBoolean(valueOffset";

            case DATA:
                return String.format("getBytes(%s, valueOffset, valueLength", fieldName);

            case INT:
            case LENGTH:
            case SEQNUM:
                return "getInt(valueOffset, endOfField";

            case QTY:
            case PRICE:
            case PRICEOFFSET:
                return String.format("getFloat(%s, valueOffset, valueLength", fieldName);

            case LOCALMKTDATE:
                return "getLocalMktDate(valueOffset, valueLength";

            case UTCTIMESTAMP:
                return "getUtcTimestamp(valueOffset, valueLength";

            default: throw new UnsupportedOperationException("Unknown type: " + type);
        }
    }

    protected String generateStringToString(String fieldName)
    {
        return String.format("new String(%s)", fieldName);
    }

}
