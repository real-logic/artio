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

import java.io.IOException;
import java.io.Writer;
import java.util.List;

import static java.util.stream.Collectors.joining;
import static uk.co.real_logic.fix_gateway.dictionary.generation.AggregateType.GROUP;
import static uk.co.real_logic.fix_gateway.dictionary.generation.AggregateType.MESSAGE;
import static uk.co.real_logic.fix_gateway.dictionary.generation.GenerationUtil.fileHeader;
import static uk.co.real_logic.fix_gateway.dictionary.ir.Field.Type.STRING;
import static uk.co.real_logic.sbe.generation.java.JavaUtil.formatPropertyName;

// TODO: optimisations
// skip decoding the msg type, since its known
// skip decoding the body string, since its known
// use ordering of fields to reduce branching
// skip decoding of unread header fields - eg: sender/target comp id.
// optimise the checksum definition to use an int and be calculated or ignored, have optional validation.
// evaluate utc parsing, adds about 100 nanos
public class DecoderGenerator extends Generator
{
    private static final double HASHSET_SIZE_FACTOR = 1.0 / 0.6;

    public static String decoderClassName(final Aggregate aggregate)
    {
        return decoderClassName(aggregate.name());
    }

    public static String decoderClassName(final String name)
    {
        return name + "Decoder";
    }

    private final int initialBufferSize;

    public DecoderGenerator(
        final Dictionary dictionary,
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

        outputManager.withOutput(className, out ->
        {
            out.append(fileHeader(builderPackage));
            out.append(generateClassDeclaration(className, type, Decoder.class, Decoder.class));
            if (isMessage)
            {
                final Message message = (Message)aggregate;
                out.append(generateMessageType(message.type()));
                out.append(commonCompoundImports("Decoder"));
            }
            generateGroupMethods(out, aggregate);
            generateGetters(out, className, aggregate.entries());
            out.append(generateDecodeMethod(aggregate.entries(), aggregate, type));
            out.append(generateResetMethods(isMessage, aggregate.entries()));
            out.append(generateToString(aggregate, isMessage));
            out.append("}\n");
        });
    }

    private void generateGroupMethods(final Writer out, final Aggregate aggregate) throws IOException
    {
        if (aggregate instanceof Group)
        {
            out.append(String.format(
                "    private %1$s next = null;\n\n" +
                "    public %1$s next()\n" +
                "    {\n" +
                "        return next;\n" +
                "    }\n\n" +
                "    private IntHashSet seenFields = new IntHashSet(%2$d, -1);\n\n",
                decoderClassName(aggregate),
                (int) Math.ceil(HASHSET_SIZE_FACTOR * aggregate.entries().size())
            ));
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
        for (final Entry entry : entries)
        {
            out.append(generateGetter(entry));
        }
    }

    private String generateGetter(final Entry entry) throws IOException
    {
        final Entry.Element element = entry.element();
        if (element instanceof Field)
        {
            return generateFieldGetter(entry, (Field) element);
        }
        else if (element instanceof Group)
        {
            return generateGroupGetter((Group) element);
        }
        else if (element instanceof Component)
        {
            return generateComponentField(decoderClassName(entry.name()), (Component) element);
        }

        return "";
    }

    private String generateGroupGetter(final Group group)
    {
        generateGroup(group);

        final Entry numberField = group.numberField();
        final String prefix = generateFieldGetter(numberField, (Field) numberField.element());

        return String.format(
            "    private %1$s %2$s = null;\n" +
            "    public %1$s %2$s()\n" +
            "    {\n" +
            "        return %2$s;" +
            "    }\n\n" +
            "%3$s",
            decoderClassName(group),
            formatPropertyName(group.name()),
            prefix
        );
    }

    private String generateFieldGetter(final Entry entry, final Field field)
    {
        final String name = field.name();
        final String fieldName = formatPropertyName(name);
        final Type type = field.type();
        final String optionalCheck = optionalCheck(entry);

        final String suffix = type.isStringBased()
            ? String.format(
                "    private int %s;\n\n" +
                "    public int %1$s()\n" +
                "    {\n" +
                "%s" +
                "        return %1$s;\n" +
                "    }\n",
                fieldName + "Length",
                optionalCheck
            )
            : "";

        return String.format(
            "    private %s %s%s;\n\n" +
                "%s" +
                "    public %1$s %2$s()\n" +
                "    {\n" +
                "%s" +
                "        return %2$s;\n" +
                "    }\n\n" +
                "%s\n" +
                "%s",
            javaTypeOf(type),
            fieldName,
            fieldInitialisation(type),
            optionalField(entry),
            optionalCheck,
            optionalGetter(entry),
            suffix
        );
    }

    private String fieldInitialisation(final Type type)
    {
        switch (type)
        {
            case STRING:
            case MULTIPLEVALUESTRING:
            case CURRENCY:
            case EXCHANGE:
            case COUNTRY:
            case UTCTIMEONLY:
            case UTCDATEONLY:
            case MONTHYEAR:
                return String.format(" = new char[%d]", initialBufferSize);

            case DATA:
                return String.format(" = new byte[%d]", initialBufferSize);

            case FLOAT:
            case PRICE:
            case PRICEOFFSET:
            case QTY:
            case PERCENTAGE:
            case AMT:
                return " = new DecimalFloat()";

            case BOOLEAN:
            case INT:
            case LENGTH:
            case SEQNUM:
            case NUMINGROUP:
            case DAYOFMONTH:
            case CHAR:
            case LOCALMKTDATE:
            case UTCTIMESTAMP:
                return "";

            default:
                throw new UnsupportedOperationException("Unknown type: " + type);
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

    private String javaTypeOf(final Type type)
    {
        switch (type)
        {
            case INT:
            case LENGTH:
            case SEQNUM:
            case NUMINGROUP:
            case DAYOFMONTH:
            case LOCALMKTDATE:
                return "int";

            case FLOAT:
            case PRICE:
            case PRICEOFFSET:
            case QTY:
            case PERCENTAGE:
            case AMT:
                return "DecimalFloat";

            case CHAR:
                return "char";

            case STRING:
            case MULTIPLEVALUESTRING:
            case CURRENCY:
            case EXCHANGE:
            case COUNTRY:
            case UTCDATEONLY:
            case UTCTIMEONLY:
            case MONTHYEAR:
                return "char[]";

            case BOOLEAN:
                return "boolean";

            case DATA:
                return "byte[]";

            case UTCTIMESTAMP:
                return "long";

            default:
                throw new UnsupportedOperationException("Unknown type: " + type);
        }
    }

    private String generateDecodeMethod(final List<Entry> entries, final Aggregate aggregate, final AggregateType type)
    {
        final boolean hasCommonCompounds = type == MESSAGE;
        final String endGroupCheck;
        if (type == GROUP)
        {
            endGroupCheck = String.format(
                "        if (!seenFields.add(tag))\n" +
                "        {\n" +
                "            if (next == null)\n" +
                "            {\n" +
                "                next = new %1$s();" +
                "            }\n" +
                "            return next.decode(buffer, position, end - position);\n" +
                "        }\n",
                decoderClassName(aggregate)
            );
        }
        else
        {
            endGroupCheck = "";
        }

        final String prefix =
            "    public int decode(final AsciiFlyweight buffer, final int offset, final int length)\n" +
            "    {\n" +
            "        final int end = offset + length;\n" +
            "        int position = offset;\n" +
            (hasCommonCompounds ? "        position += header.decode(buffer, position, length);\n" : "") +
            (type == GROUP ? "        seenFields.clear();\n" : "") +
            "        int tag;\n\n" +
            "        while (position < end)\n" +
            "        {\n" +
            "            final int equalsPosition = buffer.scan(position, end, '=');\n" +
            "            tag = buffer.getNatural(position, equalsPosition);\n" +
            endGroupCheck +
            "            final int valueOffset = equalsPosition + 1;\n" +
            "            final int endOfField = buffer.scan(valueOffset, end, START_OF_HEADER);\n" +
            "            final int valueLength = endOfField - valueOffset;\n" +
            "            switch (tag)\n" +
            "            {\n\n";

        final String body =
            entries.stream()
                   .map(this::decodeEntry)
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

    private String decodeEntry(final Entry entry)
    {
        // Uses variables from surrounding context:
        // int tag = the tag number of the field
        // int valueOffset = starting index of the value
        // int valueLength = the number of bytes for the value
        // int endOfField = the end index of the value

        if (entry.element() instanceof Field)
        {
            return decodeField(entry, "");
        }
        else if (entry.element() instanceof Group)
        {
            return decodeGroup(entry);
        }

        return "";
    }

    private String decodeGroup(final Entry entry)
    {
        final Group group = (Group) entry.element();

        final String parseGroup = String.format(
            "                if (%1$s == null)\n" +
            "                {\n" +
            "                    %1$s = new %2$s();\n" +
            "                }\n" +
            "                position = %1$s.decode(buffer, endOfField + 1, end - endOfField);\n",
            formatPropertyName(group.name()),
            decoderClassName(group)
        );
        return decodeField(group.numberField(), parseGroup);
    }

    private String decodeField(final Entry entry, final String suffix)
    {
        final Field field = (Field)entry.element();
        final int tag = field.number();
        final String name = entry.name();
        final String fieldName = formatPropertyName(name);

        return String.format(
            "            case %d:\n" +
            "%s" +
            "                %s = buffer.%s);\n" +
            "%s" +
            "%s" +
            "                break;\n",
            tag,
            optionalAssign(entry),
            fieldName,
            decodeMethod(field.type(), fieldName),
            optionalStringAssignment(field.type(), fieldName),
            suffix
        );
    }

    private String optionalStringAssignment(final Type type, final String fieldName)
    {
        return type == STRING
            ? String.format("                %sLength = valueLength;\n", fieldName)
            : "";
    }

    private String optionalAssign(final Entry entry)
    {
        return entry.required() ? "" : String.format("                has%s = true;\n", entry.name());
    }

    private String decodeMethod(final Type type, String fieldName)
    {
        switch (type)
        {
            case INT:
            case LENGTH:
            case SEQNUM:
            case NUMINGROUP:
            case DAYOFMONTH:
                return "getInt(valueOffset, endOfField";

            case FLOAT:
            case PRICE:
            case PRICEOFFSET:
            case QTY:
            case PERCENTAGE:
            case AMT:
                return String.format("getFloat(%s, valueOffset, valueLength", fieldName);

            case CHAR:
                return "getChar(valueOffset, endOfField";

            case STRING:
            case MULTIPLEVALUESTRING:
            case CURRENCY:
            case EXCHANGE:
            case COUNTRY:
            case UTCTIMEONLY:
            case UTCDATEONLY:
            case MONTHYEAR:
                return String.format("getChars(%s, valueOffset, valueLength", fieldName);

            case BOOLEAN:
                return "getBoolean(valueOffset";

            case DATA:
                return String.format("getBytes(%s, valueOffset, valueLength", fieldName);

            case LOCALMKTDATE:
                return "getLocalMktDate(valueOffset, valueLength";

            case UTCTIMESTAMP:
                return "getUtcTimestamp(valueOffset, valueLength";

            default:
                throw new UnsupportedOperationException("Unknown type: " + type);
        }
    }

    protected String generateStringToString(final String fieldName)
    {
        return String.format("new String(%s)", fieldName);
    }
}
