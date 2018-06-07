/*
 * Copyright 2015-2017 Real Logic Ltd.
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
package uk.co.real_logic.artio.dictionary.generation;

import org.agrona.generation.OutputManager;
import uk.co.real_logic.artio.builder.Encoder;
import uk.co.real_logic.artio.dictionary.ir.*;
import uk.co.real_logic.artio.dictionary.ir.Entry.Element;
import uk.co.real_logic.artio.dictionary.ir.Field.Type;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

import java.io.IOException;
import java.io.Writer;
import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.joining;
import static uk.co.real_logic.artio.dictionary.generation.AggregateType.GROUP;
import static uk.co.real_logic.artio.dictionary.generation.EnumGenerator.hasEnumGenerated;
import static uk.co.real_logic.artio.dictionary.generation.GenerationUtil.fileHeader;
import static uk.co.real_logic.artio.util.MutableAsciiBuffer.LONGEST_INT_LENGTH;
import static uk.co.real_logic.sbe.generation.java.JavaUtil.formatClassName;
import static uk.co.real_logic.sbe.generation.java.JavaUtil.formatPropertyName;

public class EncoderGenerator extends Generator
{
    private static final String SUFFIX =
        "        buffer.putSeparator(position);\n" +
        "        position++;\n" +
        "%s";

    private static final String TRAILER_ENCODE_PREFIX =
        "    public static final byte[] HEADER_PREFIX_STRING = \"%s\".getBytes(US_ASCII);\n\n" +
        "    int realStart;\n\n" +
        "    public int realStart()\n" +
        "    {\n" +
        "        return realStart;\n" +
        "    }\n\n" +
        "    public int encode(final MutableAsciiBuffer buffer, final int offset, final int bodyStart)\n" +
        "    {\n" +
        "        int position = offset;\n\n";

    private static final String GROUP_ENCODE_PREFIX =
        "    public int encode(final MutableAsciiBuffer buffer, final int offset, final int remainingElements)\n" +
        "    {\n" +
        "        if (remainingElements == 0)\n" +
        "        {\n" +
        "            return 0;\n" +
        "        }\n\n" +
        "        int position = offset;\n\n";

    // returns (offset, length) as long
    private static final String MESSAGE_ENCODE_PREFIX =
        "    public static int MAX_HEADER_PREFIX_LENGTH = %d;\n" +
        "    public long encode(final MutableAsciiBuffer buffer, final int offset)\n" +
        "    {\n" +
        "        int start = offset + MAX_HEADER_PREFIX_LENGTH;\n\n" +
        "        int position = start;\n\n" +
        "        position += header.encode(buffer, position);\n";

    // returns length as int
    private static final String OTHER_ENCODE_PREFIX =
        "    public int encode(final MutableAsciiBuffer buffer, final int offset)\n" +
        "    {\n" +
        "        int position = offset;\n\n";

    private static final String RESET_NEXT_GROUP =
        "        if (next != null)" +
        "        {\n" +
        "            next.reset();\n" +
        "        }\n";

    private static final int MAX_BODY_LENGTH_FIELD_LENGTH = String.valueOf(Integer.MAX_VALUE).length();
    public static final String METHOD_DELIMITER = "\n\n";

    private static String encoderClassName(final String name)
    {
        return formatClassName(name + "Encoder");
    }

    private final byte[] buffer = new byte[LONGEST_INT_LENGTH + 1];

    private final MutableAsciiBuffer string = new MutableAsciiBuffer(buffer);

    private final int initialArraySize;

    // Header prefix strings are of the form: "8=FIX.4.49="
    private final String headerPrefixString;
    private final int maxHeaderPrefixLength;

    public EncoderGenerator(
        final Dictionary dictionary,
        final int initialArraySize,
        final String builderPackage,
        final String builderCommonPackage,
        final OutputManager outputManager,
        final Class<?> validationClass)
    {
        super(dictionary, builderPackage, builderCommonPackage, outputManager, validationClass);

        final Component header = dictionary.header();
        validateHasField(header, BEGIN_STRING);
        validateHasField(header, BODY_LENGTH);

        this.initialArraySize = initialArraySize;
        headerPrefixString = String.format("8=%s.%d.%d\0019=",
            dictionary.specType(),
            dictionary.majorVersion(), dictionary.minorVersion());

        maxHeaderPrefixLength = headerPrefixString.length() + MAX_BODY_LENGTH_FIELD_LENGTH;
    }

    private void validateHasField(final Component header, final String fieldName)
    {
        if (!header.hasField(fieldName))
        {
            throw new IllegalArgumentException("Header does not contain needed field : " + fieldName);
        }
    }

    protected void generateAggregateFile(final Aggregate aggregate, final AggregateType aggregateType)
    {
        final String className = encoderClassName(aggregate.name());

        outputManager.withOutput(
            className,
            (out) ->
            {
                out.append(fileHeader(builderPackage));
                generateImports("Encoder", aggregateType, out);
                generateAggregateClass(aggregate, aggregateType, className, out);
            });
    }

    protected Class<?> topType(final AggregateType aggregateType)
    {
        return Encoder.class;
    }

    private void generateAggregateClass(
        final Aggregate aggregate,
        final AggregateType type,
        final String className,
        final Writer out) throws IOException
    {
        final boolean isMessage = type == AggregateType.MESSAGE;
        final List<String> interfaces = isMessage ? singletonList(Encoder.class.getSimpleName()) : emptyList();
        out.append(classDeclaration(className, interfaces, type == GROUP));
        out.append(constructor(aggregate, dictionary));
        if (isMessage)
        {
            out.append(commonCompoundImports("Encoder", false));
        }
        else if (type == GROUP)
        {
            final Group group = (Group)aggregate;
            out.append(nextMethod(group));
        }
        precomputedHeaders(out, aggregate.entries());
        setters(out, className, aggregate.entries());
        out.append(encodeMethod(aggregate.entries(), type));
        out.append(completeResetMethod(aggregate, isMessage, type));
        out.append(toString(aggregate, isMessage));
        out.append("}\n");
    }

    private String completeResetMethod(
        final Aggregate aggregate, final boolean isMessage, final AggregateType type)
    {
        final String additionalReset =  type == GROUP ? RESET_NEXT_GROUP : "";
        return super.completeResetMethod(isMessage, aggregate.entries(), additionalReset);
    }

    private void generateGroupClass(final Group group, final Writer out) throws IOException
    {
        final String className = encoderClassName(group.name());
        generateAggregateClass(group, GROUP, className, out);
    }

    private String nextMethod(final Group group)
    {
        return String.format(
            "    private %1$s next = null;\n\n" +
            "    public %1$s next()\n" +
            "    {\n" +
            "        if (next == null)\n" +
            "        {\n" +
            "            next = new %1$s();\n" +
            "        }\n" +
            "        return next;\n" +
            "    }\n\n",
            encoderClassName(group.name())
        );
    }

    private String constructor(final Aggregate aggregate, final Dictionary dictionary)
    {
        if (aggregate instanceof Message)
        {
            final Component header = dictionary.header();
            final Message message = (Message)aggregate;
            final int packedType = message.packedType();
            final String fullType = message.fullType();
            final String msgType = header.hasField(MSG_TYPE) ?
                String.format("        header.msgType(\"%s\");\n", fullType) : "";

            return String.format(
                "    public int messageType()\n" +
                "    {\n" +
                "        return %s;\n" +
                "    }\n\n" +
                "    public %sEncoder()\n" +
                "    {\n" +
                "%s" +
                "    }\n\n",
                packedType,
                message.name(),
                msgType);
        }

        return "";
    }

    private void setters(final Writer out, final String className, final List<Entry> entries) throws IOException
    {
        for (final Entry entry : entries)
        {
            setter(className, entry, out);
        }
    }

    private void setter(final String className, final Entry entry, final Writer out) throws IOException
    {
        if (!isBodyLength(entry))
        {
            entry.forEach(
                (field) -> out.append(fieldSetter(className, field)),
                (group) -> generateGroup(className, group, out),
                (component) -> componentField(encoderClassName(entry.name()), component, out));
        }
    }

    private String fieldSetter(final String className, final Field field)
    {
        final String name = field.name();
        final String fieldName = formatPropertyName(name);
        final String hasField =
            String.format("    private boolean has%1$s;\n\n", name) + hasGetter(name);

        final String hasAssign = String.format("        has%s = true;\n", name);

        final String enumSetter = hasEnumGenerated(field) && !field.type().isMultiValue() ?
            enumSetter(className, fieldName, field.name()) : "";

        final Function<String, String> generateSetter =
            (type) -> setter(name, type, fieldName, hasField, className, hasAssign, enumSetter);

        switch (field.type())
        {
            case STRING:
            case MULTIPLEVALUESTRING:
            case MULTIPLESTRINGVALUE:
            case MULTIPLECHARVALUE:
            case CURRENCY:
            case EXCHANGE:
            case COUNTRY:
            case LANGUAGE:
                return generateStringSetter(className, fieldName, name, enumSetter);
            case BOOLEAN:
                return generateSetter.apply("boolean");

            case CHAR:
                return generateSetter.apply("char");

            case INT:
            case LENGTH:
            case SEQNUM:
            case NUMINGROUP:
            case DAYOFMONTH:
                return generateSetter.apply("int");

            case FLOAT:
            case PRICE:
            case PRICEOFFSET:
            case QTY:
            case PERCENTAGE:
            case AMT:
                return generateSetter.apply("DecimalFloat");

            case DATA:
            case XMLDATA:
                // DATA fields always come with their own Length field defined by the schema
                return generateSetter.apply("byte[]");

            case UTCTIMESTAMP:
            case LOCALMKTDATE:
            case UTCDATEONLY:
            case UTCTIMEONLY:
            case MONTHYEAR:
            case TZTIMEONLY:
            case TZTIMESTAMP:
                return generateByteArraySetter(className, fieldName, name);

            default: throw new UnsupportedOperationException("Unknown type: " + field.type());
        }
    }

    private void generateGroup(final String className, final Group group, final Writer out) throws IOException
    {
        generateGroupClass(group, out);

        final Entry numberField = group.numberField();
        setter(className, numberField, out);

        out.append(String.format(
            "\n" +
            "    private %1$s %2$s = null;\n\n" +
            "    public %1$s %2$s(final int numberOfElements)\n" +
            "    {\n" +
            "        has%3$s = true;\n" +
            "        %4$s = numberOfElements;\n" +
            "        if (%2$s == null)\n" +
            "        {\n" +
            "            %2$s = new %1$s();\n" +
            "        }\n" +
            "        return %2$s;\n" +
            "    }\n\n",
            encoderClassName(group.name()),
            formatPropertyName(group.name()),
            numberField.name(),
            formatPropertyName(numberField.name())));
    }

    private String generateByteArraySetter(final String className, final String fieldName, final String name)
    {
        return String.format(
            "    private byte[] %1$s = new byte[%3$d];\n\n" +
            "    private int %1$sLength = 0;\n\n" +
            "    public %2$s %1$s(final byte[] value, final int length)\n" +
            "    {\n" +
            "        %1$s = value;\n" +
            "        %1$sLength = length;\n" +
            "        return this;\n" +
            "    }\n\n" +
            "    public %2$s %1$s(final byte[] value)\n" +
            "    {\n" +
            "        return %1$s(value, value.length);\n" +
            "    }\n\n" +
            "    public boolean has%4$s()\n" +
            "    {\n" +
            "        return %1$sLength > 0;\n" +
            "    }\n\n" +
            "    public byte[] %1$s()\n" +
            "    {\n" +
            "        return %1$s;\n" +
            "    }\n\n",
            fieldName,
            className,
            initialArraySize,
            name);
    }

    private String generateStringSetter(
        final String className,
        final String fieldName,
        final String name,
        final String enumSetter)
    {
        return String.format(
            "%2$s" +
            "    public %3$s %1$s(final CharSequence value)\n" +
            "    {\n" +
            "        %1$s = toBytes(value, %1$s);\n" +
            "        %1$sLength = value.length();\n" +
            "        return this;\n" +
            "    }\n\n" +
            "    public %3$s %1$s(final char[] value)\n" +
            "    {\n" +
            "        return %1$s(value, value.length);\n" +
            "    }\n\n" +
            "    public %3$s %1$s(final char[] value, final int length)\n" +
            "    {\n" +
            "        %1$s = toBytes(value, %1$s, length);\n" +
            "        %1$sLength = length;\n" +
            "        return this;\n" +
            "    }\n\n" +
            "    public %3$s %1$s(final char[] value, final int offset, final int length)\n" +
            "    {\n" +
            "        %1$s = toBytes(value, %1$s, offset, length);\n" +
            "        %1$sLength = length;\n" +
            "        return this;\n" +
            "    }\n\n" +
            "%4$s",
            fieldName,
            generateByteArraySetter(className, fieldName, name),
            className,
            enumSetter);
    }

    private String setter(
        final String name,
        final String type,
        final String fieldName,
        final String optionalField,
        final String className,
        final String optionalAssign,
        final String enumSetter)
    {
        return String.format(
            "    %s %s %s;\n\n" +
            "%s" +
            "    public %s %3$s(%2$s value)\n" +
            "    {\n" +
            "        %3$s = value;\n" +
            "%s" +
            "        return this;\n" +
            "    }\n\n" +
            "    public %2$s %3$s()\n" +
            "    {\n" +
            "        return %3$s;\n" +
            "    }\n\n" +
            "%7$s",
            isBodyLength(name) ? "public" : "private",
            type,
            fieldName,
            optionalField,
            className,
            optionalAssign,
            enumSetter);
    }

    private String enumSetter(
        final String className,
        final String fieldName,
        final String enumType)
    {
        return String.format(
            "    public %s %2$s(%3$s value)\n" +
            "    {\n" +
            "        return %2$s(value.representation());\n" +
            "    }\n\n",
            className, fieldName, enumType
        );
    }

    private String encodeMethod(final List<Entry> entries, final AggregateType aggregateType)
    {
        final boolean hasCommonCompounds = aggregateType == AggregateType.MESSAGE;

        final String prefix;
        switch (aggregateType)
        {
            case TRAILER:
                prefix = String.format(TRAILER_ENCODE_PREFIX, headerPrefixString);
                break;

            case GROUP:
                prefix = GROUP_ENCODE_PREFIX;
                break;

            case MESSAGE:
                prefix = String.format(MESSAGE_ENCODE_PREFIX, maxHeaderPrefixLength);
                break;

            default:
                prefix = OTHER_ENCODE_PREFIX;
                break;
        }

        final String body = entries.stream()
            .map(this::encodeEntry)
            .collect(joining("\n"));

        String suffix;
        if (hasCommonCompounds)
        {
            suffix =
                "        position += trailer.encode(buffer, position, start);\n" +
                "        final int realStart = trailer.realStart;" +
                "        return Encoder.result(position - realStart, realStart);\n" +
                "    }\n\n";
        }
        else
        {
            suffix =
                "        return position - offset;\n" +
                "    }\n\n";

            if (aggregateType == GROUP)
            {
                suffix =
                    "        if (next != null)\n" +
                    "        {\n" +
                    "            position += next.encode(buffer, position, remainingElements - 1);\n" +
                    "        }\n" + suffix;
            }
        }

        return prefix + body + suffix;
    }

    private String encodeEntry(final Entry entry)
    {
        if (isBodyLength(entry) || isBeginString(entry))
        {
            return "";
        }
        else if (isCheckSum(entry))
        {
            return encodeChecksum();
        }
        else
        {
            return entry.matchEntry(this::encodeField, this::encodeGroup, this::encodeComponent);
        }
    }

    private String encodeChecksum()
    {
        return
            "        final int bodyLength = position - bodyStart;\n" +
            "        buffer.putSeparator(bodyStart - 1);\n" +
            "        final int bodyLengthStart = buffer.putNaturalFromEnd(bodyLength, bodyStart - 1);\n" +
            "        final int realStart = bodyLengthStart - HEADER_PREFIX_STRING.length;\n" +
            "        this.realStart = realStart;" +
            "        buffer.putBytes(realStart, HEADER_PREFIX_STRING);\n" +
            formatTag("checkSum", "") +
            // 17 to account for the common sized prefix size before bodyStart.
            // position - 3, to get back to the point before the checksum, ie skip behind (10)
            "        final int checkSum = buffer.computeChecksum(realStart, position - 3);\n" +
            "        buffer.putNatural(position, 3, checkSum);\n" +
            "        position += 3;\n" +
            "        buffer.putSeparator(position);\n" +
            "        position++;\n";
    }

    private String encodeField(final Entry entry)
    {
        final Element element = entry.element();
        final Field field = (Field)element;
        final String name = field.name();
        final String fieldName = formatPropertyName(name);
        final Field.Type type = field.type();
        final boolean mustCheckFlag = hasFlag(entry, field);
        final boolean mustCheckLength = type.hasLengthField();
        final boolean needsMissingThrow =
            (mustCheckFlag || mustCheckLength) && entry.required() && !"MsgSeqNum".equals(name);

        final String enablingPrefix;
        if (mustCheckFlag)
        {
            enablingPrefix = String.format("        if (has%s) {\n", name);
        }
        else if (mustCheckLength)
        {
            enablingPrefix = String.format("        if (%sLength > 0) {\n", fieldName);
        }
        else
        {
            enablingPrefix = "";
        }
        String enablingSuffix = mustCheckFlag || mustCheckLength ? "        }\n" : "";

        if (needsMissingThrow)
        {
            enablingSuffix = enablingSuffix +
                "        else if (" + CODEC_VALIDATION_ENABLED + ")\n" +
                "        {\n" +
                "            throw new EncodingException(\"Missing Field: " + name + "\");\n" +
                "        }\n";
        }

        final String tag = formatTag(fieldName, enablingPrefix);

        switch (type)
        {
            case INT:
            case LENGTH:
            case SEQNUM:
            case NUMINGROUP:
            case DAYOFMONTH:
                return putValue(fieldName, tag, "Int", enablingSuffix);

            case FLOAT:
            case PRICE:
            case PRICEOFFSET:
            case QTY:
            case PERCENTAGE:
            case AMT:
                return putValue(fieldName, tag, "Float", enablingSuffix);

            case CHAR:
                return putValue(fieldName, tag, "Char", enablingSuffix);

            case BOOLEAN:
                return putValue(fieldName, tag, "Boolean", enablingSuffix);

            case STRING:
            case MULTIPLEVALUESTRING:
            case MULTIPLESTRINGVALUE:
            case MULTIPLECHARVALUE:
            case CURRENCY:
            case EXCHANGE:
            case COUNTRY:
            case LANGUAGE:
            case LOCALMKTDATE:
            case UTCTIMESTAMP:
            case MONTHYEAR:
            case UTCTIMEONLY:
            case UTCDATEONLY:
            case TZTIMEONLY:
            case TZTIMESTAMP:
                return stringPut(fieldName, enablingSuffix, tag);

            case DATA:
            case XMLDATA:
                return String.format(
                    "%s" +
                    "        buffer.putBytes(position, %s);\n" +
                    "        position += %2$s.length;\n" +
                    SUFFIX,
                    tag,
                    fieldName,
                    enablingSuffix);

            default:
                throw new UnsupportedOperationException("Unknown type: " + type);
        }
    }

    private String stringPut(final String fieldName, final String optionalSuffix, final String tag)
    {
        return formatEncoder(fieldName, optionalSuffix, tag,
            "        buffer.putBytes(position, %s, 0, %2$sLength);\n" +
            "        position += %2$sLength;\n");
    }

    private String formatEncoder(
        final String fieldName, final String optionalSuffix, final String tag, final String format)
    {
        return String.format(
            "%s" + format + SUFFIX,
            tag,
            fieldName,
            optionalSuffix);
    }

    private String encodeGroup(final Entry entry)
    {
        final Group group = (Group)entry.element();
        return String.format(
            "%1$s\n" +
            "        if (%2$s != null)\n" +
            "        {\n" +
            "            position += %2$s.encode(buffer, position, %3$s);\n" +
            "        }\n",
            encodeField(group.numberField()),
            formatPropertyName(group.name()),
            formatPropertyName(group.numberField().name()));
    }

    private String encodeComponent(final Entry entry)
    {
        // TODO: make component return int, split encode prefix
        return String.format(
            "            position += %1$s.encode(buffer, position);\n",
            formatPropertyName(entry.name()));
    }

    private String formatTag(final String fieldName, final String optionalPrefix)
    {
        return String.format(
            "%s" +
            "        buffer.putBytes(position, %sHeader, 0, %2$sHeaderLength);\n" +
            "        position += %2$sHeaderLength;\n",
            optionalPrefix,
            fieldName);
    }

    private String putValue(final String fieldName, final String tag, final String type, final String optionalSuffix)
    {
        return String.format(
            "%s" +
            "        position += buffer.putAscii%s(position, %s);\n" +
            SUFFIX,
            tag,
            type,
            fieldName,
            optionalSuffix);
    }

    private void precomputedHeaders(final Writer out, final List<Entry> entries) throws IOException
    {
        for (final Entry entry : entries)
        {
            if (!isBodyLength(entry))
            {
                final Element element = entry.element();
                if (element instanceof Field)
                {
                    precomputedFieldHeader(out, (Field)element);
                }
                else if (element instanceof Group)
                {
                    final Group group = (Group)element;
                    precomputedFieldHeader(out, (Field)group.numberField().element());
                }
            }
        }
    }

    private void precomputedFieldHeader(final Writer out, final Field field) throws IOException
    {
        final String name = field.name();
        final String fieldName = formatPropertyName(name);
        final int length = string.putAsciiInt(0, field.number());
        final String bytes = IntStream
            .range(0, length)
            .mapToObj(i -> String.valueOf(buffer[i]))
            .collect(joining(", ", "", ", (byte) '='"));

        out.append(String.format(
            "    private static final int %sHeaderLength = %d;\n" +
            "    private static final byte[] %1$sHeader = new byte[] {%s};\n\n",
            fieldName,
            length + 1,
            bytes));
    }

    protected String stringToString(final String fieldName)
    {
        return String.format("new String(%s, 0, %1$sLength, StandardCharsets.US_ASCII)", fieldName);
    }

    protected String componentToString(final Component component)
    {
        final String name = component.name();
        return String.format(
            "                String.format(\"  \\\"%1$s\\\":  %%s\\n\", %2$s" + EXPAND_INDENT + ")",
            name,
            formatPropertyName(name));
    }

    private void componentField(final String className, final Component element, final Writer out) throws IOException
    {
        out.append(String.format(
            "    private final %1$s %2$s = new %1$s();\n" +
            "    public %1$s %2$s()\n" +
            "    {\n" +
            "        return %2$s;\n" +
            "    }\n\n",
            className,
            formatPropertyName(element.name())));
    }

    protected String resetRequiredFloat(final String name)
    {
        return resetByFlag(name);
    }

    protected String resetRequiredInt(final Field field)
    {
        return resetByFlag(field.name());
    }

    protected String toStringGroupParameters()
    {
        return "final int remainingEntries";
    }

    protected String toStringGroupSuffix()
    {
        return
            "        if (remainingEntries > 1)\n" +
            "        {\n" +
            "            entries += \",\\n\" + next.toString(remainingEntries - 1);\n" +
            "        }\n";
    }

    protected boolean hasFlag(final Entry entry, final Field field)
    {
        final Type type = field.type();
        return (!entry.required() && !type.hasLengthField()) || type.isFloatBased() || type.isIntBased();
    }

    protected String resetTemporalValue(final String name)
    {
        return resetLength(name);
    }

    protected String resetComponents(final List<Entry> entries, final StringBuilder methods)
    {
        return entries
            .stream()
            .filter(Entry::isComponent)
            .map(this::callComponentReset)
            .collect(joining());
    }

    @Override
    protected String resetStringBasedData(final String name)
    {
        return resetLength(name);
    }

    protected String groupEntryToString(final Group element, final String name)
    {
        final Entry numberField = element.numberField();
        return String.format(
            "                (%3$s > 0 ? String.format(\"  \\\"%1$s\\\": [\\n" +
            "  %%s" +
            "\\n  ]" +
            "\\n\", %2$s.toString(%3$s).replace(\"\\n\", \"\\n  \")" + ") : \"\")",
            name,
            formatPropertyName(name),
            formatPropertyName(numberField.name()));
    }

    protected String optionalReset(final Field field, final String name)
    {
        return field.type().hasLengthField() ? resetLength(name) : resetByFlag(name);
    }

    protected boolean toStringChecksHasGetter(final Entry entry, final Field field)
    {
        return hasFlag(entry, field) || field.type().hasLengthField();
    }
}
