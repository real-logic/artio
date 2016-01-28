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
import uk.co.real_logic.fix_gateway.builder.Encoder;
import uk.co.real_logic.fix_gateway.builder.MessageEncoder;
import uk.co.real_logic.fix_gateway.dictionary.ir.*;
import uk.co.real_logic.fix_gateway.dictionary.ir.Entry.Element;
import uk.co.real_logic.fix_gateway.util.MutableAsciiBuffer;

import java.io.IOException;
import java.io.Writer;
import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.joining;
import static uk.co.real_logic.fix_gateway.dictionary.generation.AggregateType.GROUP;
import static uk.co.real_logic.fix_gateway.dictionary.generation.GenerationUtil.fileHeader;
import static uk.co.real_logic.fix_gateway.util.MutableAsciiBuffer.LONGEST_INT_LENGTH;
import static uk.co.real_logic.sbe.generation.java.JavaUtil.formatClassName;
import static uk.co.real_logic.sbe.generation.java.JavaUtil.formatPropertyName;

public class EncoderGenerator extends Generator
{
    private static final String SUFFIX =
        "        buffer.putSeparator(position);\n" +
        "        position++;\n" +
        "%s";

    private static final String TRAILER_PREFIX =
        "    public int encode(final MutableAsciiBuffer buffer, final int offset)\n" +
        "    {\n" +
        "        throw new UnsupportedOperationException();\n" +
        "    }\n\n" +
        "    public int encode(final MutableAsciiBuffer buffer, final int offset, final int bodyStart)\n" +
        "    {\n" +
        "        int position = offset;\n\n";

    public static String encoderClassName(final String name)
    {
        return formatClassName(name + "Encoder");
    }

    private final byte[] buffer = new byte[LONGEST_INT_LENGTH + 1];

    private final MutableAsciiBuffer string = new MutableAsciiBuffer(buffer);

    private final int initialArraySize;

    public EncoderGenerator(
        final Dictionary dictionary,
        final int initialArraySize,
        final String builderPackage,
        final OutputManager outputManager,
        final Class<?> validationClass)
    {
        super(dictionary, builderPackage, outputManager, validationClass);
        this.initialArraySize = initialArraySize;
    }

    protected void aggregate(final Aggregate aggregate, final AggregateType aggregateType)
    {
        final String className = encoderClassName(aggregate.name());
        final boolean isMessage = aggregateType == AggregateType.MESSAGE;

        outputManager.withOutput(className, out ->
        {
            out.append(fileHeader(builderPackage));
            final Class<?> type = isMessage ? MessageEncoder.class : Encoder.class;
            out.append(classDeclaration(className, aggregateType, emptyList(), "Encoder", type));
            out.append(constructor(aggregate, dictionary));
            if (isMessage)
            {
                out.append(commonCompoundImports("Encoder"));
            }
            else if (aggregateType == GROUP)
            {
                final Group group = (Group) aggregate;
                out.append(nextMethod(group));
            }
            precomputedHeaders(out, aggregate.entries());
            setters(out, className, aggregate.entries());
            out.append(encodeMethod(aggregate.entries(), aggregateType));
            out.append(completeResetMethod(isMessage, aggregate.entries(), ""));
            out.append(toString(aggregate, isMessage));
            out.append("}\n");
        });
    }

    private String nextMethod(final Group group)
    {
        return String.format(
            "    private %1$s next = null;\n\n" +
            "    public %1$s next()\n" +
            "    {\n" +
            "        if (next == null)\n" +
            "        {\n" +
            "            next = new %1$s(onNext);\n" +
            "            onNext.run();\n" +
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
            final Message message = (Message) aggregate;
            final int packedType = message.packedType();
            final String fullType = message.fullType();
            final String msgType =
                header.hasField(MSG_TYPE)
                    ? String.format("        header.msgType(\"%s\");\n", fullType) : "";

            final String beginString =
                header.hasField("BeginString")
                    ? String.format("        header.beginString(\"FIX.%d.%d\");\n",
                    dictionary.majorVersion(), dictionary.minorVersion()) : "";

            return String.format(
                "    public int messageType()\n" +
                "    {\n" +
                "        return %s;\n" +
                "    }\n\n" +
                "    public %sEncoder()\n" +
                "    {\n" +
                "%s" +
                "%s" +
                "    }\n\n",
                packedType,
                message.name(),
                msgType,
                beginString
            );
        }
        else if (aggregate instanceof Group)
        {
            final Group group = (Group) aggregate;
            return String.format(
                "    private final Runnable onNext;\n\n" +
                "    public %1$s(final Runnable onNext)\n" +
                "    {\n" +
                "        this.onNext = onNext;\n" +
                "    }\n",
                encoderClassName(group.name())
            );
        }

        return "";
    }

    private void setters(final Writer out, final String className, final List<Entry> entries) throws IOException
    {
        for (final Entry entry : entries)
        {
            out.append(setter(className, entry));
        }
    }

    private String setter(final String className, final Entry entry)
    {
        return entry.match(
            (e, field) -> fieldSetter(className, field),
            (e, group) -> groupSetter(className, group),
            (e, component) -> componentField(encoderClassName(e.name()), component));
    }

    private String fieldSetter(final String className, final Field field)
    {
        final String name = field.name();
        final String fieldName = formatPropertyName(name);
        final String hasField =
            String.format("    private boolean has%1$s;\n\n", name) + hasGetter(name);

        final String hasAssign = String.format("        has%s = true;\n", name);

        // TODO: make encoding generation more regular and delegate to library calls more
        final Function<String, String> generateSetter =
            (type) -> setter(name, type, fieldName, hasField, className, hasAssign);

        switch (field.type())
        {
            case STRING:
            case MULTIPLEVALUESTRING:
            case CURRENCY:
            case EXCHANGE:
            case COUNTRY:
                return stringSetter(className, fieldName, hasField, hasAssign);

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
                return generateSetter.apply("byte[]");

            case UTCTIMESTAMP:
            case LOCALMKTDATE:
            case UTCDATEONLY:
            case UTCTIMEONLY:
            case MONTHYEAR:
            {
                return String.format(
                    "    private byte[] %1$s;\n\n" +
                    "    private int %1$sLength;\n\n" +
                    "%2$s" +
                    "    public %3$s %1$s(final byte[] value, final int length)\n" +
                    "    {\n" +
                    "        %1$s = value;\n" +
                    "        %1$sLength = length;\n" +
                    "%4$s" +
                    "        return this;\n" +
                    "    }\n\n" +
                    "    public %3$s %1$s(final byte[] value)\n" +
                    "    {\n" +
                    "        return %1$s(value, value.length);\n" +
                    "    }\n\n",
                    fieldName,
                    hasField,
                    className,
                    hasAssign);
            }

            default: throw new UnsupportedOperationException("Unknown type: " + field.type());
        }
    }

    private String groupSetter(final String className, final Group group)
    {
        group(group);

        final Entry numberField = group.numberField();
        final String setter = setter(className, numberField);

        return String.format(
            "%1$s\n" +
                "    public void inc%4$s()\n" +
                "    {\n" +
                "        %5$s++;\n" +
                "    }\n\n" +
                "    private %2$s %3$s = null;\n\n" +
                "    public %2$s %3$s()\n" +
                "    {\n" +
                "        if (%3$s == null)\n" +
                "        {\n" +
                "            has%4$s = true;\n" +
                "            %5$s = 1;\n" +
                "            %3$s = new %2$s(this::inc%4$s);\n" +
                "        }\n" +
                "        return %3$s;\n" +
                "    }\n\n",
            setter,
            encoderClassName(group.name()),
            formatPropertyName(group.name()),
            numberField.name(),
            formatPropertyName(numberField.name()));
    }

    private String stringSetter(
        final String className,
        final String fieldName,
        final String optionalField,
        final String optionalAssign)
    {
        return String.format(
            "    private byte[] %s = new byte[%d];\n\n" +
            "    private int %1$sLength = 0;\n\n" +
            "%s" +
            "    public %s %1$s(final CharSequence value)\n" +
            "    {\n" +
            "        %1$s = toBytes(value, %1$s);\n" +
            "        %1$sLength = value.length();\n" +
            "%s" +
            "        return this;\n" +
            "    }\n\n" +
            "    public %4$s %1$s(final char[] value)\n" +
            "    {\n" +
            "        return %1$s(value, value.length);\n" +
            "    }\n\n" +
            "    public %4$s %1$s(final char[] value, final int length)\n" +
            "    {\n" +
            "        %1$s = toBytes(value, %1$s, length);\n" +
            "        %1$sLength = length;\n" +
            "%5$s" +
            "        return this;\n" +
            "    }\n\n" +
            "    public %4$s %1$s(final byte[] value)\n" +
            "    {\n" +
            "        return %1$s(value, value.length);\n" +
            "    }\n\n" +
            "    public %4$s %1$s(final byte[] value, final int length)\n" +
            "    {\n" +
            "        %1$s = value;\n" +
            "        %1$sLength = length;\n" +
            "%5$s" +
            "        return this;\n" +
            "    }\n\n",
            fieldName,
            initialArraySize,
            optionalField,
            className,
            optionalAssign);
    }

    private String setter(
        final String name,
        final String type,
        final String fieldName,
        final String optionalField,
        final String className,
        final String optionalAssign)
    {
        return String.format(
            "    %s %s %s;\n\n" +
            "%s" +
            "    public %s %3$s(%2$s value)\n" +
            "    {\n" +
            "        %3$s = value;\n" +
            "%s" +
            "        return this;\n" +
            "    }\n\n",
            isBodyLength(name) ? "public" : "private",
            type,
            fieldName,
            optionalField,
            className,
            optionalAssign);
    }

    private String encodeMethod(final List<Entry> entries, final AggregateType aggregateType)
    {
        final boolean hasCommonCompounds = aggregateType == AggregateType.MESSAGE;

        final String prefix =
            aggregateType == AggregateType.TRAILER ?
                TRAILER_PREFIX :
                ("    public int encode(final MutableAsciiBuffer buffer, final int offset)\n" +
                "    {\n" +
                "        int position = offset;\n\n" +
                (hasCommonCompounds ? "        position += header.encode(buffer, position);\n" : ""));

        final String body =
            entries.stream()
                   .map(this::encodeEntry)
                   .collect(joining("\n"));

        String suffix = "";
        if (hasCommonCompounds)
        {
            suffix = "        position += trailer.encode(buffer, position, header.bodyLength);\n";
        }
        else if (aggregateType == GROUP)
        {
            suffix =
                "        if (next != null)\n" +
                "        {\n" +
                "            position += next.encode(buffer, position);\n" +
                "        }\n";
        }
        suffix += "        return position - offset;\n" +
                  "    }\n\n";

        return prefix + body + suffix;
    }

    private String encodeEntry(final Entry entry)
    {
        if (isBodyLength(entry))
        {
            return encodeBodyLength();
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

    private String encodeBodyLength()
    {
        return "        buffer.putBytes(position, BODY_LENGTH);\n" +
               "        position += BODY_LENGTH.length;\n" +
               "        bodyLength(position);\n";
    }

    private String encodeChecksum()
    {
        return "        final int bodyLength = position - bodyStart;\n" +
               "        buffer.putNatural(bodyStart - BODY_LENGTH_SIZE, BODY_LENGTH_GAP, bodyLength);\n" +
               formatTag("checkSum", "") +
               // 17 to account for the common sized prefix size before bodyStart.
               // position - 2, to get back to the point before the checksum
               "        final int checkSum = buffer.computeChecksum(bodyStart - 17, position - 3);\n" +
               "        buffer.putNatural(position, 3, checkSum);\n" +
               "        position += 3;\n" +
               "        buffer.putSeparator(position);\n" +
               "        position++;\n";
    }

    private String encodeField(final Entry entry)
    {
        final Element element = entry.element();
        final Field field = (Field) element;
        final String name = field.name();
        final String fieldName = formatPropertyName(name);
        final Field.Type type = field.type();
        final boolean mustCheckFlag = hasFlag(entry, field);
        final boolean mustCheckLength = type.isStringBased();
        final boolean needsMissingThrow = (type.isFloatBased() || mustCheckLength) && entry.required();

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
            case CURRENCY:
            case EXCHANGE:
            case COUNTRY:
            case LOCALMKTDATE:
            case UTCTIMESTAMP:
            case MONTHYEAR:
            case UTCTIMEONLY:
            case UTCDATEONLY:
                return stringPut(fieldName, enablingSuffix, tag);

            case DATA:
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
        final Group group = (Group) entry.element();
        return String.format(
            "%1$s\n" +
            "        if (%2$s != null)\n" +
            "        {\n" +
            "            position += %2$s.encode(buffer, position);\n" +
            "        }\n",
            encodeField(group.numberField()),
            formatPropertyName(group.name())
        );
    }

    private String encodeComponent(final Entry entry)
    {
        return String.format(
            "            position += %1$s.encode(buffer, position);\n",
            formatPropertyName(entry.name())
        );
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

    private String putValue(final String fieldName, final String tag, final String type, String optionalSuffix)
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
            final Element element = entry.element();
            if (element instanceof Field)
            {
                precomputedFieldHeader(out, (Field) element);
            }
            else if (element instanceof Group)
            {
                final Group group = (Group) element;
                precomputedFieldHeader(out, (Field) group.numberField().element());
            }
        }
    }

    private void precomputedFieldHeader(final Writer out, final Field field) throws IOException
    {
        final String name = field.name();
        final String fieldName = formatPropertyName(name);
        // TODO: tags aren't always ints
        final int length = string.putAsciiInt(0, field.number());
        final String bytes =
            IntStream.range(0, length)
                     .mapToObj(i -> String.valueOf(buffer[i]))
                     .collect(joining(", ", "", ", (byte) '='"));

        out.append(String.format(
            "    private static final int %sHeaderLength = %d;\n" +
            "    private static final byte[] %1$sHeader = new byte[] {%s};\n\n",
            fieldName,
            length + 1,
            bytes));
    }

    protected String stringToString(String fieldName)
    {
        return String.format("new String(%s, 0, %1$sLength, StandardCharsets.US_ASCII)", fieldName);
    }

    protected String componentToString(final Component component)
    {
        final String name = component.name();
        return String.format(
            "                String.format(\"  \\\"%1$s\\\":  %%s\\n\", %2$s" + EXPAND_INDENT + ")",
            name,
            formatPropertyName(name)
        );
    }

    protected String componentField(final String className, final Component element)
    {
        return String.format(
            "    private final %1$s %2$s = new %1$s();\n" +
            "    public %1$s %2$s()\n" +
            "    {" +
            "        return %2$s;" +
            "    }",
            className,
            formatPropertyName(element.name())
        );
    }

    protected String resetFloat(final String name)
    {
        return resetByFlag(name);
    }

    protected boolean hasFlag(final Entry entry, final Field field)
    {
        return !entry.required() || field.type().isFloatBased();
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
}
