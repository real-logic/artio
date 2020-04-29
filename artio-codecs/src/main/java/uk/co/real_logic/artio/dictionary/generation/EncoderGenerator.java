/*
 * Copyright 2015-2020 Real Logic Limited., Monotonic Ltd.
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

import org.agrona.AsciiSequenceView;
import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.generation.OutputManager;
import uk.co.real_logic.artio.builder.Encoder;
import uk.co.real_logic.artio.builder.SessionHeaderEncoder;
import uk.co.real_logic.artio.dictionary.ir.*;
import uk.co.real_logic.artio.dictionary.ir.Entry.Element;
import uk.co.real_logic.artio.dictionary.ir.Field.Type;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

import java.io.IOException;
import java.io.Writer;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.IntStream;

import static java.util.Collections.*;
import static java.util.stream.Collectors.joining;
import static uk.co.real_logic.artio.dictionary.generation.AggregateType.GROUP;
import static uk.co.real_logic.artio.dictionary.generation.AggregateType.HEADER;
import static uk.co.real_logic.artio.dictionary.generation.EnumGenerator.hasEnumGenerated;
import static uk.co.real_logic.artio.dictionary.generation.GenerationUtil.fileHeader;
import static uk.co.real_logic.artio.dictionary.generation.GenerationUtil.importFor;
import static uk.co.real_logic.artio.dictionary.generation.OptionalSessionFields.ENCODER_OPTIONAL_SESSION_FIELDS;
import static uk.co.real_logic.artio.util.MutableAsciiBuffer.LONGEST_INT_LENGTH;
import static uk.co.real_logic.sbe.generation.java.JavaUtil.formatClassName;
import static uk.co.real_logic.sbe.generation.java.JavaUtil.formatPropertyName;

public class EncoderGenerator extends Generator
{
    private static final Set<String> REQUIRED_SESSION_CODECS = new HashSet<>(Arrays.asList(
        "LogonEncoder",
        "ResendRequestEncoder",
        "LogoutEncoder",
        "HeartbeatEncoder",
        "RejectEncoder",
        "TestRequestEncoder",
        "SequenceResetEncoder"));

    private static final String SUFFIX =
        "        buffer.putSeparator(position);\n" +
        "        position++;\n" +
        "%s";

    private static final String TRAILER_ENCODE_PREFIX =
        "    // |10=...|\n" +
        "    long finishMessage(final MutableAsciiBuffer buffer, final int messageStart, final int offset)\n" +
        "    {\n" +
        "        int position = offset;\n" +
        "\n" +
        "        final int checkSum = buffer.computeChecksum(messageStart, position);\n" +
        "        buffer.putBytes(position, checkSumHeader, 0, checkSumHeaderLength);\n" +
        "        position += checkSumHeaderLength;\n" +
        "        buffer.putNaturalPaddedIntAscii(position, 3, checkSum);\n" +
        "        position += 3;\n" +
        "        buffer.putSeparator(position);\n" +
        "        position++;\n" +
        "\n" +
        "        return Encoder.result(position - messageStart, messageStart);\n" +
        "    }" +
        "\n" +
        "    // Optional trailer fields\n" +
        "    int startTrailer(final MutableAsciiBuffer buffer, final int offset)\n" +
        "    {\n" +
        "        final int start = offset;\n" +
        "        int position = start;\n" +
        "\n";

    // returns offset where message starts
    private static final String HEADER_ENCODE_PREFIX =
        "    // 8=...|9=...|\n" +
        "    int finishHeader(final MutableAsciiBuffer buffer, final int bodyStart, final int bodyLength)\n" +
        "    {\n" +
        "        int position = bodyStart - 1;\n" +
        "\n" +
        "        buffer.putSeparator(position);\n" +
        "        position = buffer.putNaturalIntAsciiFromEnd(bodyLength, position);\n" +
        "        position -= bodyLengthHeaderLength;\n" +
        "        buffer.putBytes(position, bodyLengthHeader, 0, bodyLengthHeaderLength);\n" +
        "\n" +
        "        if (beginStringLength > 0) {\n" +
        "        position--;\n" +
        "        buffer.putSeparator(position);\n" +
        "        position -= beginStringLength;\n" +
        "        buffer.putBytes(position, beginString, beginStringOffset, beginStringLength);\n" +
        "        position -= beginStringHeaderLength;\n" +
        "        buffer.putBytes(position, beginStringHeader, 0, beginStringHeaderLength);\n" +
        "        } else if (" + CODEC_VALIDATION_ENABLED + ")\n" +
        "        {\n" +
        "            throw new EncodingException(\"Missing Field: BeginString\");\n" +
        "        }\n" +
        "\n" +
        "        return position;\n" +
        "    }\n" +
        "\n" +
        "    // 35=...| + other header fields\n" +
        "    public long startMessage(final MutableAsciiBuffer buffer, final int offset)\n" +
        "    {\n" +
        "        final int start = offset + beginStringLength + 16;\n" +
        "        int position = start;";

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
        "    public long encode(final MutableAsciiBuffer buffer, final int offset)\n" +
        "    {\n" +
        "        final long startMessageResult = header.startMessage(buffer, offset);\n" +
        "        final int bodyStart = Encoder.offset(startMessageResult);\n" +
        "        int position = bodyStart + Encoder.length(startMessageResult);\n" +
        "\n";

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

    static String encoderClassName(final String name)
    {
        return formatClassName(name + "Encoder");
    }

    private final byte[] buffer = new byte[LONGEST_INT_LENGTH + 1];

    private final MutableAsciiBuffer string = new MutableAsciiBuffer(buffer);

    private final String beginString;  // e.g. "FIX.4.4"

    public EncoderGenerator(
        final Dictionary dictionary,
        final String builderPackage,
        final String builderCommonPackage,
        final OutputManager outputManager,
        final Class<?> validationClass,
        final Class<?> rejectUnknownFieldClass,
        final Class<?> rejectUnknownEnumValueClass,
        final String codecRejectUnknownEnumValueEnabled)
    {
        super(dictionary, builderPackage, builderCommonPackage, outputManager, validationClass, rejectUnknownFieldClass,
            rejectUnknownEnumValueClass, false, codecRejectUnknownEnumValueEnabled);

        final Component header = dictionary.header();
        validateHasField(header, BEGIN_STRING);
        validateHasField(header, BODY_LENGTH);

        beginString = dictionary.beginString();
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
                out.append(fileHeader(thisPackage));

                if (REQUIRED_SESSION_CODECS.contains(className))
                {
                    out.append(importFor("uk.co.real_logic.artio.builder.Abstract" + className));
                }

                generateImports(
                    "Encoder",
                    aggregateType,
                    out,
                    DirectBuffer.class,
                    MutableDirectBuffer.class,
                    UnsafeBuffer.class,
                    AsciiSequenceView.class);
                generateAggregateClass(aggregate, aggregateType, className, out);
            });
    }

    protected Class<?> topType(final AggregateType aggregateType)
    {
        return Encoder.class;
    }

    protected String resetGroup(final Entry entry)
    {
        final Group group = (Group)entry.element();
        final String name = group.name();
        final Entry numberField = group.numberField();
        return String.format(
                "    public void %1$s()\n" +
                "    {\n" +
                "        if (%2$s != null)\n" +
                "        {\n" +
                "            %2$s.reset();\n" +
                "        }\n" +
                "        %3$s = 0;\n" +
                "        has%4$s = false;\n" +
                "    }\n\n",
                nameOfResetMethod(name),
                formatPropertyName(name),
                formatPropertyName(numberField.name()),
                numberField.name());
    }

    private void generateAggregateClass(
        final Aggregate aggregate,
        final AggregateType type,
        final String className,
        final Writer out) throws IOException
    {
        final boolean isHeader = type == AggregateType.HEADER;
        final boolean isMessage = type == AggregateType.MESSAGE;
        final List<String> interfaces;
        if (isMessage)
        {
            final String parentName =
                (REQUIRED_SESSION_CODECS.contains(className)) ?
                "Abstract" + className :
                Encoder.class.getSimpleName();
            interfaces = singletonList(parentName);
        }
        else if (isHeader)
        {
            interfaces = singletonList(SessionHeaderEncoder.class.getName());
        }
        else
        {
            interfaces = emptyList();
        }
        out.append(classDeclaration(className, interfaces, type == GROUP));
        out.append(constructor(aggregate, type, dictionary));
        if (isMessage)
        {
            out.append(commonCompoundImports("Encoder", false, ""));
        }
        else if (type == GROUP)
        {
            final Group group = (Group)aggregate;
            out.append(nextMethod(group));
        }
        else if (type == HEADER)
        {
            out.append(
                String.format("\n" +
                "    private static final byte[] DEFAULT_BEGIN_STRING=\"%s\".getBytes(StandardCharsets.US_ASCII);" +
                "\n\n",
                beginString));
        }

        precomputedHeaders(out, aggregate.entries());
        generateSetters(out, className, aggregate.entries());
        out.append(encodeMethod(aggregate.entries(), type));
        out.append(completeResetMethod(aggregate, isMessage, type));
        out.append(generateAppendTo(aggregate, isMessage));
        out.append("}\n");
    }

    private String completeResetMethod(
        final Aggregate aggregate, final boolean isMessage, final AggregateType type)
    {
        final String additionalReset;
        switch (type)
        {
            case GROUP:
                additionalReset = RESET_NEXT_GROUP;
                break;
            case HEADER:
                additionalReset = "        beginString(DEFAULT_BEGIN_STRING);\n";
                break;
            default:
                additionalReset = "";
        }
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

    private String constructor(final Aggregate aggregate, final AggregateType type, final Dictionary dictionary)
    {
        if (type == AggregateType.MESSAGE)
        {
            final Component header = dictionary.header();
            final Message message = (Message)aggregate;
            final long packedType = message.packedType();
            final String fullType = message.fullType();
            final String msgType = header.hasField(MSG_TYPE) ?
                String.format("        header.msgType(\"%s\");\n", fullType) : "";

            return String.format(
                "    public long messageType()\n" +
                "    {\n" +
                "        return %sL;\n" +
                "    }\n\n" +
                "    public %sEncoder()\n" +
                "    {\n" +
                "%s" +
                "    }\n\n",
                packedType,
                aggregate.name(),
                msgType);
        }
        else if (type == AggregateType.HEADER)
        {
            return String.format(
                "    public %sEncoder()\n" +
                "    {\n" +
                "        beginString(DEFAULT_BEGIN_STRING);\n" +
                "    }\n\n",
                aggregate.name());
        }
        else
        {
            return "";
        }
    }

    private void generateSetters(final Writer out, final String className, final List<Entry> entries)
        throws IOException
    {
        final List<String> optionalFields = ENCODER_OPTIONAL_SESSION_FIELDS.get(className);
        final Set<String> missingOptionalFields = (optionalFields == null) ? emptySet() : new HashSet<>(optionalFields);

        for (final Entry entry : entries)
        {
            generateSetter(className, entry, out, missingOptionalFields);
        }

        generateMissingOptionalSessionFields(out, className, missingOptionalFields);
        generateOptionalSessionFieldsSupportedMethods(optionalFields, missingOptionalFields, out);
    }

    private void generateMissingOptionalSessionFields(
        final Writer out, final String className, final Set<String> missingOptionalFields)
        throws IOException
    {
        for (final String optionalField : missingOptionalFields)
        {
            final String propertyName = formatPropertyName(optionalField);

            out.append(String.format(
                "    public %2$s %1$s(final DirectBuffer value, final int offset, final int length)\n" +
                "    {\n" +
                "        throw new UnsupportedOperationException();\n" +
                "    }\n" +
                "\n" +
                "    public %2$s %1$s(final DirectBuffer value, final int length)\n" +
                "    {\n" +
                "        throw new UnsupportedOperationException();\n" +
                "    }\n" +
                "\n" +
                "    public %2$s %1$s(final DirectBuffer value)\n" +
                "    {\n" +
                "        throw new UnsupportedOperationException();\n" +
                "    }\n" +
                "\n" +
                "    public %2$s %1$s(final byte[] value, final int offset, final int length)\n" +
                "    {\n" +
                "        throw new UnsupportedOperationException();\n" +
                "    }\n" +
                "\n" +
                "    public %2$s %1$s(final byte[] value, final int length)\n" +
                "    {\n" +
                "        throw new UnsupportedOperationException();\n" +
                "    }\n" +
                "\n" +
                "    public %2$s %1$s(final byte[] value)\n" +
                "    {\n" +
                "        throw new UnsupportedOperationException();\n" +
                "    }\n" +
                "\n" +
                "    public boolean has%3$s()\n" +
                "    {\n" +
                "        throw new UnsupportedOperationException();\n" +
                "    }\n" +
                "\n" +
                "    public String %1$sAsString()\n" +
                "    {\n" +
                "        throw new UnsupportedOperationException();\n" +
                "    }\n" +
                "\n" +
                "    public %2$s %1$s(final CharSequence value)\n" +
                "    {\n" +
                "        throw new UnsupportedOperationException();\n" +
                "    }\n" +
                "\n" +
                "    public %2$s %1$s(final AsciiSequenceView value)\n" +
                "    {\n" +
                "        throw new UnsupportedOperationException();\n" +
                "    }\n" +
                "\n" +
                "    public %2$s %1$s(final char[] value, final int offset, final int length)\n" +
                "    {\n" +
                "        throw new UnsupportedOperationException();\n" +
                "    }\n" +
                "\n" +
                "    public %2$s %1$s(final char[] value, final int length)\n" +
                "    {\n" +
                "        throw new UnsupportedOperationException();\n" +
                "    }\n" +
                "\n" +
                "    public %2$s %1$s(final char[] value)\n" +
                "    {\n" +
                "        throw new UnsupportedOperationException();\n" +
                "    }\n" +
                "\n" +
                "    public MutableDirectBuffer %1$s()\n" +
                "    {\n" +
                "        throw new UnsupportedOperationException();\n" +
                "    }\n" +
                "\n" +
                "    public void reset%3$s()\n" +
                "    {\n" +
                "        throw new UnsupportedOperationException();\n" +
                "    }",
                propertyName,
                className,
                optionalField));
        }
    }

    private void generateSetter(
        final String className, final Entry entry, final Writer out, final Set<String> optionalFields)
    {
        if (!isBodyLength(entry))
        {
            entry.forEach(
                (field) -> out.append(generateFieldSetter(className, field, optionalFields)),
                (group) -> generateGroup(className, group, out, optionalFields),
                (component) -> generateComponentField(encoderClassName(entry.name()), component, out));
        }
    }

    private String generateFieldSetter(final String className, final Field field, final Set<String> optionalFields)
    {
        final String name = field.name();
        final String fieldName = formatPropertyName(name);
        final String hasField =
            String.format("    private boolean has%1$s;\n\n", name) + hasGetter(name);

        final String hasAssign = String.format("        has%s = true;\n", name);

        final String enumSetter = hasEnumGenerated(field) && !field.type().isMultiValue() ?
            enumSetter(className, fieldName, field.name()) : "";

        final Function<String, String> generateSetter =
            (type) -> generateSetter(name, type, fieldName, hasField, className, hasAssign, enumSetter);

        optionalFields.remove(name);

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
                return decimalFloatSetter(fieldName, hasField, className, hasAssign, enumSetter);

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
                return generateBytesSetter(className, fieldName, name);

            default: throw new UnsupportedOperationException("Unknown type: " + field.type());
        }
    }

    private void generateGroup(
        final String className, final Group group, final Writer out, final Set<String> optionalFields)
        throws IOException
    {
        generateGroupClass(group, out);

        final Entry numberField = group.numberField();
        generateSetter(className, numberField, out, optionalFields);

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

    private String generateBytesSetter(final String className, final String fieldName, final String name)
    {
        return String.format(
            "    private final MutableDirectBuffer %1$s = new UnsafeBuffer();\n\n" +
            "    private int %1$sOffset = 0;\n\n" +
            "    private int %1$sLength = 0;\n\n" +
            "    public %2$s %1$s(final DirectBuffer value, final int offset, final int length)\n" +
            "    {\n" +
            "        %1$s.wrap(value);\n" +
            "        %1$sOffset = offset;\n" +
            "        %1$sLength = length;\n" +
            "        return this;\n" +
            "    }\n\n" +
            "    public %2$s %1$s(final DirectBuffer value, final int length)\n" +
            "    {\n" +
            "        return %1$s(value, 0, length);\n" +
            "    }\n\n" +
            "    public %2$s %1$s(final DirectBuffer value)\n" +
            "    {\n" +
            "        return %1$s(value, 0, value.capacity());\n" +
            "    }\n\n" +
            "    public %2$s %1$s(final byte[] value, final int offset, final int length)\n" +
            "    {\n" +
            "        %1$s.wrap(value);\n" +
            "        %1$sOffset = offset;\n" +
            "        %1$sLength = length;\n" +
            "        return this;\n" +
            "    }\n\n" +
            "    public %2$s %1$s(final byte[] value, final int length)\n" +
            "    {\n" +
            "        return %1$s(value, 0, length);\n" +
            "    }\n\n" +
            "    public %2$s %1$s(final byte[] value)\n" +
            "    {\n" +
            "        return %1$s(value, 0, value.length);\n" +
            "    }\n\n" +
            "    public boolean has%3$s()\n" +
            "    {\n" +
            "        return %1$sLength > 0;\n" +
            "    }\n\n" +
            "    public MutableDirectBuffer %1$s()\n" +
            "    {\n" +
            "        return %1$s;\n" +
            "    }\n\n" +
            "    public String %1$sAsString()\n" +
            "    {\n" +
            "        return %1$s.getStringWithoutLengthAscii(%1$sOffset, %1$sLength);\n" +
            "    }\n\n",
            fieldName,
            className,
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
            "        toBytes(value, %1$s);\n" +
            "        %1$sOffset = 0;\n" +
            "        %1$sLength = value.length();\n" +
            "        return this;\n" +
            "    }\n\n" +
            "    public %3$s %1$s(final AsciiSequenceView value)\n" +
            "    {\n" +
            "        final DirectBuffer buffer = value.buffer();\n" +
            "        if (buffer != null)\n" +
            "        {\n" +
            "            %1$s.wrap(buffer);\n" +
            "            %1$sOffset = value.offset();\n" +
            "            %1$sLength = value.length();\n" +
            "        }\n" +
            "        return this;\n" +
            "    }\n\n" +
            "    public %3$s %1$s(final char[] value)\n" +
            "    {\n" +
            "        return %1$s(value, 0, value.length);\n" +
            "    }\n\n" +
            "    public %3$s %1$s(final char[] value, final int length)\n" +
            "    {\n" +
                "        return %1$s(value, 0, length);\n" +
            "    }\n\n" +
            "    public %3$s %1$s(final char[] value, final int offset, final int length)\n" +
            "    {\n" +
            "        toBytes(value, %1$s, offset, length);\n" +
            "        %1$sOffset = 0;\n" +
            "        %1$sLength = length;\n" +
            "        return this;\n" +
            "    }\n\n" +
            "%4$s",
            fieldName,
            generateBytesSetter(className, fieldName, name),
            className,
            enumSetter);
    }

    private String generateSetter(
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

    private String decimalFloatSetter(
        final String fieldName,
        final String optionalField,
        final String className,
        final String optionalAssign,
        final String enumSetter)
    {
        return String.format(
            "    private final DecimalFloat %1$s = new DecimalFloat();\n\n" +
            "%2$s" +
            "    public %3$s %1$s(DecimalFloat value)\n" +
            "    {\n" +
            "        %1$s.set(value);\n" +
            "%4$s" +
            "        return this;\n" +
            "    }\n\n" +
            "    public %3$s %1$s(long value, int scale)\n" +
            "    {\n" +
            "        %1$s.set(value, scale);\n" +
            "%4$s" +
            "        return this;\n" +
            "    }\n\n" +
            "    public DecimalFloat %1$s()\n" +
            "    {\n" +
            "        return %1$s;\n" +
            "    }\n\n" +
            "%5$s",
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
            "    public %1$s %2$s(%3$s value)\n" +
            "    {\n" +
            "        if (" + CODEC_VALIDATION_ENABLED + ")\n" +
            "        {\n" +
            "            if (value == %3$s.ARTIO_UNKNOWN)\n" +
            "            {\n" +
            "                throw new EncodingException(\"Invalid Value Field: " + fieldName +
            " Value: \" + value );\n" +
            "            }\n" +
            "            if (value == %3$s.NULL_VAL)\n" +
            "            {\n" +
            "                return this;\n" +
            "            }\n" +
            "        }\n" +
            "        return %2$s(value.representation());\n" +
            "    }\n\n",
            className, fieldName, enumType
        );
    }

    private String encodeMethod(final List<Entry> entries, final AggregateType aggregateType)
    {
        final String prefix;
        switch (aggregateType)
        {
            case TRAILER:
                prefix = TRAILER_ENCODE_PREFIX;
                break;

            case GROUP:
                prefix = GROUP_ENCODE_PREFIX;
                break;

            case MESSAGE:
                prefix = MESSAGE_ENCODE_PREFIX;
                break;

            case HEADER:
                prefix = HEADER_ENCODE_PREFIX;
                break;

            default:
                prefix = OTHER_ENCODE_PREFIX;
                break;
        }

        final String body = entries.stream()
            .map(this::encodeEntry)
            .collect(joining("\n"));

        String suffix;
        if (aggregateType == AggregateType.MESSAGE)
        {
            suffix =
                "        position += trailer.startTrailer(buffer, position);\n" +
                "\n" +
                "        final int messageStart = header.finishHeader(buffer, bodyStart, position - bodyStart);\n" +
                "        return trailer.finishMessage(buffer, messageStart, position);\n" +
                "    }\n\n";
        }
        else if (aggregateType == AggregateType.HEADER)
        {
            suffix =
                "\n" +
                "        return Encoder.result(position - start, start);\n" +
                "    }\n\n";
        }
        else if (aggregateType == AggregateType.TRAILER)
        {
            suffix =
                "        return position - start;\n" +
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
        if (isBodyLength(entry) || isBeginString(entry) || isCheckSum(entry))
        {
            return "";
        }
        else
        {
            return entry.matchEntry(this::encodeField, this::encodeGroup, this::encodeComponent);
        }
    }

    private String encodeField(final Entry entry)
    {
        final Element element = entry.element();
        final Field field = (Field)element;
        final String name = field.name();
        final String fieldName = formatPropertyName(name);
        final Field.Type type = field.type();
        final boolean mustCheckFlag = hasFlag(entry, field);
        final boolean mustCheckLength = type.hasLengthField(false);
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
                return encodeStringField(fieldName, enablingSuffix, tag);

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

    private String encodeStringField(final String fieldName, final String optionalSuffix, final String tag)
    {
        return formatEncoder(fieldName, optionalSuffix, tag,
        "        buffer.putBytes(position, %s, %2$sOffset, %2$sLength);\n" +
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
            "        position += buffer.put%sAscii(position, %s);\n" +
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
                precomputedFieldHeader(out, (Field)element);
            }
            else if (element instanceof Group)
            {
                final Group group = (Group)element;
                precomputedFieldHeader(out, (Field)group.numberField().element());
            }
        }
    }

    private void precomputedFieldHeader(final Writer out, final Field field) throws IOException
    {
        final String name = field.name();
        final String fieldName = formatPropertyName(name);
        final int length = string.putIntAscii(0, field.number());
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

    protected String stringAppendTo(final String fieldName)
    {
        return String.format("appendBuffer(builder, %1$s, %1$sOffset, %1$sLength)", fieldName);
    }

    protected String timeAppendTo(final String fieldName)
    {
        return stringAppendTo(fieldName);
    }

    protected String dataAppendTo(final Field field, final String fieldName)
    {
        final String lengthName = formatPropertyName(field.associatedLengthField().name());
        return String.format("appendData(builder, %1$s, %2$s)", fieldName, lengthName);
    }

    protected String componentAppendTo(final Component component)
    {
        final String name = component.name();
        return String.format(
            "    indent(builder, level);\n" +
            "    builder.append(\"\\\"%1$s\\\": \");\n" +
            "    %2$s.appendTo(builder, level + 1);\n" +
            "    builder.append(\"\\n\");\n",
            name,
            formatPropertyName(name));
    }

    private void generateComponentField(
        final String className, final Component element, final Writer out)
        throws IOException
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

    protected boolean hasFlag(final Entry entry, final Field field)
    {
        final Type type = field.type();
        return (!entry.required() && !type.hasLengthField(false)) ||
            type.isFloatBased() || type.isIntBased() || type.isCharBased();
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

    protected String resetStringBasedData(final String name)
    {
        return resetLength(name);
    }

    protected String groupEntryAppendTo(final Group group, final String name)
    {
        final Entry numberField = group.numberField();

        return String.format(
            "    if (has%2$s)\n" +
            "    {\n" +
            "    indent(builder, level);\n" +
            "    builder.append(\"\\\"%1$s\\\": [\\n\");\n" +
            "    final int %3$s = this.%3$s;\n" +
            "    %5$s %4$s = this.%4$s;\n" +
            "    for (int i = 0; i < %3$s; i++)\n" +
            "    {\n" +
            "        indent(builder, level);\n" +
            "        %4$s.appendTo(builder, level + 1);" +
            "        if (i < (%3$s - 1))\n" +
            "        {\n" +
            "            builder.append(',');\n" +
            "        }\n" +
            "        builder.append('\\n');\n" +
            "        %4$s = %4$s.next();\n" +
            "    }\n" +
            "    indent(builder, level);\n" +
            "    builder.append(\"],\\n\");\n" +
            "    }\n",
            name,
            group.numberField().name(),
            formatPropertyName(numberField.name()),
            formatPropertyName(name),
            encoderClassName(name));
    }

    protected String optionalReset(final Field field, final String name)
    {
        return field.type().hasLengthField(false) ? resetLength(name) : resetByFlag(name);
    }

    protected boolean appendToChecksHasGetter(final Entry entry, final Field field)
    {
        return hasFlag(entry, field) || field.type().hasLengthField(false);
    }
}
