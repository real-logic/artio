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

import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.agrona.generation.OutputManager;
import uk.co.real_logic.fix_gateway.builder.Encoder;
import uk.co.real_logic.fix_gateway.builder.MessageEncoder;
import uk.co.real_logic.fix_gateway.dictionary.ir.*;
import uk.co.real_logic.fix_gateway.util.MutableAsciiFlyweight;
import uk.co.real_logic.sbe.generation.java.JavaUtil;

import java.io.IOException;
import java.io.Writer;
import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.joining;
import static uk.co.real_logic.fix_gateway.dictionary.generation.GenerationUtil.fileHeader;
import static uk.co.real_logic.fix_gateway.util.MutableAsciiFlyweight.LONGEST_INT_LENGTH;

public class EncoderGenerator extends Generator
{

    private static final String SUFFIX =
        "        buffer.putSeparator(position);\n" +
        "        position++;\n" +
        "%s";

    private static final String TRAILER_PREFIX =
        "    public int encode(final MutableAsciiFlyweight buffer, final int offset)\n" +
        "    {\n"+
        "        throw new UnsupportedOperationException();\n" +
        "    }\n\n" +
        "    public int encode(final MutableAsciiFlyweight buffer, final int offset, final int bodyStart)\n" +
        "    {\n"+
        "        int position = offset;\n\n";

    private final byte[] buffer = new byte[LONGEST_INT_LENGTH + 1];
    private final MutableAsciiFlyweight string = new MutableAsciiFlyweight(new UnsafeBuffer(buffer));

    private final int initialArraySize;

    public EncoderGenerator(
        final DataDictionary dictionary,
        final int initialArraySize,
        final String builderPackage,
        final OutputManager outputManager)
    {
        super(dictionary, builderPackage, outputManager);
        this.initialArraySize = initialArraySize;
    }

    protected void generateAggregate(final Aggregate aggregate, final AggregateType aggregateType)
    {
        final String className = aggregate.name() + "Encoder";
        final boolean isMessage = aggregateType == AggregateType.MESSAGE;

        try (final Writer out = outputManager.createOutput(className))
        {
            out.append(fileHeader(builderPackage));
            Class<?> type = isMessage ? MessageEncoder.class : Encoder.class;
            out.append(generateClassDeclaration(className, isMessage, type, Encoder.class));
            out.append(generateConstructor(aggregate, dictionary));
            if (isMessage)
            {
                out.append(commonCompoundImports("Encoder"));
            }
            generatePrecomputedHeaders(out, aggregate.entries());
            generateSetters(out, className, aggregate.entries());
            out.append(generateEncodeMethod(aggregate.entries(), aggregateType));
            out.append(generateResetMethod(aggregate.entries()));
            out.append(generateToString(aggregate, isMessage));
            out.append("}\n");
        }
        catch (IOException e)
        {
            // TODO: logging
            e.printStackTrace();
        }
    }

    private String generateConstructor(final Aggregate aggregate, final DataDictionary dictionary)
    {
        if (!(aggregate instanceof Message))
        {
            return "";
        }

        final Component header = dictionary.header();
        final Message message = (Message) aggregate;
        final int type = message.type();
        final String msgType = header.hasField(MSG_TYPE)
                             ? String.format("        header.msgType(\"%s\");\n", (char) type) : "";

        final String beginString = header.hasField("BeginString")
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
            type,
            message.name(),
            msgType,
            beginString
        );
    }

    private void generateSetters(final Writer out, final String className, final List<Entry> entries) throws IOException
    {
        for (Entry entry : entries)
        {
            out.append(generateSetter(className, entry));
        }
    }

    private String generateSetter(final String className, final Entry entry)
    {
        final Field field = (Field) entry.element();
        final String name = field.name();
        final String fieldName = JavaUtil.formatPropertyName(name);
        final String optionalField = optionalField(entry);
        final String optionalAssign = optionalAssign(entry);

        // TODO: make encoding generation more regular and delegate to library calls more
        Function<String, String> generateSetter =
            type -> generateSetter(name, type, fieldName, optionalField, className, optionalAssign);

        switch (field.type())
        {
            // TODO: other type cases
            // TODO: how do we reset optional fields - clear method?
            case STRING:
                return generateStringSetter(className, fieldName, optionalField, optionalAssign);

            case BOOLEAN:
                return generateSetter.apply("boolean");

            case DATA:
                return generateSetter.apply("byte[]");

            case INT:
            case LENGTH:
            case SEQNUM:
            case LOCALMKTDATE:
                return generateSetter.apply("int");

            case UTCTIMESTAMP:
                return generateSetter.apply("long");

            case QTY:
            case PRICE:
            case PRICEOFFSET:
                return generateSetter.apply("DecimalFloat");

            default: throw new UnsupportedOperationException("Unknown type: " + field.type());
        }
    }

    private boolean isBodyLength(final String name)
    {
        return "BodyLength".equals(name);
    }

    private String generateStringSetter(
        final String className,
        final String fieldName,
        final String optionalField,
        final String optionalAssign)
    {
        return String.format(
            "    private byte[] %s = new byte[%d];\n\n" +
                "    private int %1$sLength = 0;\n\n" +
                "%s" +
                "    public %s %1$s(CharSequence value)\n" +
                "    {\n" +
                "        %1$s = toBytes(value, %1$s);\n" +
                "        %1$sLength = value.length();\n" +
                "%s" +
                "        return this;\n" +
                "    }\n\n" +
                "    public %4$s %1$s(char[] value)\n" +
                "    {\n" +
                "        %1$s = toBytes(value, %1$s);\n" +
                "        %1$sLength = value.length;\n" +
                "%5$s" +
                "        return this;\n" +
                "    }\n\n" +
                "    public %4$s %1$s(byte[] value)\n" +
                "    {\n" +
                "        %1$s = value;\n" +
                "        %1$sLength = value.length;\n" +
                "%5$s" +
                "        return this;\n" +
                "    }\n\n",
            fieldName,
            initialArraySize,
            optionalField,
            className,
            optionalAssign);
    }

    private String generateSetter(
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

    private String generateEncodeMethod(final List<Entry> entries, final AggregateType aggregateType)
    {
        final boolean hasCommonCompounds = aggregateType == AggregateType.MESSAGE;

        final String prefix =
            aggregateType == AggregateType.TRAILER ?
            TRAILER_PREFIX :
            ("    public int encode(final MutableAsciiFlyweight buffer, final int offset)\n" +
            "    {\n"+
            "        int position = offset;\n\n" +
            (hasCommonCompounds ? "        position += header.encode(buffer, position);\n" : ""));

        final String body =
            entries.stream()
                   .map(this::encodeField)
                   .collect(joining("\n"));

        final String suffix =
            (hasCommonCompounds ? "        position += trailer.encode(buffer, position, header.bodyLength);\n" : "") +
            "        return position - offset;\n" +
            "    }\n\n";

        return prefix + body + suffix;
    }

    private String encodeField(final Entry entry)
    {
        if (isBodyLength(entry))
        {
            return "        position += buffer.putBytes(position, BODY_LENGTH);\n" +
                   "        bodyLength(position);\n";
        }

        if (isCheckSum(entry))
        {
            return "        final int bodyLength = position - bodyStart;\n" +
                   "        buffer.putNatural(bodyStart - BODY_LENGTH_SIZE, BODY_LENGTH_GAP, bodyLength);\n" +
                   formatTag("checkSum", "") +
                   // 17 to account for the common sized prefix size before bodyStart.
                   // position - 2, to get back to the point before the checksum
                   "        final int checkSum = buffer.computeChecksum(bodyStart - 17, position - 3);\n" +
                   "        buffer.putNatural(position, 3, checkSum);\n" +
                   "        position += 3;" +
                   "        buffer.putSeparator(position);\n" +
                   "        position++;\n";
        }

        return encodeRegularField(entry);
    }

    private boolean isCheckSum(final Entry entry)
    {
        return "CheckSum".equals(entry.name());
    }

    private boolean isBodyLength(final Entry entry)
    {
        return isBodyLength(entry.name());
    }

    private String encodeRegularField(final Entry entry)
    {
        final Field field = (Field) entry.element();
        final String name = field.name();
        final String fieldName = JavaUtil.formatPropertyName(name);

        final String optionalPrefix = entry.required() ? "" : String.format("        if (has%s) {\n", name);
        final String optionalSuffix = entry.required() ? "" : "        }\n";

        final String tag = formatTag(fieldName, optionalPrefix);

        switch (field.type())
        {
            case STRING:
                return String.format(
                    "%s" +
                    "        buffer.putBytes(position, %s, 0, %2$sLength);\n" +
                    "        position += %2$sLength;\n" +
                    SUFFIX,
                    tag,
                    fieldName,
                    optionalSuffix);

            case BOOLEAN:
                return generatePut(fieldName, tag, "Boolean", optionalSuffix);

            case DATA:
                return generatePut(fieldName, tag, "Bytes", optionalSuffix);

            case INT:
            case LENGTH:
            case SEQNUM:
                return generatePut(fieldName, tag, "Int", optionalSuffix);

            case QTY:
            case PRICE:
            case PRICEOFFSET:
                return generatePut(fieldName, tag, "Float", optionalSuffix);

            case LOCALMKTDATE:
                return String.format(
                    "%s" +
                    "        position += LocalMktDateEncoder.encode(%s, buffer, position);\n" +
                    SUFFIX,
                    tag,
                    fieldName,
                    optionalSuffix);

            case UTCTIMESTAMP:
                return String.format(
                    "%s" +
                    "        position += UtcTimestampEncoder.encode(%s, buffer, position);\n" +
                    SUFFIX,
                    tag,
                    fieldName,
                    optionalSuffix);

            default: throw new UnsupportedOperationException("Unknown type: " + field.type());
        }
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

    private String generatePut(final String fieldName, final String tag, final String type, String optionalSuffix)
    {
        return String.format(
            "%s" +
            "        position += buffer.put%s(position, %s);\n" +
            SUFFIX,
            tag,
            type,
            fieldName,
            optionalSuffix);
    }

    private void generatePrecomputedHeaders(
        final Writer out,
        final List<Entry> entries) throws IOException
    {
        for (Entry entry : entries)
        {
            final Field field = (Field) entry.element();
            final String name = field.name();
            final String fieldName = JavaUtil.formatPropertyName(name);
            // TODO: tags aren't always ints
            final int length = string.putInt(0, field.number());
            final String bytes = IntStream.range(0, length)
                                          .mapToObj(i -> String.valueOf(buffer[i]))
                                          .collect(joining(", ", "", ", (byte) '='"));

            out.append(String.format(
                "    private static final int %sHeaderLength = %d;\n" +
                "    private static final byte[] %1$sHeader = new byte[] {%s};\n\n",
                fieldName,
                length + 1,
                bytes));
        }
    }

    private String optionalAssign(final Entry entry)
    {
        return entry.required() ? "" : String.format("        has%s = true;\n", entry.name());
    }

    protected String generateStringToString(String fieldName)
    {
        return String.format("new String(%s, 0, %1$sLength, StandardCharsets.US_ASCII)", fieldName);
    }

}
