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

import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.agrona.generation.StringWriterOutputManager;
import uk.co.real_logic.fix_gateway.dictionary.ir.*;
import uk.co.real_logic.fix_gateway.fields.DecimalFloat;
import uk.co.real_logic.fix_gateway.fields.LocalMktDateEncoder;
import uk.co.real_logic.fix_gateway.fields.UtcTimestampEncoder;
import uk.co.real_logic.fix_gateway.util.MutableAsciiFlyweight;
import uk.co.real_logic.sbe.generation.java.JavaUtil;

import java.io.IOException;
import java.io.Writer;
import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.joining;
import static uk.co.real_logic.fix_gateway.dictionary.generation.GenerationUtil.*;
import static uk.co.real_logic.fix_gateway.util.MutableAsciiFlyweight.LONGEST_INT_LENGTH;

public class EncoderGenerator
{
    private static final String SUFFIX =
        "        buffer.putSeparator(position);\n" +
        "        position++;\n" +
        "%s";

    private static final String COMMON_COMPOUNDS =
        "    private Header header = new Header();\n\n" +
        "    public Header header() {\n" +
        "        return header;\n" +
        "    }\n\n" +

        "    private Trailer trailer = new Trailer();\n\n" +
        "    public Trailer trailer() {\n" +
        "        return trailer;\n" +
        "    }\n\n";

    private static final String COMMON_COMPOUND_IMPORTS =
        "import %1$s.Header;\n" +
        "import %1$s.Trailer;\n";

    private final byte[] buffer = new byte[LONGEST_INT_LENGTH + 1];
    private final MutableAsciiFlyweight string = new MutableAsciiFlyweight(new UnsafeBuffer(buffer));

    private final int initialArraySize;
    private final DataDictionary dictionary;
    private final StringWriterOutputManager outputManager;

    public EncoderGenerator(
        final DataDictionary dictionary,
        final int initialArraySize,
        final StringWriterOutputManager outputManager)
    {
        this.dictionary = dictionary;
        this.initialArraySize = initialArraySize;
        this.outputManager = outputManager;
    }

    public void generate()
    {
        generateAggregate(dictionary.header(), false);
        generateAggregate(dictionary.trailer(), false);

        dictionary.messages()
                  .forEach(msg -> generateAggregate(msg, true));
    }

    private void generateAggregate(final Aggregate message, final boolean hasCommonCompounds)
    {
        final String className = message.name();

        try (final Writer out = outputManager.createOutput(className))
        {
            out.append(fileHeader(BUILDER_PACKAGE));
            out.append(generateClassDeclaration(className, hasCommonCompounds));
            if (hasCommonCompounds)
            {
                out.append(COMMON_COMPOUNDS);
            }
            generatePrecomputedHeaders(out, message.entries());
            generateSetters(out, className, message.entries());
            out.append(generateEncodeMethod(message.entries(), hasCommonCompounds));
            out.append(generateResetMethod(message.entries()));
            out.append("}\n");
        }
        catch (IOException e)
        {
            // TODO: logging
            e.printStackTrace();
        }
    }

    private String generateResetMethod(List<Entry> entries)
    {
        return "    public void reset() {\n" +
               "    }\n\n";
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

        String optionalField;
        String optionalAssign;

        if (entry.required())
        {
            optionalField = "";
            optionalAssign = "";
        }
        else
        {
            optionalField = String.format("    private boolean has%s;\n\n", name);
            optionalAssign = String.format("        has%s = true;\n", name);
        }

        Function<String, String> generateSetter =
            type -> generateSetter(type, fieldName, optionalField, className, optionalAssign);

        switch (field.type())
        {
            // TODO: other type cases
            // TODO: how do we reset optional fields - clear method?
            case STRING:
                return generateStringSetter(className, fieldName, optionalField, optionalAssign);

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
            "    }\n" +
            "\n" +
            "    public %4$s %1$s(char[] value)\n" +
            "    {\n" +
            "        %1$s = toBytes(value, %1$s);\n" +
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
        final String type,
        final String fieldName,
        final String optionalField,
        final String className,
        final String optionalAssign)
    {
        return String.format(
            "    private %s %s;\n\n" +
            "%s" +
            "    public %s %2$s(%1$s value)\n" +
            "    {\n" +
            "        %2$s = value;\n" +
            "%s" +
            "        return this;\n" +
            "    }\n\n",
            type,
            fieldName,
            optionalField,
            className,
            optionalAssign);
    }

    private String generateEncodeMethod(final List<Entry> entries, final boolean hasCommonCompounds)
    {
        String header =
            "    public int encode(final MutableAsciiFlyweight buffer, final int offset)\n" +
            "    {\n"+
            "        int position = offset;\n\n" +
            (hasCommonCompounds ? "        position += header.encode(buffer, position);\n" : "");

        String body = entries.stream()
                             .map(this::encodeField)
                             .collect(joining("\n"));

        String footer =
            (hasCommonCompounds ? "        position += trailer.encode(buffer, position);\n" : "") +
            "        return position - offset;\n" +
            "    }\n\n";

        return header + body + footer;
    }

    private String encodeField(final Entry entry)
    {
        final Field field = (Field) entry.element();
        final String name = field.name();
        final String fieldName = JavaUtil.formatPropertyName(name);

        final String optionalPrefix = entry.required() ? "" : String.format("        if (has%s) {\n", name);
        final String optionalSuffix = entry.required() ? "" : "}\n";

        final String tag = String.format(
            "%s" +
            "        buffer.putBytes(position, %sHeader, 0, %2$sHeaderLength);\n" +
            "        position += %2$sHeaderLength;\n",
            optionalPrefix,
            fieldName);

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

    private String generateClassDeclaration(final String className, final boolean hasCommonCompounds)
    {

        return String.format(
            importFor(MutableDirectBuffer.class) +
            "import static uk.co.real_logic.fix_gateway.dictionary.generation.EncodingUtil.*;\n" +
            "import %s.Encoder;\n" +
            (hasCommonCompounds ? COMMON_COMPOUND_IMPORTS : "") +
            importFor(DecimalFloat.class) +
            importFor(MutableAsciiFlyweight.class) +
            importFor(LocalMktDateEncoder.class) +
            importFor(UtcTimestampEncoder.class) +
            "\n" +
            "public final class %s implements Encoder\n" +
            "{\n\n",
            BUILDER_PACKAGE,
            className);
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

}
