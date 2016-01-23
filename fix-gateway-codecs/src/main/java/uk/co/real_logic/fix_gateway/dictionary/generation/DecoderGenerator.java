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

import uk.co.real_logic.agrona.LangUtil;
import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.generation.OutputManager;
import uk.co.real_logic.fix_gateway.builder.Decoder;
import uk.co.real_logic.fix_gateway.dictionary.StandardFixConstants;
import uk.co.real_logic.fix_gateway.dictionary.ir.*;
import uk.co.real_logic.fix_gateway.dictionary.ir.Field.Type;
import uk.co.real_logic.fix_gateway.dictionary.ir.Field.Value;
import uk.co.real_logic.fix_gateway.fields.*;
import uk.co.real_logic.fix_gateway.util.AsciiBuffer;
import uk.co.real_logic.fix_gateway.util.MutableAsciiBuffer;

import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static uk.co.real_logic.fix_gateway.dictionary.generation.AggregateType.*;
import static uk.co.real_logic.fix_gateway.dictionary.generation.ConstantGenerator.generateFieldDictionary;
import static uk.co.real_logic.fix_gateway.dictionary.generation.ConstantGenerator.sizeHashSet;
import static uk.co.real_logic.fix_gateway.dictionary.generation.GenerationUtil.*;
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

    public static final String REQUIRED_FIELDS = "REQUIRED_FIELDS";
    public static final String GROUP_FIELDS = "GROUP_FIELDS";

    public static final int INVALID_TAG_NUMBER = 0;
    public static final int REQUIRED_TAG_MISSING = 1;
    public static final int TAG_NOT_DEFINED_FOR_THIS_MESSAGE_TYPE = 2;
    public static final int TAG_SPECIFIED_WITHOUT_A_VALUE = 4;
    public static final int VALUE_IS_INCORRECT = 5;
    public static final int TAG_APPEARS_MORE_THAN_ONCE = 13;
    public static final int TAG_SPECIFIED_OUT_OF_REQUIRED_ORDER = 14;

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
        if (type == COMPONENT)
        {
            generateComponentInterface((Component) aggregate);
            return;
        }

        final String className = decoderClassName(aggregate);
        final boolean isMessage = type == MESSAGE;

        outputManager.withOutput(className, out ->
        {
            out.append(fileHeader(builderPackage));

            final List<String> interfaces =
                aggregate.entriesWith(element -> element instanceof Component)
                         .map(comp -> decoderClassName((Aggregate) comp.element()))
                         .collect(toList());

            out.append(generateClassDeclaration(className, type, interfaces, "Decoder", Decoder.class));
            generateValidation(out, aggregate, type);
            if (isMessage)
            {
                final Message message = (Message)aggregate;
                out.append(generateMessageType(message.fullType(), message.packedType()));
                out.append(commonCompoundImports("Decoder"));
            }
            generateGroupMethods(out, aggregate);
            generateGetters(out, className, aggregate.entries());
            out.append(generateDecodeMethod(aggregate.entries(), aggregate, type));
            out.append(generateResetMethods(isMessage, aggregate.entries(), resetValidation()));
            out.append(generateToString(aggregate, isMessage));
            out.append("}\n");
        });
    }

    private String resetValidation()
    {
        return
            "        invalidTagId = NO_ERROR;\n" +
            "        rejectReason = NO_ERROR;\n" +
            "        missingRequiredFields.clear();\n" +
            "        unknownFields.clear();\n" +
            "        alreadyVisitedFields.clear();\n";
    }

    private void generateValidation(final Writer out, final Aggregate aggregate, final AggregateType type) throws IOException
    {
        final List<Field> requiredFields = requiredFields(aggregate.entries()).collect(toList());
        out.append(generateFieldDictionary(requiredFields, REQUIRED_FIELDS));

        if (aggregate.containsGroup())
        {
            final List<Field> groupFields = aggregate.allGroupFields().collect(toList());
            final String groupFieldString = generateFieldDictionary(groupFields, GROUP_FIELDS);
            out.append(groupFieldString);
        }

        final String enumValidation =
            aggregate.allChildEntries()
                     .filter(entry -> entry.element().isEnumField())
                     .map((entry) -> validateEnum(entry, out))
                     .collect(joining("\n"));

        final boolean isMessage = type == MESSAGE;
        final String messageValidation = isMessage ?
            "        else if (unknownFieldsIterator.hasNext())\n" +
            "        {\n" +
            "            invalidTagId = unknownFieldsIterator.nextValue();\n" +
            "            rejectReason = Constants.ALL_FIELDS.contains(invalidTagId) ? " +
            TAG_NOT_DEFINED_FOR_THIS_MESSAGE_TYPE + " : " + INVALID_TAG_NUMBER + ";\n" +
            "            return false;\n" +
            "        }\n" +
            "        else if (!header.validate())\n" +
            "        {\n" +
            "            invalidTagId = header.invalidTagId();\n" +
            "            rejectReason = header.rejectReason();\n" +
            "            return false;\n" +
            "        }\n" +
            "        else if (!trailer.validate())\n" +
            "        {\n" +
            "            invalidTagId = trailer.invalidTagId();\n" +
            "            rejectReason = trailer.rejectReason();\n" +
            "            return false;\n" +
            "        }\n"
            : "";

        out.append(String.format(
            "    private IntHashSet alreadyVisitedFields = new IntHashSet(%4$d, -1);\n\n" +
            "    private IntHashSet missingRequiredFields = new IntHashSet(%1$d, -1);\n\n" +
            "    private IntHashSet unknownFields = new IntHashSet(10, -1);\n\n" +
            "    private int invalidTagId = NO_ERROR;\n\n" +
            "    public int invalidTagId()\n" +
            "    {\n" +
            "        return invalidTagId;\n" +
            "    }\n\n" +
            "    private int rejectReason = NO_ERROR;\n\n" +
            "    public int rejectReason()\n" +
            "    {\n" +
            "        return rejectReason;\n" +
            "    }\n\n" +
            "    public boolean validate()\n" +
            "    {\n" +
            // validation for some tags performed in the decode method
            "        if (rejectReason != NO_ERROR)\n" +
            "        {\n" +
            "            return false;\n" +
            "        }\n" +
            "        final IntIterator missingFieldsIterator = missingRequiredFields.iterator();\n" +
            (isMessage ? "        final IntIterator unknownFieldsIterator = unknownFields.iterator();\n" : "") +
            "        if (missingFieldsIterator.hasNext())\n" +
            "        {\n" +
            "            invalidTagId = missingFieldsIterator.nextValue();\n" +
            "            rejectReason = " + REQUIRED_TAG_MISSING + ";\n" +
            "            return false;\n" +
            "        }\n" +
            "%2$s" +
            "%3$s" +
            "        return true;\n" +
            "    }\n\n",
            sizeHashSet(requiredFields),
            messageValidation,
            enumValidation,
            2 * aggregate.allChildEntries().count()));
    }

    private CharSequence validateEnum(final Entry entry, final Writer out)
    {
        final Field field = (Field) entry.element();
        final String name = entry.name();
        final String valuesField = "valuesOf" + entry.name();
        final String optionalCheck = entry.required() ? "" : String.format("has%s && ", name);
        final int tagNumber = field.number();
        final Type type = field.type();
        final String propertyName = formatPropertyName(name);

        final boolean isChar = type == Type.CHAR;
        final boolean isPrimitive = type.isIntBased() || isChar;
        try
        {
            if (isPrimitive)
            {

                final String addValues =
                    field.values()
                        .stream()
                        .map(Value::representation)
                        .map(repr -> isChar ? "'" + repr + "'" : repr)
                        .map(repr -> String.format("        %1$s.add(%2$s);\n", valuesField, repr))
                        .collect(joining());

                out.append(String.format(
                    "    public static final IntHashSet %1$s = new IntHashSet(%3$s, -1);\n" +
                    "    static \n" +
                    "    {\n" +
                    "%2$s" +
                    "    }\n\n",
                    valuesField,
                    addValues,
                    sizeHashSet(field.values())
                ));
            }
            else if (type.isStringBased())
            {
                final String addValues =
                    field.values()
                        .stream()
                        .map(value -> "\"" + value.representation() + '"')
                        .collect(joining(", "));

                out.append(String.format(
                    "    public static final CharArraySet %1$s = new CharArraySet(%2$s);\n", valuesField, addValues));

            }
            else
            {
                return "";
            }
        }
        catch (IOException e)
        {
            LangUtil.rethrowUnchecked(e);
        }

        return String.format(
            "        if (%1$s!%2$s.contains(%3$s%5$s))\n" +
            "        {\n" +
            "            invalidTagId = %4$s;\n" +
            "            rejectReason = " + VALUE_IS_INCORRECT + ";\n" +
            "            return false;\n" +
            "        }\n",
            optionalCheck,
            valuesField,
            propertyName,
            tagNumber,
            isPrimitive ? "" : ", " + propertyName + "Length"
        );
    }

    private Stream<Field> requiredFields(final List<Entry> entries)
    {
        return entries
            .stream()
            .filter(Entry::required)
            .flatMap(this::extractFields);
    }

    private Stream<Field> extractFields(final Entry entry)
    {
        return entry.match(
            (e, field) -> Stream.of(field),
            (e, group) -> Stream.of((Field) group.numberField().element()),
            (e, component) -> requiredFields(component.entries())
        );
    }

    private void generateComponentInterface(final Component component)
    {
        final String className = decoderClassName(component);
        outputManager.withOutput(className, out ->
        {
            out.append(fileHeader(builderPackage));

            out.append(String.format(
                importFor(MutableDirectBuffer.class) +
                importStaticFor(CodecUtil.class) +
                importStaticFor(StandardFixConstants.class) +
                importFor(DecimalFloat.class) +
                importFor(MutableAsciiBuffer.class) +
                importFor(AsciiBuffer.class) +
                importFor(LocalMktDateEncoder.class) +
                importFor(UtcTimestampEncoder.class) +
                importFor(StandardCharsets.class) +
                "\npublic interface %1$s\n" +
                "{\n\n",
                className));

            for (final Entry entry : component.entries())
            {
                out.append(generateInterfaceGetter(entry));
            }
            out.append("\n}\n");
        });
    }

    private String generateInterfaceGetter(final Entry entry)
    {
        return entry.match(
            this::generateFieldInterfaceGetter,
            (e, group) -> generateGroupInterfaceGetter(group),
            (e, component) -> generateComponentInterfaceGetter(component));
    }

    private String generateComponentInterfaceGetter(Component component)
    {
        return component.entries()
                        .stream()
                        .map(this::generateInterfaceGetter)
                        .collect(joining("\n", "", "\n"));
    }

    private String generateGroupInterfaceGetter(Group group)
    {
        return String.format(
            "    public %1$s %2$s();\n",
            decoderClassName(group),
            formatPropertyName(group.name())
        );
    }

    private String generateFieldInterfaceGetter(Entry entry, Field field)
    {
        final String name = field.name();
        final String fieldName = formatPropertyName(name);
        final Type type = field.type();

        final String length = type.isStringBased()
                            ? String.format("    public int %1$sLength();\n", fieldName) : "";

        final String optional = !entry.required()
                              ? String.format("    public boolean has%1$s();\n", name) : "";

        return String.format(
                "    public %1$s %2$s();\n" +
                "%3$s" +
                "%4$s",
            javaTypeOf(type),
            fieldName,
            optional,
            length
        );
    }

    private String generateGetter(final Entry entry)
    {
        return entry.match(
            this::generateFieldGetter,
            (e, group) -> generateGroupGetter(group),
            (e, component) -> generateComponentField(component));
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

    private String generateMessageType(final String fullType, final int packedType)
    {
        return String.format(
            "    public static final int MESSAGE_TYPE = %1$d;\n\n" +
            "    public static final byte[] MESSAGE_TYPE_BYTES = \"%2$s\".getBytes(US_ASCII);\n\n",
            packedType,
            fullType);
    }

    private void generateGetters(final Writer out, final String className, final List<Entry> entries) throws IOException
    {
        for (final Entry entry : entries)
        {
            out.append(generateGetter(entry));
        }
    }

    private String generateComponentField(final Component component)
    {
        return component.entries()
                        .stream()
                        .map(this::generateGetter)
                        .collect(joining("\n", "", "\n"));
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

        final String asStringBody = String.format(
            entry.required() ?
                "new String(%1$s, 0, %1$sLength)" :
                "has%2$s ? new String(%1$s, 0, %1$sLength) : null",
            fieldName,
            name
            );

        final String suffix = type.isStringBased()
            ? String.format(
                "    private int %1$sLength;\n\n" +
                "    public int %1$sLength()\n" +
                "    {\n" +
                "%2$s" +
                "        return %1$sLength;\n" +
                "    }\n\n" +
                "    public String %1$sAsString()\n" +
                "    {\n" +
                "        return %3$s;\n" +
                "    }\n\n",
                fieldName,
                optionalCheck,
                asStringBody
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
                return String.format(" = new char[%d]", initialBufferSize);

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
                return "";

            case DATA:
                return initByteArray(initialBufferSize);

            case UTCTIMESTAMP:
                return initByteArray(UtcTimestampDecoder.LONG_LENGTH);

            case LOCALMKTDATE:
                return initByteArray(LocalMktDateDecoder.LENGTH);

            case UTCTIMEONLY:
                return initByteArray(UtcTimeOnlyDecoder.LONG_LENGTH);

            case UTCDATEONLY:
                return initByteArray(UtcDateOnlyDecoder.LENGTH);

            case MONTHYEAR:
                return initByteArray(MonthYear.LONG_LENGTH);

            default:
                throw new UnsupportedOperationException("Unknown type: " + type);
        }
    }

    private String initByteArray(final int initialBufferSize)
    {
        return String.format(" = new byte[%d]", initialBufferSize);
    }

    private String optionalGetter(final Entry entry)
    {
        return entry.required() ? "" : hasGetter(entry.name());
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
                return "char[]";

            case BOOLEAN:
                return "boolean";

            case DATA:
            case UTCTIMESTAMP:
            case UTCTIMEONLY:
            case UTCDATEONLY:
            case MONTHYEAR:
            case LOCALMKTDATE:
                return "byte[]";

            default:
                throw new UnsupportedOperationException("Unknown type: " + type);
        }
    }

    private String generateDecodeMethod(
        final List<Entry> entries, final Aggregate aggregate, final AggregateType type)
    {
        final boolean hasCommonCompounds = type == MESSAGE;
        final boolean isGroup = type == GROUP;
        final boolean isHeader = type == HEADER;
        final String endGroupCheck = endGroupCheck(aggregate, isGroup);

        final String prefix =
            "    public int decode(final AsciiBuffer buffer, final int offset, final int length)\n" +
                "    {\n" +
                "        int seenFieldCount = 0;\n" +
                "        if (" + CODEC_VALIDATION_ENABLED + ")\n" +
                "        {\n" +
                "            missingRequiredFields.copy(" + REQUIRED_FIELDS + ");\n" +
                "        }\n" +
                "        final int end = offset + length;\n" +
                "        int position = offset;\n" +
                (hasCommonCompounds ? "        position += header.decode(buffer, position, length);\n" : "") +
                (isGroup ? "        seenFields.clear();\n" : "") +
                "        int tag;\n\n" +
                "        while (position < end)\n" +
                "        {\n" +
                "            final int equalsPosition = buffer.scan(position, end, '=');\n" +
                "            tag = buffer.getInt(position, equalsPosition);\n" +
                endGroupCheck +
                "            final int valueOffset = equalsPosition + 1;\n" +
                "            final int endOfField = buffer.scan(valueOffset, end, START_OF_HEADER);\n" +
                "            final int valueLength = endOfField - valueOffset;\n" +
                "            if (" + CODEC_VALIDATION_ENABLED + ")\n" +
                "            {\n" +
                "                if (tag <= 0)\n" +
                "                {\n" +
                "                    invalidTagId = tag;\n" +
                "                    rejectReason = " + INVALID_TAG_NUMBER + ";\n" +
                "                }\n" +
                "                else if (valueLength == 0)\n" +
                "                {\n" +
                "                    invalidTagId = tag;\n" +
                "                    rejectReason = " + TAG_SPECIFIED_WITHOUT_A_VALUE + ";\n" +
                "                }\n" +
                headerValidation(isHeader) +

                (isGroup ? "" :
                "                if (!alreadyVisitedFields.add(tag))\n" +
                "                {\n" +
                "                    invalidTagId = tag;\n" +
                "                    rejectReason = " + TAG_APPEARS_MORE_THAN_ONCE + ";\n" +
                "                }\n") +

                "                missingRequiredFields.remove(tag);\n" +
                "                seenFieldCount++;\n" +
                "            }\n" +
                "            switch (tag)\n" +
                "            {\n\n";

        final String body =
            entries.stream()
                   .map(this::decodeEntry)
                   .collect(joining("\n", "", "\n"));

        final String groupSuffix = aggregate.containsGroup() ? " && !" + GROUP_FIELDS + ".contains(tag)" : "";

        final String suffix =
            "            default:\n" +
            "                if (" + CODEC_VALIDATION_ENABLED +
            " && !TrailerDecoder." + REQUIRED_FIELDS + ".contains(tag)" + groupSuffix + ")\n" +
            "                {\n" +
            "                    unknownFields.add(tag);\n" +
            "                }\n" +
            "                return position - offset;\n\n" +
            "            }\n\n" +
            "            position = endOfField + 1;\n" +
            "        }\n\n" +
            (hasCommonCompounds ? "        position += trailer.decode(buffer, position, end - position);\n" : "") +
            "        return position - offset;\n" +
            "    }\n\n";

        return prefix + body + suffix;
    }

    private String endGroupCheck(final Aggregate aggregate, final boolean isGroup)
    {
        final String endGroupCheck;
        if (isGroup)
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
        return endGroupCheck;
    }

    private String headerValidation(final boolean isHeader)
    {
        return isHeader ?
            "                else if (seenFieldCount == 0 && tag != 8)\n" +
            "                {\n" +
            "                    invalidTagId = tag;\n" +
            "                    rejectReason = " + TAG_SPECIFIED_OUT_OF_REQUIRED_ORDER + ";\n" +
            "                }\n" +
            "                else if (seenFieldCount == 1 && tag != 9)\n" +
            "                {\n" +
            "                    invalidTagId = tag;\n" +
            "                    rejectReason = " + TAG_SPECIFIED_OUT_OF_REQUIRED_ORDER + ";\n" +
            "                }\n" +
            "                else if (seenFieldCount == 2 && tag != 35)\n" +
            "                {\n" +
            "                    invalidTagId = tag;\n" +
            "                    rejectReason = " + TAG_SPECIFIED_OUT_OF_REQUIRED_ORDER + ";\n" +
            "                }\n"
            : "";
    }

    private String decodeEntry(final Entry entry)
    {
        return entry.matchEntry(
            e -> decodeField(e, ""),
            this::decodeGroup,
            this::decodeComponent);
    }

    private String decodeComponent(final Entry entry)
    {
        final Component component = (Component) entry.element();
        return component.entries()
                        .stream()
                        .map(this::decodeEntry)
                        .collect(joining("\n", "", "\n"));
    }

    protected String generateComponentToString(final Component component)
    {
        return component.entries()
                        .stream()
                        .map(this::generateEntryToString)
                        .collect(joining(" + \n"));
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
        // Uses variables from surrounding context:
        // int tag = the tag number of the field
        // int valueOffset = starting index of the value
        // int valueLength = the number of bytes for the value
        // int endOfField = the end index of the value

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
            decodeMethodFor(field.type(), fieldName),
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

    private String decodeMethodFor(final Type type, String fieldName)
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
                return "getChar(valueOffset";

            case STRING:
            case MULTIPLEVALUESTRING:
            case CURRENCY:
            case EXCHANGE:
            case COUNTRY:
                return String.format("getChars(%s, valueOffset, valueLength", fieldName);

            case BOOLEAN:
                return "getBoolean(valueOffset";

            case DATA:
            case UTCTIMESTAMP:
            case LOCALMKTDATE:
            case UTCTIMEONLY:
            case UTCDATEONLY:
            case MONTHYEAR:
                return String.format("getBytes(%s, valueOffset, valueLength", fieldName);

            default:
                throw new UnsupportedOperationException("Unknown type: " + type);
        }
    }

    protected String generateStringToString(final String fieldName)
    {
        return String.format("new String(%s)", fieldName);
    }
}
