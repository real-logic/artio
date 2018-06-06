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

import org.agrona.LangUtil;
import org.agrona.generation.OutputManager;
import org.agrona.generation.ResourceConsumer;
import uk.co.real_logic.artio.builder.Decoder;
import uk.co.real_logic.artio.dictionary.ir.*;
import uk.co.real_logic.artio.dictionary.ir.Field.Type;
import uk.co.real_logic.artio.fields.*;

import java.io.IOException;
import java.io.Writer;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static uk.co.real_logic.artio.dictionary.generation.AggregateType.*;
import static uk.co.real_logic.artio.dictionary.generation.ConstantGenerator.constantValuesOfField;
import static uk.co.real_logic.artio.dictionary.generation.ConstantGenerator.sizeHashSet;
import static uk.co.real_logic.artio.dictionary.generation.Exceptions.rethrown;
import static uk.co.real_logic.artio.dictionary.generation.GenerationUtil.constantName;
import static uk.co.real_logic.artio.dictionary.generation.GenerationUtil.fileHeader;
import static uk.co.real_logic.sbe.generation.java.JavaUtil.formatPropertyName;

// TODO: optimisations
// skip decoding the msg type, since its known
// skip decoding the body string, since its known
// use ordering of fields to reduce branching
// skip decoding of unread header fields - eg: sender/target comp id.
// optimise the checksum definition to use an int and be calculated or ignored, have optional validation.
// evaluate utc parsing, adds about 100 nanos
// remove the REQUIRED_FIELDS validation when there are no required fields

public class DecoderGenerator extends Generator
{
    public static final boolean CODEC_LOGGING = Boolean.getBoolean("fix.codec.log");

    public static final String REQUIRED_FIELDS = "REQUIRED_FIELDS";
    public static final String GROUP_FIELDS = "GROUP_FIELDS";
    public static final String ALL_FIELDS = "allFields";

    public static final int INVALID_TAG_NUMBER =
        RejectReason.INVALID_TAG_NUMBER.representation();
    public static final int REQUIRED_TAG_MISSING =
        RejectReason.REQUIRED_TAG_MISSING.representation();
    public static final int TAG_NOT_DEFINED_FOR_THIS_MESSAGE_TYPE =
        RejectReason.TAG_NOT_DEFINED_FOR_THIS_MESSAGE_TYPE.representation();
    public static final int TAG_SPECIFIED_WITHOUT_A_VALUE =
        RejectReason.TAG_SPECIFIED_WITHOUT_A_VALUE.representation();
    public static final int VALUE_IS_INCORRECT =
        RejectReason.VALUE_IS_INCORRECT.representation();

    // TODO: ensure that these are only used in the case that we're dealing with FIX 4.4. or later
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

    private Aggregate currentAggregate = null;

    private final int initialBufferSize;

    private String allFieldsDictionary;

    public DecoderGenerator(
        final Dictionary dictionary,
        final int initialBufferSize,
        final String builderPackage,
        final String builderCommonPackage,
        final OutputManager outputManager,
        final Class<?> validationClass)
    {
        super(dictionary, builderPackage, builderCommonPackage, outputManager, validationClass);
        this.initialBufferSize = initialBufferSize;
    }

    public void generate()
    {
        allFieldsDictionary = generateAllFieldsDictionary();
        super.generate();
    }

    private String generateAllFieldsDictionary()
    {
        final int hashMapSize = sizeHashSet(dictionary.fields().values());

        return intHashSetCopy(hashMapSize, ALL_FIELDS, "Constants.ALL_FIELDS");
    }

    private String intHashSetCopy(
        final int hashMapSize,
        final String name,
        final String from)
    {
        return String.format(
            "    public final IntHashSet %2$s = new IntHashSet(%1$d);\n" +
            "    {\n" +
            "        %2$s.copy(%3$s);\n" +
            "    }\n\n",
            hashMapSize,
            name,
            from);
    }

    protected void generateAggregateFile(final Aggregate aggregate, final AggregateType type)
    {
        if (type == COMPONENT)
        {
            componentInterface((Component)aggregate);
            return;
        }

        final String className = decoderClassName(aggregate);

        outputManager.withOutput(
            className,
            (out) ->
            {
                out.append(fileHeader(builderPackage));

                generateImports("Decoder", type, out);
                generateAggregateClass(aggregate, type, className, out);
            });
    }

    private void generateAggregateClass(
        final Aggregate aggregate,
        final AggregateType type,
        final String className,
        final Writer out) throws IOException
    {
        final Aggregate parentAggregate = currentAggregate;
        currentAggregate = aggregate;

        final boolean isMessage = type == MESSAGE;
        final List<String> interfaces = aggregate
            .entriesWith((element) -> element instanceof Component)
            .map((comp) -> decoderClassName((Aggregate)comp.element()))
            .collect(toList());

        interfaces.add(Decoder.class.getSimpleName());

        out.append(classDeclaration(className, interfaces, false));
        validation(out, aggregate, type);
        if (isMessage)
        {
            final Message message = (Message)aggregate;
            out.append(messageType(message.fullType(), message.packedType()));
            out.append(commonCompoundImports("Decoder", true));
        }
        groupMethods(out, aggregate);
        headerMethods(out, aggregate, type);
        getters(out, aggregate.entries());
        out.append(decodeMethod(aggregate.entries(), aggregate, type));
        out.append(completeResetMethod(isMessage, aggregate.entries(), resetValidation()));
        out.append(toString(aggregate, isMessage));
        out.append("}\n");
        currentAggregate = parentAggregate;
    }

    private void headerMethods(final Writer out, final Aggregate aggregate, final AggregateType type) throws IOException
    {
        if (type == HEADER)
        {
            // Default constructor so that the header decoder can be used independently to parser headers.
            out.append(
                "    public HeaderDecoder()\n" +
                "    {\n" +
                "        this(new TrailerDecoder());\n" +
                "    }\n\n");
            wrapTrailerInConstructor(out, aggregate);
        }
    }

    private void groupClass(final Group group, final Writer out) throws IOException
    {
        final String className = decoderClassName(group);
        generateAggregateClass(group, GROUP, className, out);
    }

    protected Class<?> topType(final AggregateType aggregateType)
    {
        return Decoder.class;
    }

    protected String resetRequiredFloat(final String name)
    {
        return resetByMethod(name);
    }

    protected String resetRequiredInt(final Field field)
    {
        return resetFieldValue(field.name(), "MISSING_INT");
    }

    protected String toStringGroupParameters()
    {
        return "";
    }

    protected String toStringGroupSuffix()
    {
        return
            "        if (next != null)\n" +
            "        {\n" +
            "            entries += \",\\n\" + next.toString();\n" +
            "        }\n";
    }

    private String resetValidation()
    {
        return
            "        if (" + CODEC_VALIDATION_ENABLED + ")\n" +
            "        {\n" +
            "            invalidTagId = NO_ERROR;\n" +
            "            rejectReason = NO_ERROR;\n" +
            "            missingRequiredFields.clear();\n" +
            "            unknownFields.clear();\n" +
            "            alreadyVisitedFields.clear();\n" +
            "        }\n";
    }

    private void validation(final Writer out, final Aggregate aggregate, final AggregateType type)
        throws IOException
    {
        final List<Field> requiredFields = requiredFields(aggregate.entries()).collect(toList());
        out.append(generateFieldDictionary(requiredFields, REQUIRED_FIELDS));
        out.append(allFieldsDictionary);

        if (aggregate.containsGroup())
        {
            final List<Field> groupFields = aggregate.allGroupFields().collect(toList());
            final String groupFieldString = generateFieldDictionary(groupFields, GROUP_FIELDS);
            out.append(groupFieldString);
        }

        final String enumValidation = aggregate
            .allChildEntries()
            .filter((entry) -> entry.element().isEnumField())
            .map((entry) -> validateEnum(entry, out))
            .collect(joining("\n"));

        final boolean isMessage = type == MESSAGE;
        final String messageValidation = isMessage ?
            "        else if (unknownFieldsIterator.hasNext())\n" +
            "        {\n" +
            "            invalidTagId = unknownFieldsIterator.nextValue();\n" +
            "            rejectReason = allFields.contains(invalidTagId) ? " +
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
            "        }\n" :
            "";

        out.append(String.format(
            "    private final IntHashSet alreadyVisitedFields = new IntHashSet(%4$d);\n\n" +
            "    private final IntHashSet missingRequiredFields = new IntHashSet(%1$d);\n\n" +
            "    private final IntHashSet unknownFields = new IntHashSet(10);\n\n" +
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

    private String generateFieldDictionary(final Collection<Field> fields, final String name)
    {
        final String addFields = fields
            .stream()
            .map((field) -> addField(field, name))
            .collect(joining());

        final int hashMapSize = sizeHashSet(fields);
        return String.format(
            "    public final IntHashSet %3$s = new IntHashSet(%1$d);\n" +
            "    {\n" +
            "        if (" + CODEC_VALIDATION_ENABLED + ")\n" +
            "        {\n" +
            "%2$s" +
            "        }\n" +
            "    }\n\n",
            hashMapSize,
            addFields,
            name);
    }

    public static String addField(final Field field, final String name)
    {
        final String fieldName = formatPropertyName(field.name());

        return String.format(
            "        %1$s.add(Constants.%2$s);\n",
            name,
            constantName(fieldName));
    }

    private CharSequence validateEnum(final Entry entry, final Writer out)
    {
        final Field field = (Field)entry.element();
        if (!EnumGenerator.hasEnumGenerated(field))
        {
            return "";
        }

        final String name = entry.name();
        final String valuesField = "valuesOf" + entry.name();
        final String optionalCheck = entry.required() ? "" : String.format("has%s && ", name);
        final int tagNumber = field.number();
        final Type type = field.type();
        final String propertyName = formatPropertyName(name);

        final boolean isChar = type == Type.CHAR;
        final boolean isPrimitive = type.isIntBased() || isChar;
        final String copyFrom = "Constants." + constantValuesOfField(name);
        try
        {
            if (isPrimitive)
            {
                out.append(intHashSetCopy(
                    sizeHashSet(field.values()), valuesField, copyFrom));
            }
            else if (type.isStringBased())
            {
                out.append(String.format(
                    "    public final CharArraySet %1$s = new CharArraySet(%2$s);\n", valuesField, copyFrom));
            }
            else
            {
                return "";
            }
        }
        catch (final IOException ex)
        {
            LangUtil.rethrowUnchecked(ex);
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
            isPrimitive ? "" : ", " + propertyName + "Length");
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
            (e, group) -> Stream.of((Field)group.numberField().element()),
            (e, component) -> requiredFields(component.entries()));
    }

    private void componentInterface(final Component component)
    {
        final String className = decoderClassName(component);
        outputManager.withOutput(className,
            (out) ->
            {
                out.append(fileHeader(builderPackage));

                generateImports("Decoder", AggregateType.COMPONENT, out);
                out.append(String.format(
                    "\npublic interface %1$s\n" +
                    "{\n\n",
                    className));

                for (final Entry entry : component.entries())
                {
                    interfaceGetter(entry, out);
                }
                out.append("\n}\n");
            });
    }

    private void interfaceGetter(final Entry entry, final Writer out) throws IOException
    {
        entry.forEach(
            (field) -> out.append(fieldInterfaceGetter(entry, field)),
            (group) -> groupInterfaceGetter(group, out),
            (component) -> componentInterfaceGetter(component, out));
    }

    private void groupInterfaceGetter(final Group group, final Writer out) throws IOException
    {
        groupClass(group, out);

        out.append(String.format(
            "    public %1$s %2$s();\n",
            decoderClassName(group),
            formatPropertyName(group.name())));
    }

    private void componentInterfaceGetter(final Component component, final Writer out)
        throws IOException
    {
        wrappedForEachEntry(component, out, (entry) -> interfaceGetter(entry, out));
    }

    private void wrappedForEachEntry(
        final Aggregate aggregate, final Writer out, final ResourceConsumer<Entry> consumer)
        throws IOException
    {
        out.append("\n");
        aggregate
            .entries()
            .forEach(rethrown(consumer));
        out.append("\n");
    }

    private String fieldInterfaceGetter(final Entry entry, final Field field)
    {
        final String name = field.name();
        final String fieldName = formatPropertyName(name);
        final Type type = field.type();

        final String length = type.isStringBased() ?
            String.format("    public int %1$sLength();\n", fieldName) : "";

        final String optional = !entry.required() ?
            String.format("    public boolean has%1$s();\n", name) : "";

        return String.format(
            "    public %1$s %2$s();\n" +
            "%3$s" +
            "%4$s",
            javaTypeOf(type),
            fieldName,
            optional,
            length);
    }

    private void getter(final Entry entry, final Writer out) throws IOException
    {
        entry.forEach(
            (field) -> out.append(fieldGetter(entry, field)),
            (group) -> groupGetter(group, out),
            (component) -> componentGetter(component, out));
    }

    private void groupMethods(final Writer out, final Aggregate aggregate) throws IOException
    {
        if (aggregate instanceof Group)
        {
            wrapTrailerInConstructor(out, aggregate);

            out.append(String.format(
                "    private %1$s next = null;\n\n" +
                "    public %1$s next()\n" +
                "    {\n" +
                "        return next;\n" +
                "    }\n\n" +
                "    private IntHashSet seenFields = new IntHashSet(%2$d);\n\n",
                decoderClassName(aggregate),
                sizeHashSet(aggregate.entries())));
        }
    }

    private void wrapTrailerInConstructor(final Writer out, final Aggregate aggregate) throws IOException
    {
        out.append(String.format(
            "    private final TrailerDecoder trailer;\n" +
            "    public %1$s(final TrailerDecoder trailer)\n" +
            "    {\n" +
            "        this.trailer = trailer;\n" +
            "    }\n\n",
            decoderClassName(aggregate)));
    }

    private String iteratorClassName(final Group group)
    {
        return group.name() + "Iterator";
    }

    private String iteratorFieldName(final Group group)
    {
        return formatPropertyName(iteratorClassName(group));
    }

    private String messageType(final String fullType, final int packedType)
    {
        return String.format(
            "    public static final int MESSAGE_TYPE = %1$d;\n\n" +
            "    public static final String MESSAGE_TYPE_AS_STRING = \"%2$s\";\n\n" +
            "    public static final byte[] MESSAGE_TYPE_BYTES = MESSAGE_TYPE_AS_STRING.getBytes(US_ASCII);\n\n",
            packedType,
            fullType);
    }

    private void getters(final Writer out, final List<Entry> entries) throws IOException
    {
        for (final Entry entry : entries)
        {
            getter(entry, out);
        }
    }

    private void componentGetter(final Component component, final Writer out) throws IOException
    {
        final Aggregate parentAggregate = currentAggregate;
        currentAggregate = component;
        wrappedForEachEntry(component, out, entry -> getter(entry, out));
        currentAggregate = parentAggregate;
    }

    private void groupGetter(final Group group, final Writer out) throws IOException
    {
        // The component interface will generate the group class
        if (!(currentAggregate instanceof Component))
        {
            groupClass(group, out);
        }

        final Entry numberField = group.numberField();
        final String prefix = fieldGetter(numberField, (Field)numberField.element());

        out.append(String.format(
            "\n" +
            "    private %1$s %2$s = null;\n" +
            "    public %1$s %2$s()\n" +
            "    {\n" +
            "        return %2$s;\n" +
            "    }\n\n" +
            "%3$s",
            decoderClassName(group),
            formatPropertyName(group.name()),
            prefix));

        generateGroupIterator(out, group);
    }

    private void generateGroupIterator(final Writer out, final Group group) throws IOException
    {
        out.append(String.format(
            "    private %1$s %2$s = new %1$s();\n\n" +
            "    public %1$s %2$s()\n" +
            "    {\n" +
            "        return %2$s.iterator();\n" +
            "    }\n\n" +
            "    public class %1$s implements Iterable<%4$s>, java.util.Iterator<%4$s>\n" +
            "    {\n" +
            "        private int remainder;\n" +
            "        private %4$s current;\n" +
            "        public boolean hasNext()\n" +
            "        {\n" +
            "            return remainder > 0;\n" +
            "        }\n" +
            "        public %4$s next()\n" +
            "        {\n" +
            "            remainder--;\n" +
            "            final %4$s value = current;\n" +
            "            current = current.next();\n" +
            "            return value;\n" +
            "        }\n" +
            "        public void reset()\n" +
            "        {\n" +
            "            remainder = %3$s;\n" +
            "            current = %5$s();\n" +
            "        }\n" +
            "        public %1$s iterator()\n" +
            "        {\n" +
            "            reset();\n" +
            "            return this;\n" +
            "        }\n" +
            "    }\n\n",
            iteratorClassName(group),
            iteratorFieldName(group),
            formatPropertyName(group.numberField().name()),
            decoderClassName(group),
            formatPropertyName(group.name())));
    }

    private String fieldGetter(final Entry entry, final Field field)
    {
        final String name = field.name();
        final String fieldName = formatPropertyName(name);
        final Type type = field.type();
        final String optionalCheck = optionalCheck(entry);

        final String asStringBody = String.format(entry.required() ?
            "new String(%1$s, 0, %1$sLength)" :
            "has%2$s ? new String(%1$s, 0, %1$sLength) : null",
            fieldName,
            name);

        final String enumValueDecoder = String.format(
            type.isStringBased() ?
            "%1$s.decode(%2$s, %2$sLength)" :
            "%1$s.decode(%2$s)",
            name,
            fieldName);

        final String asEnumBody = String.format(
            entry.required() ?
            "%1$s" :
            "has%2$s ? %1$s : null",
            enumValueDecoder,
            name
        );

        final String stringDecoder = type.isStringBased() ? String.format(
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
            asStringBody) : "";

        final String enumDecoder = EnumGenerator.hasEnumGenerated(field) && !field.type().isMultiValue() ?
            String.format(
            "    public %s %sAsEnum()\n" +
            "    {\n" +
            "        return %s;\n" +
            "    }\n\n",
            name,
            fieldName,
            asEnumBody
        ) : "";

        return String.format(
            "    private %s %s%s;\n\n" +
            "%s" +
            "    public %1$s %2$s()\n" +
            "    {\n" +
            "%s" +
            "        return %2$s;\n" +
            "    }\n\n" +
            "%s\n" +
            "%s\n" +
            "%s",
            javaTypeOf(type),
            fieldName,
            fieldInitialisation(type),
            hasField(entry),
            optionalCheck,
            optionalGetter(entry),
            stringDecoder,
            enumDecoder);
    }

    private String fieldInitialisation(final Type type)
    {
        switch (type)
        {
            case STRING:
            case MULTIPLEVALUESTRING:
            case MULTIPLESTRINGVALUE:
            case MULTIPLECHARVALUE:
            case CURRENCY:
            case EXCHANGE:
            case COUNTRY:
            case LANGUAGE:
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
            case XMLDATA:
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

            case TZTIMEONLY:
                return initByteArray(UtcTimeOnlyDecoder.LONG_LENGTH + 7);

            case TZTIMESTAMP:
                return initByteArray(UtcTimestampDecoder.LONG_LENGTH + 7);

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
            case MULTIPLESTRINGVALUE:
            case MULTIPLECHARVALUE:
            case CURRENCY:
            case EXCHANGE:
            case COUNTRY:
            case LANGUAGE:
                return "char[]";

            case BOOLEAN:
                return "boolean";

            case DATA:
            case XMLDATA:
            case UTCTIMESTAMP:
            case UTCTIMEONLY:
            case UTCDATEONLY:
            case MONTHYEAR:
            case LOCALMKTDATE:
            case TZTIMEONLY:
            case TZTIMESTAMP:
                return "byte[]";

            default:
                throw new UnsupportedOperationException("Unknown type: " + type);
        }
    }

    private String decodeMethod(final List<Entry> entries, final Aggregate aggregate, final AggregateType type)
    {
        final boolean hasCommonCompounds = type == MESSAGE;
        final boolean isGroup = type == GROUP;
        final boolean isHeader = type == HEADER;
        final String endGroupCheck = endGroupCheck(aggregate, isGroup);

        final String prefix =
            "    public int decode(final AsciiBuffer buffer, final int offset, final int length)\n" +
            "    {\n" +
            "        // Decode " + aggregate.name() + "\n" +
            "        int seenFieldCount = 0;\n" +
            "        if (" + CODEC_VALIDATION_ENABLED + ")\n" +
            "        {\n" +
            "            missingRequiredFields.copy(" + REQUIRED_FIELDS + ");\n" +
            "            alreadyVisitedFields.clear();\n" +
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

        final String body = entries.stream()
            .map(this::decodeEntry)
            .collect(joining("\n", "", "\n"));

        final String groupSuffix = aggregate.containsGroup() ? " && !" + GROUP_FIELDS + ".contains(tag)" : "";

        final String suffix =
            "            default:\n" +
            "                if (" + CODEC_VALIDATION_ENABLED + isTrailerTag(type) + groupSuffix + ")\n" +
            "                {\n" +
            "                    unknownFields.add(tag);\n" +
            "                }\n" +
            // Skip the thing if it's a completely unknown field and you aren't validating messages
            "                if (" + CODEC_VALIDATION_ENABLED + " || allFields.contains(tag))\n" +
            "                {\n" +
            decodeTrailerOrReturn(hasCommonCompounds, 5) +
            "                }\n" +
            "\n" +
            "            }\n\n" +
            "            if (position < (endOfField + 1))\n" +
            "            {\n" +
            "                position = endOfField + 1;\n" +
            "            }\n" +
            "        }\n" +
            decodeTrailerOrReturn(hasCommonCompounds, 2) +
            "    }\n\n";

        return prefix + body + suffix;
    }

    private String decodeTrailerOrReturn(final boolean hasCommonCompounds, final int indent)
    {
        return (hasCommonCompounds ?
            indent(indent, "position += trailer.decode(buffer, position, end - position);\n") : "") +
            indent(indent, "return position - offset;\n");
    }

    private String isTrailerTag(final AggregateType type)
    {
        if (type == TRAILER)
        {
            return " && !" + REQUIRED_FIELDS + ".contains(tag)";
        }
        else
        {
            return " && !trailer." + REQUIRED_FIELDS + ".contains(tag)";
        }
    }

    private String endGroupCheck(final Aggregate aggregate, final boolean isGroup)
    {
        final String endGroupCheck;
        if (isGroup)
        {
            endGroupCheck = String.format(
                "            if (!seenFields.add(tag))\n" +
                "            {\n" +
                "                if (next == null)\n" +
                "                {\n" +
                "                    next = new %1$s(trailer);\n" +
                "                }\n" +
                "                return position - offset;\n" +
                "            }\n",
                decoderClassName(aggregate));
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
            "                }\n" :
            "";
    }

    private String decodeEntry(final Entry entry)
    {
        return entry.matchEntry(
            (e) -> decodeField(e, ""),
            this::decodeGroup,
            this::decodeComponent);
    }

    private String decodeComponent(final Entry entry)
    {
        final Component component = (Component)entry.element();
        return component
            .entries()
            .stream()
            .map(this::decodeEntry)
            .collect(joining("\n", "", "\n"));
    }

    protected String componentToString(final Component component)
    {
        return component
            .entries()
            .stream()
            .map(this::entryToString)
            .collect(joining(" + \n"));
    }

    private String decodeGroup(final Entry entry)
    {
        final Group group = (Group)entry.element();

        final String parseGroup = String.format(
            "                if (%1$s == null)\n" +
            "                {\n" +
            "                    %1$s = new %2$s(trailer);\n" +
            "                }\n" +
            "                %2$s %1$sCurrent = %1$s;\n" +
            "                position = endOfField + 1;\n" +
            "                for (int i = 0; i < %3$s && position < end; i++)\n" +
            "                {\n" +
            "                    position += %1$sCurrent.decode(buffer, position, end - position);\n" +
            "                    %1$sCurrent = %1$sCurrent.next();\n" +
            "                }\n",
            formatPropertyName(group.name()),
            decoderClassName(group),
            formatPropertyName(group.numberField().name()));

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
        final String name = entry.name();
        final String fieldName = formatPropertyName(name);

        return String.format(
            "            case Constants.%s:\n" +
            "%s" +
            "                %s = buffer.%s);\n" +
            "%s" +
            "%s" +
            "                break;\n",
            constantName(fieldName),
            optionalAssign(entry),
            fieldName,
            decodeMethodFor(field.type(), fieldName),
            storeLengthForArrays(field.type(), fieldName),
            suffix);
    }

    private String storeLengthForArrays(final Type type, final String fieldName)
    {
        return type.hasLengthField() ?
            String.format("                %sLength = valueLength;\n", fieldName) :
            "";
    }

    private String optionalAssign(final Entry entry)
    {
        return entry.required() ? "" : String.format("                has%s = true;\n", entry.name());
    }

    private String decodeMethodFor(final Type type, final String fieldName)
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
            case MULTIPLESTRINGVALUE:
            case MULTIPLECHARVALUE:
            case CURRENCY:
            case EXCHANGE:
            case COUNTRY:
            case LANGUAGE:
                return String.format("getChars(%s, valueOffset, valueLength", fieldName);

            case BOOLEAN:
                return "getBoolean(valueOffset";

            case DATA:
            case XMLDATA:
            case UTCTIMESTAMP:
            case LOCALMKTDATE:
            case UTCTIMEONLY:
            case UTCDATEONLY:
            case TZTIMEONLY:
            case TZTIMESTAMP:
            case MONTHYEAR:
                return String.format("getBytes(%s, valueOffset, valueLength", fieldName);

            default:
                throw new UnsupportedOperationException("Unknown type: " + type);
        }
    }

    protected String stringToString(final String fieldName)
    {
        return String.format("new String(%s, 0, %1$sLength)", fieldName);
    }

    protected boolean hasFlag(final Entry entry, final Field field)
    {
        return !entry.required();
    }

    protected String resetTemporalValue(final String name)
    {
        return resetNothing(name);
    }

    protected String resetComponents(final List<Entry> entries, final StringBuilder methods)
    {
        return entries
            .stream()
            .filter(Entry::isComponent)
            .map((entry) -> resetEntries(((Component)entry.element()).entries(), methods))
            .collect(joining());
    }

    protected String groupEntryToString(final Group element, final String name)
    {
        return String.format(
            "                (%2$s != null ? String.format(\"  \\\"%1$s\\\": [\\n" +
            "  %%s" +
            "\\n  ]" +
            "\\n\", %2$s.toString().replace(\"\\n\", \"\\n  \")" + ") : \"\")",
            name,
            formatPropertyName(name));
    }

    protected String optionalReset(final Field field, final String name)
    {
        return resetByFlag(name);
    }

    protected boolean toStringChecksHasGetter(final Entry entry, final Field field)
    {
        return hasFlag(entry, field);
    }
}
