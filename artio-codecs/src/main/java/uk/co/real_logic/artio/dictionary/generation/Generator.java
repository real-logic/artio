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

import org.agrona.MutableDirectBuffer;
import org.agrona.collections.IntHashSet;
import org.agrona.generation.OutputManager;
import uk.co.real_logic.artio.EncodingException;
import uk.co.real_logic.artio.dictionary.CharArraySet;
import uk.co.real_logic.artio.dictionary.StandardFixConstants;
import uk.co.real_logic.artio.dictionary.ir.*;
import uk.co.real_logic.artio.dictionary.ir.Entry.Element;
import uk.co.real_logic.artio.fields.DecimalFloat;
import uk.co.real_logic.artio.fields.LocalMktDateEncoder;
import uk.co.real_logic.artio.fields.UtcTimestampEncoder;
import uk.co.real_logic.artio.util.AsciiBuffer;
import uk.co.real_logic.artio.util.AsciiSequenceView;
import uk.co.real_logic.artio.util.MutableAsciiBuffer;

import java.io.IOException;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.joining;
import static uk.co.real_logic.artio.dictionary.generation.AggregateType.*;
import static uk.co.real_logic.artio.dictionary.generation.GenerationUtil.importFor;
import static uk.co.real_logic.artio.dictionary.generation.GenerationUtil.importStaticFor;
import static uk.co.real_logic.sbe.generation.java.JavaUtil.formatPropertyName;

public abstract class Generator
{
    public static final String MSG_TYPE = "MsgType";
    public static final String BEGIN_STRING = "BeginString";
    public static final String BODY_LENGTH = "BodyLength";

    public static final String EXPAND_INDENT = ".toString().replace(\"\\n\", \"\\n  \")";
    public static final String CODEC_VALIDATION_ENABLED = "CODEC_VALIDATION_ENABLED";

    protected String commonCompoundImports(final String form, final boolean headerWrapsTrailer)
    {
        final String headerParameter = headerWrapsTrailer ? "trailer" : "";
        return String.format(
            "    private Trailer%1$s trailer = new Trailer%1$s();\n\n" +
            "    public Trailer%1$s trailer()\n" +
            "    {\n" +
            "        return trailer;\n" +
            "    }\n\n" +

            "    private Header%1$s header = new Header%1$s(%2$s);\n\n" +
            "    public Header%1$s header()\n" +
            "    {\n" +
            "        return header;\n" +
            "    }\n\n",
            form,
            headerParameter);
    }

    private static final String COMMON_COMPOUND_IMPORTS =
        "import %1$s.Header%2$s;\n" +
        "import %1$s.Trailer%2$s;\n";

    protected final Dictionary dictionary;
    protected final String builderPackage;
    private String builderCommonPackage;
    protected final OutputManager outputManager;
    protected final Class<?> validationClass;

    protected Generator(
        final Dictionary dictionary,
        final String builderPackage,
        final String builderCommonPackage,
        final OutputManager outputManager,
        final Class<?> validationClass)
    {
        this.dictionary = dictionary;
        this.builderPackage = builderPackage;
        this.builderCommonPackage = builderCommonPackage;
        this.outputManager = outputManager;
        this.validationClass = validationClass;
    }

    public void generate()
    {
        generateAggregateFile(dictionary.header(), AggregateType.HEADER);
        generateAggregateFile(dictionary.trailer(), AggregateType.TRAILER);
        dictionary.components().forEach((name, component) -> generateAggregateFile(component, COMPONENT));
        dictionary.messages().forEach((msg) -> generateAggregateFile(msg, MESSAGE));
    }

    protected abstract void generateAggregateFile(Aggregate aggregate, AggregateType type);

    protected abstract Class<?> topType(AggregateType aggregateType);

    protected void generateImports(
        final String compoundSuffix,
        final AggregateType type,
        final Writer out) throws IOException
    {
        out
            .append(importFor(MutableDirectBuffer.class))
            .append(importFor(AsciiSequenceView.class))
            .append(importStaticFor(CodecUtil.class))
            .append(importStaticFor(StandardFixConstants.class))
            .append(importFor(topType(MESSAGE)));

        if (topType(GROUP) != topType(MESSAGE))
        {
            out.append(importFor(topType(GROUP)));
        }

        out
            .append(type == MESSAGE ? String.format(COMMON_COMPOUND_IMPORTS, builderPackage, compoundSuffix) : "")
            .append(importFor(DecimalFloat.class))
            .append(importFor(MutableAsciiBuffer.class))
            .append(importFor(AsciiBuffer.class))
            .append(importFor(LocalMktDateEncoder.class))
            .append(importFor(UtcTimestampEncoder.class))
            .append(importFor(StandardCharsets.class))
            .append(importFor(Arrays.class))
            .append(importFor(CharArraySet.class))
            .append(importFor(IntHashSet.class))
            .append(importFor(IntHashSet.IntIterator.class))
            .append(importFor(EncodingException.class))
            .append(importStaticFor(StandardCharsets.class, "US_ASCII"))
            .append(importStaticFor(validationClass, CODEC_VALIDATION_ENABLED));
        if (!builderPackage.equals(builderCommonPackage) && !builderCommonPackage.isEmpty())
        {
            out.append(importFor(builderCommonPackage + ".*"));
        }
    }

    protected String classDeclaration(
        final String className,
        final List<String> interfaces,
        final boolean isStatic)
    {
        final String interfaceList = interfaces.isEmpty() ? "" : " implements " + String.join(", ", interfaces);

        return String.format(
            "\n\npublic %3$sclass %1$s%2$s\n" +
            "{\n",
            className,
            interfaceList,
            isStatic ? "static " : "");
    }

    protected String completeResetMethod(
        final boolean isMessage,
        final List<Entry> entries,
        final String additionalReset)
    {
        final StringBuilder methods = new StringBuilder();

        final String resetEntries = resetEntries(entries, methods);

        if (isMessage)
        {
            return String.format(
                "    public void reset()\n" +
                "    {\n" +
                "        header.reset();\n" +
                "        trailer.reset();\n" +
                "        resetMessage();\n" +
                "%2$s" +
                "    }\n\n" +
                "    public void resetMessage()\n" +
                "    {\n" +
                "%1$s" +
                "    }\n\n" +
                "%3$s",
                resetEntries,
                additionalReset,
                methods);
        }
        else
        {
            return String.format(
                "    public void reset()\n" +
                "    {\n" +
                "%s" +
                "%s" +
                "    }\n\n" +
                "%s",
                resetEntries,
                additionalReset,
                methods);
        }
    }

    protected String resetEntries(final List<Entry> entries, final StringBuilder methods)
    {
        return resetFields(entries, methods) +
            resetComponents(entries, methods) +
            resetGroups(entries, methods);
    }

    private String resetFields(final List<Entry> entries, final StringBuilder methods)
    {
        return resetAllBy(
            entries,
            methods,
            Entry::isField,
            (entry) -> resetField(entry.required(), (Field)entry.element()),
            this::callResetMethod);
    }

    protected String resetAllBy(
        final List<Entry> entries,
        final StringBuilder methods,
        final Predicate<Entry> predicate,
        final Function<Entry, String> methodFactory,
        final Function<Entry, String> callFactory)
    {
        methods.append(entries
            .stream()
            .filter(predicate)
            .map(methodFactory)
            .collect(joining()));

        return entries
            .stream()
            .filter(predicate)
            .map(callFactory)
            .collect(joining());
    }

    private String resetGroups(final List<Entry> entries, final StringBuilder methods)
    {
        return resetAllBy(
            entries,
            methods,
            Entry::isGroup,
            this::resetGroup,
            this::callResetMethod);
    }

    private String resetGroup(final Entry entry)
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

    private String resetField(final boolean isRequired, final Field field)
    {
        final String name = field.name();

        if (isNotResettableField(name))
        {
            return "";
        }

        if (!isRequired)
        {
            return optionalReset(field, name);
        }

        switch (field.type())
        {
            case INT:
            case LENGTH:
            case SEQNUM:
            case NUMINGROUP:
            case DAYOFMONTH:
                return resetRequiredInt(field);

            case FLOAT:
            case PRICE:
            case PRICEOFFSET:
            case QTY:
            case PERCENTAGE:
            case AMT:
                return resetRequiredFloat(name);

            case CHAR:
                return resetFieldValue(name, "MISSING_CHAR");

            case DATA:
            case XMLDATA:
                return resetFieldValue(name, "null");

            case BOOLEAN:
                return resetFieldValue(name, "false");

            case STRING:
            case MULTIPLEVALUESTRING:
            case MULTIPLESTRINGVALUE:
            case MULTIPLECHARVALUE:
            case CURRENCY:
            case EXCHANGE:
            case COUNTRY:
            case LANGUAGE:
                return resetStringBasedData(name);

            case UTCTIMESTAMP:
            case LOCALMKTDATE:
            case UTCTIMEONLY:
            case UTCDATEONLY:
            case MONTHYEAR:
            case TZTIMEONLY:
            case TZTIMESTAMP:
                return resetTemporalValue(name);

            default:
                throw new IllegalArgumentException("Unknown type: " + field.type());
        }
    }

    protected abstract String resetRequiredInt(Field field);

    protected abstract String optionalReset(Field field, String name);

    protected abstract String resetTemporalValue(String name);

    protected abstract String resetComponents(List<Entry> entries, StringBuilder methods);

    protected abstract String resetStringBasedData(String name);

    protected String nameOfResetMethod(final String name)
    {
        return "reset" + name;
    }

    private String callResetMethod(final Entry entry)
    {
        if (isNotResettableField(entry.name()))
        {
            return "";
        }

        return String.format(
            "        %1$s();\n",
            nameOfResetMethod(entry.name()));
    }

    protected String callComponentReset(final Entry entry)
    {
        return String.format(
            "        %1$s.reset();\n",
            formatPropertyName(entry.name()));
    }

    protected String hasField(final Entry entry)
    {
        final String name = entry.name();
        return entry.required() ?
            "" :
            String.format("    private boolean has%1$s;\n\n", name);
    }

    protected String resetNothing(final String name)
    {
        return String.format(
            "    public void %1$s()\n" +
            "    {\n" +
            "    }\n\n",
            nameOfResetMethod(name));
    }

    private boolean isNotResettableField(final String name)
    {
        return isDerivedField(name) || isPreCalculatedField(name);
    }

    private boolean isPreCalculatedField(final String name)
    {
        return "MessageName".equals(name) || BEGIN_STRING.equals(name) || MSG_TYPE.equals(name);
    }

    private boolean isDerivedField(final String name)
    {
        return isBodyLength(name) || isCheckSum(name);
    }

    protected abstract String resetRequiredFloat(String name);

    protected String resetLength(final String name)
    {
        return String.format(
            "    public void %1$s()\n" +
            "    {\n" +
            "        %2$sLength = 0;\n" +
            "    }\n\n",
            nameOfResetMethod(name),
            formatPropertyName(name));
    }

    protected String resetByFlag(final String name)
    {
        return String.format(
            "    public void %2$s()\n" +
            "    {\n" +
            "        has%1$s = false;\n" +
            "    }\n\n",
            name,
            nameOfResetMethod(name));
    }

    protected String resetByMethod(final String name)
    {
        return String.format(
            "    public void %2$s()\n" +
            "    {\n" +
            "        %1$s.reset();\n" +
            "    }\n\n",
            formatPropertyName(name),
            nameOfResetMethod(name));
    }

    protected String resetFieldValue(final String name, final String resetValue)
    {
        return String.format(
            "    public void %1$s()\n" +
            "    {\n" +
            "        %2$s = %3$s;\n" +
            "    }\n\n",
            nameOfResetMethod(name),
            formatPropertyName(name),
            resetValue);
    }

    protected String toString(final Aggregate aggregate, final boolean hasCommonCompounds)
    {
        final String entriesToString = aggregate
            .entries()
            .stream()
            .map(this::entryToString)
            .collect(joining(" + \n"));

        final String prefix = !hasCommonCompounds ?
            "" : "\"  \\\"header\\\": \" + header" + EXPAND_INDENT + " + \"\\n\" + ";

        final String suffix;
        final String parameters;
        if (aggregate instanceof Group)
        {
            suffix = toStringGroupSuffix();

            parameters = toStringGroupParameters();
        }
        else
        {
            suffix = "";
            parameters = "";
        }

        return String.format(
            "    public String toString(%5$s)\n" +
            "    {\n" +
            "        String entries = %1$s\n" +
            "%2$s;\n\n" +
            "        entries = \"{\\n  \\\"MessageName\\\": \\\"%4$s\\\",\\n\" + entries + \"}\";\n" +
            "%3$s" +
            "        return entries;\n" +
            "    }\n\n",
            prefix,
            entriesToString,
            suffix,
            aggregate.name(),
            parameters);
    }

    protected abstract String toStringGroupParameters();

    protected abstract String toStringGroupSuffix();

    protected String entryToString(final Entry entry)
    {
        //"  \"OnBehalfOfCompID\": \"abc\",\n" +

        if (isBodyLength(entry))
        {
            return "\"\"";
        }

        final Element element = entry.element();
        final String name = entry.name();
        if (element instanceof Field)
        {
            final Field field = (Field)element;
            final String value = fieldToString(field);

            final String formatter = String.format(
                "String.format(\"  \\\"%s\\\": \\\"%%s\\\",\\n\", %s)",
                name,
                value);

            final boolean hasFlag = toStringChecksHasGetter(entry, field);
            return "             " +
                (hasFlag ? String.format("(has%s() ? %s : \"\")", name, formatter) : formatter);
        }
        else if (element instanceof Group)
        {
            return groupEntryToString((Group)element, name);
        }
        else if (element instanceof Component)
        {
            return componentToString((Component)element);
        }

        return "\"\"";
    }

    protected abstract boolean toStringChecksHasGetter(Entry entry, Field field);

    protected abstract String groupEntryToString(Group element, String name);

    protected abstract boolean hasFlag(Entry entry, Field field);

    protected String hasGetter(final String name)
    {
        return String.format(
            "    public boolean has%s()\n" +
            "    {\n" +
            "        return has%1$s;\n" +
            "    }\n\n",
            name);
    }

    protected abstract String componentToString(Component component);

    protected String fieldToString(final Field field)
    {
        final String fieldName = formatPropertyName(field.name());
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
            case UTCTIMEONLY:
            case UTCDATEONLY:
            case UTCTIMESTAMP:
            case LOCALMKTDATE:
            case MONTHYEAR:
            case TZTIMEONLY:
            case TZTIMESTAMP:
                return stringToString(fieldName);

            case DATA:
            case XMLDATA:
                return String.format("Arrays.toString(%s)", fieldName);

            default:
                return fieldName;
        }
    }

    protected boolean isCheckSum(final Entry entry)
    {
        return entry != null && isCheckSum(entry.name());
    }

    private boolean isCheckSum(final String name)
    {
        return "CheckSum".equals(name);
    }

    protected boolean isBodyLength(final Entry entry)
    {
        return entry != null && isBodyLength(entry.name());
    }

    protected boolean isBeginString(final Entry entry)
    {
        return entry != null && BEGIN_STRING.equals(entry.name());
    }

    protected boolean isBodyLength(final String name)
    {
        return BODY_LENGTH.equals(name);
    }

    protected abstract String stringToString(String fieldName);

    protected String indent(final int times, final String suffix)
    {
        final StringBuilder sb = new StringBuilder(times * 4 + suffix.length());
        IntStream.range(0, times).forEach((ignore) -> sb.append("    "));
        sb.append(suffix);

        return sb.toString();
    }
}
