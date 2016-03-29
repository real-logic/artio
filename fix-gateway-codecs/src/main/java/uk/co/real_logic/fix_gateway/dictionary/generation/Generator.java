/*
 * Copyright 2015-2016 Real Logic Ltd.
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

import org.agrona.MutableDirectBuffer;
import org.agrona.collections.IntHashSet;
import org.agrona.collections.IntIterator;
import org.agrona.generation.OutputManager;
import uk.co.real_logic.fix_gateway.EncodingException;
import uk.co.real_logic.fix_gateway.dictionary.CharArraySet;
import uk.co.real_logic.fix_gateway.dictionary.StandardFixConstants;
import uk.co.real_logic.fix_gateway.dictionary.ir.*;
import uk.co.real_logic.fix_gateway.dictionary.ir.Entry.Element;
import uk.co.real_logic.fix_gateway.fields.DecimalFloat;
import uk.co.real_logic.fix_gateway.fields.LocalMktDateEncoder;
import uk.co.real_logic.fix_gateway.fields.UtcTimestampEncoder;
import uk.co.real_logic.fix_gateway.util.AsciiBuffer;
import uk.co.real_logic.fix_gateway.util.MutableAsciiBuffer;

import javax.annotation.Generated;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import static java.util.stream.Collectors.joining;
import static uk.co.real_logic.fix_gateway.dictionary.generation.AggregateType.*;
import static uk.co.real_logic.fix_gateway.dictionary.generation.GenerationUtil.importFor;
import static uk.co.real_logic.fix_gateway.dictionary.generation.GenerationUtil.importStaticFor;
import static uk.co.real_logic.sbe.generation.java.JavaUtil.formatPropertyName;

public abstract class Generator
{

    protected static final String MSG_TYPE = "MsgType";
    public static final String EXPAND_INDENT = ".toString().replace(\"\\n\", \"\\n  \")";
    public static final String CODEC_VALIDATION_ENABLED = "CODEC_VALIDATION_ENABLED";

    protected String commonCompoundImports(final String form)
    {
        return String.format(
            "    private Header%s header = new Header%1$s();\n\n" +
            "    public Header%1$s header() {\n" +
            "        return header;\n" +
            "    }\n\n" +

            "    private Trailer%1$s trailer = new Trailer%1$s();\n\n" +
            "    public Trailer%1$s trailer() {\n" +
            "        return trailer;\n" +
            "    }\n\n",
            form);
    }

    private static final String COMMON_COMPOUND_IMPORTS =
            "import %1$s.Header%4$s;\n" +
            "import %1$s.Trailer%4$s;\n";

    protected final Dictionary dictionary;
    protected final String builderPackage;
    protected final OutputManager outputManager;
    protected final Class<?> validationClass;

    private final Set<String> groupNames = new HashSet<>();

    protected Generator(
        final Dictionary dictionary,
        final String builderPackage,
        final OutputManager outputManager,
        final Class<?> validationClass)
    {
        this.dictionary = dictionary;
        this.builderPackage = builderPackage;
        this.outputManager = outputManager;
        this.validationClass = validationClass;
    }

    public void generate()
    {
        aggregate(dictionary.header(), AggregateType.HEADER);
        aggregate(dictionary.trailer(), AggregateType.TRAILER);
        dictionary.components().forEach((name, component) -> aggregate(component, COMPONENT));
        dictionary.messages().forEach(msg -> aggregate(msg, MESSAGE));
    }

    protected abstract void aggregate(final Aggregate aggregate, final AggregateType type);

    protected void group(final Group group)
    {
        final String name = group.name();
        if (groupNames.add(name))
        {
            aggregate(group, GROUP);
        }
    }

    protected String classDeclaration(
        final String className,
        final AggregateType type,
        final List<String> interfaces,
        final String compoundSuffix,
        final Class<?> topType)
    {
        final String interfaceList = interfaces.isEmpty() ? "" : (", " + String.join(", ", interfaces));

        return String.format(
            importFor(MutableDirectBuffer.class) +
            importStaticFor(CodecUtil.class) +
            importStaticFor(StandardFixConstants.class) +
            importFor(topType) +
            (type == MESSAGE ? COMMON_COMPOUND_IMPORTS : "") +
            importFor(DecimalFloat.class) +
            importFor(MutableAsciiBuffer.class) +
            importFor(AsciiBuffer.class) +
            importFor(LocalMktDateEncoder.class) +
            importFor(UtcTimestampEncoder.class) +
            importFor(StandardCharsets.class) +
            importFor(Arrays.class) +
            importFor(CharArraySet.class) +
            importFor(IntHashSet.class) +
            importFor(IntIterator.class) +
            importFor(Generated.class) +
            importFor(EncodingException.class) +
            importStaticFor(StandardCharsets.class, "US_ASCII") +
            importStaticFor(validationClass, CODEC_VALIDATION_ENABLED) +
            String.format("\n@Generated(\"%s\")\n", getClass().getName()) +
            "public class %2$s implements %5$s%3$s\n" +
            "{\n",
            builderPackage,
            className,
            interfaceList,
            compoundSuffix,
            topType.getSimpleName());
    }

    protected String completeResetMethod(
        final boolean isMessage,
        final List<Entry> entries,
        final String additionalReset)
    {
        final StringBuilder methods = new StringBuilder();

        final String resetEntries = resetEntries(entries, methods);

        final String resetHeaderAndTrailer = isMessage
            ? "        header.reset();\n" +
              "        trailer.reset();\n"
            : "";

        return String.format(
            "    public void reset()\n" +
            "    {\n" +
            "%s" +
            "%s" +
            "%s" +
            "    }\n\n" +
            "%s",
            resetHeaderAndTrailer,
            resetEntries,
            additionalReset,
            methods
        );
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
            entry -> resetField(entry.required(), (Field) entry.element()),
            this::callResetMethod);
    }

    protected String resetAllBy(final List<Entry> entries,
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
        final Group group = (Group) entry.element();
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
            numberField.name()
        );
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
            return resetByFlag(name);
        }

        switch (field.type())
        {
            case INT:
            case LENGTH:
            case SEQNUM:
            case NUMINGROUP:
            case DAYOFMONTH:
                return resetFieldValue(name, "MISSING_INT");

            case FLOAT:
            case PRICE:
            case PRICEOFFSET:
            case QTY:
            case PERCENTAGE:
            case AMT:
                return resetFloat(name);

            case CHAR:
                return resetFieldValue(name, "MISSING_CHAR");

            case DATA:
                return resetFieldValue(name, "null");

            case BOOLEAN:
                return resetFieldValue(name, "false");

            case STRING:
            case MULTIPLEVALUESTRING:
            case CURRENCY:
            case EXCHANGE:
            case COUNTRY:
                return resetLength(name);

            case UTCTIMESTAMP:
            case LOCALMKTDATE:
            case UTCTIMEONLY:
            case UTCDATEONLY:
            case MONTHYEAR:
                return resetTemporalValue(name);

            default:
                throw new IllegalArgumentException("Unknown type: " + field.type());
        }
    }

    protected abstract String resetTemporalValue(final String name);

    protected abstract String resetComponents(final List<Entry> entries, final StringBuilder methods);

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
            nameOfResetMethod(entry.name())
        );
    }

    protected String callComponentReset(final Entry entry)
    {
        return String.format(
            "        %1$s.reset();\n",
            formatPropertyName(entry.name())
        );
    }

    protected String hasField(final Entry entry)
    {
        final String name = entry.name();
        return entry.required()
             ? ""
             : String.format("    private boolean has%1$s;\n\n", name);
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
        return isDerivedField(name) || isPrecalculatedField(name);
    }

    private boolean isPrecalculatedField(final String name)
    {
        return "MessageName".equals(name) || "BeginString".equals(name) || "MsgType".equals(name);
    }

    private boolean isDerivedField(final String name)
    {
        return isBodyLength(name) || isCheckSum(name);
    }

    protected abstract String resetFloat(final String name);

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
            nameOfResetMethod(name)
        );
    }

    protected String resetByMethod(final String name)
    {
        return String.format(
            "    public void %2$s()\n" +
                "    {\n" +
                "        %1$s.reset();\n" +
                "    }\n\n",
            formatPropertyName(name),
            nameOfResetMethod(name)
        );
    }

    private String resetFieldValue(final String name, final String resetValue)
    {
        return String.format(
            "    public void %1$s()\n" +
            "    {\n" +
            "        %2$s = %3$s;\n" +
            "    }\n\n",
            nameOfResetMethod(name),
            formatPropertyName(name),
            resetValue
        );
    }

    protected String toString(final Aggregate aggregate, final boolean hasCommonCompounds)
    {
        final String entriesToString =
                aggregate.entries()
                        .stream()
                        .map(this::entryToString)
                        .collect(joining(" + \n"));

        final String prefix =
            !hasCommonCompounds ? ""
            : "\"  \\\"header\\\": \" + header" + EXPAND_INDENT + " + \"\\n\" + ";

        final String suffix =
            !(aggregate instanceof Group) ? ""
            : "        if (next != null)\n" +
              "        {\n" +
              "            entries += \",\\n\" + next.toString();\n" +
              "        }\n";

        return String.format(
            "    public String toString()\n" +
                "    {\n" +
                "        String entries =%1$s\n" +
                "%2$s;\n\n" +
                "        entries = \"{\\n  \\\"MessageName\\\": \\\"%4$s\\\",\\n\" + entries + \"}\";\n" +
                "%3$s" +
                "        return entries;\n" +
                "    }\n\n",
            prefix,
            entriesToString,
            suffix,
            aggregate.name());
    }

    protected String entryToString(final Entry entry)
    {
        //"  \"OnBehalfOfCompID\": \"abc\",\n" +

        final Element element = entry.element();
        final String name = entry.name();
        if (element instanceof Field)
        {
            final Field field = (Field) element;
            final String value = fieldToString(field);

            final String formatter = String.format(
                "String.format(\"  \\\"%s\\\": \\\"%%s\\\",\\n\", %s)",
                name,
                value
            );

            final boolean hasFlag = hasFlag(entry, field);
            return "             " + (hasFlag ? String.format("(has%s ? %s : \"\")", name, formatter) : formatter);
        }
        else if (element instanceof Group)
        {
            return String.format(
                "                (%2$s != null ? String.format(\"  \\\"%1$s\\\": [\\n" +
                "  %%s" +
                "\\n  ]" +
                "\\n\", %2$s" + EXPAND_INDENT + ") : \"\")",
                name,
                formatPropertyName(name)
            );
        }
        else if (element instanceof Component)
        {
            return componentToString((Component)element);
        }

        return "\"\"";
    }

    protected abstract boolean hasFlag(final Entry entry, final Field field);

    protected String hasGetter(final String name)
    {
        return String.format(
            "    public boolean has%s()\n" +
            "    {\n" +
            "        return has%1$s;\n" +
            "    }\n\n",
            name);
    }

    protected abstract String componentToString(final Component component);

    protected String fieldToString(final Field field)
    {
        final String fieldName = formatPropertyName(field.name());
        switch (field.type())
        {
            case STRING:
            case MULTIPLEVALUESTRING:
            case CURRENCY:
            case EXCHANGE:
            case COUNTRY:
            case UTCTIMEONLY:
            case UTCDATEONLY:
            case UTCTIMESTAMP:
            case LOCALMKTDATE:
            case MONTHYEAR:
                return stringToString(fieldName);

            case DATA:
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

    protected boolean isBodyLength(final String name)
    {
        return "BodyLength".equals(name);
    }

    protected abstract String stringToString(String fieldName);
}
