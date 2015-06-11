/*
 * Copyright 2013 Real Logic Ltd.
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
import uk.co.real_logic.agrona.generation.OutputManager;
import uk.co.real_logic.fix_gateway.dictionary.ir.Dictionary;
import uk.co.real_logic.fix_gateway.dictionary.ir.Field;
import uk.co.real_logic.fix_gateway.dictionary.ir.Field.Type;
import uk.co.real_logic.fix_gateway.dictionary.ir.Field.Value;

import java.io.IOException;
import java.io.Writer;
import java.util.List;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static uk.co.real_logic.fix_gateway.dictionary.generation.GenerationUtil.*;

public final class EnumGenerator
{
    private final Dictionary dictionary;
    private final OutputManager outputManager;

    public EnumGenerator(final Dictionary dictionary, final OutputManager outputManager)
    {
        this.dictionary = dictionary;
        this.outputManager = outputManager;
    }

    public void generate()
    {
        dictionary
            .fields()
            .values()
            .stream()
            .filter(Field::isEnum)
            .forEach(this::generateEnum);
    }

    private void generateEnum(final Field field)
    {
        final String enumName = field.name();
        final Type type = field.type();
        final List<Value> values = field.values();

        try (final Writer out = outputManager.createOutput(enumName))
        {
            out.append(fileHeader(PARENT_PACKAGE));
            out.append(generateEnumDeclaration(enumName));

            out.append(generateEnumValues(values, type));

            out.append(generateEnumBody(enumName));
            out.append(generateEnumLookupMethod(enumName, values, type));

            out.append("}\n");
        }
        catch (final IOException e)
        {
            LangUtil.rethrowUnchecked(e);
        }
    }

    private String generateEnumDeclaration(final String name)
    {
        return "public enum " + name + "\n{\n";
    }

    private String generateEnumValues(final List<Value> allValues, final Type type)
    {
        return allValues.stream()
                        .map((value) -> format("%s%s(%s)", INDENT, value.description(), literal(value, type)))
                        .collect(joining(",\n"));
    }

    private String generateEnumBody(final String name)
    {
        final Var representation = new Var("int", "representation");

        return ";\n\n" +
            representation.field() +
            constructor(name, representation) +
            representation.getter();
    }

    private String generateEnumLookupMethod(final String name, final List<Value> allValues, final Type type)
    {
        final Var representation = new Var("int", "representation");

        final String cases = allValues
            .stream()
            .map((value) -> format("        case %s: return %s;\n", literal(value, type), value.description()))
            .collect(joining());

        return method("valueOf", name, representation) +
            "        switch(representation)\n" +
            "        {\n" +
            cases +
            "        default: throw new IllegalArgumentException(\"Unknown: \" + representation);\n" +
            "        }\n" +
            "    }\n";
    }

    private String literal(final Value value, final Type type)
    {
        final String representation = value.representation();

        switch (type)
        {
            case INT:
            case LENGTH:
            case SEQNUM:
            case NUMINGROUP:
                Integer.parseInt(representation);
                return representation;

            case STRING:
            case MULTIPLEVALUESTRING:
            case CURRENCY:
            case EXCHANGE:
            case COUNTRY:
            case UTCTIMEONLY:
            case UTCDATEONLY:
            case MONTHYEAR:
                return String.valueOf(getMessageType(representation));

            case CHAR:
                return "'" + representation + "'";

            default:
                final String msg = "Unknown type for creating an enum from: " + type + " for value" + value.description();
                throw new IllegalArgumentException(msg);
        }
    }

}
