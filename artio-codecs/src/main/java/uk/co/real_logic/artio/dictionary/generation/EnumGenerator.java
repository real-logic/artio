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
package uk.co.real_logic.artio.dictionary.generation;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.agrona.LangUtil;
import org.agrona.collections.IntHashSet;
import org.agrona.generation.OutputManager;


import uk.co.real_logic.artio.dictionary.CharArrayMap;
import uk.co.real_logic.artio.dictionary.ir.Dictionary;
import uk.co.real_logic.artio.dictionary.ir.Field;
import uk.co.real_logic.artio.dictionary.ir.Field.Type;
import uk.co.real_logic.artio.dictionary.ir.Field.Value;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static uk.co.real_logic.artio.dictionary.generation.GenerationUtil.INDENT;
import static uk.co.real_logic.artio.dictionary.generation.GenerationUtil.Var;
import static uk.co.real_logic.artio.dictionary.generation.GenerationUtil.constructor;
import static uk.co.real_logic.artio.dictionary.generation.GenerationUtil.fileHeader;
import static uk.co.real_logic.artio.dictionary.generation.GenerationUtil.importFor;
import static uk.co.real_logic.artio.dictionary.generation.GenerationUtil.optionalStaticInit;

public final class EnumGenerator
{
    private final Dictionary dictionary;
    private final String builderPackage;
    private final OutputManager outputManager;

    public EnumGenerator(final Dictionary dictionary, final String builderPackage, final OutputManager outputManager)
    {
        this.dictionary = dictionary;
        this.builderPackage = builderPackage;
        this.outputManager = outputManager;
    }

    public void generate()
    {
        dictionary
            .fields()
            .values()
            .stream()
            .filter(EnumGenerator::hasEnumGenerated)
            .forEach(this::generateEnum);
    }

    public static boolean hasEnumGenerated(final Field field)
    {
        return field.isEnum() && field.type() != Type.BOOLEAN;
    }

    private void generateEnum(final Field field)
    {
        final String enumName = field.name();
        final Type type = field.type();
        final List<Value> values = field.values();

        outputManager.withOutput(enumName, (out) ->
        {
            try
            {
                out.append(fileHeader(builderPackage));
                out.append(importFor(CharArrayMap.class));
                out.append(importFor(IntHashSet.class));
                out.append(importFor(Map.class));
                out.append(importFor(HashMap.class));
                out.append(generateEnumDeclaration(enumName));

                out.append(generateEnumValues(values, type));

                out.append(generateEnumBody(enumName, type));
                out.append(generateEnumLookupMethod(enumName, values, type));
            }
            catch (final IOException e)
            {
                LangUtil.rethrowUnchecked(e);
            }
            catch (final IllegalArgumentException e)
            {
                System.err.printf("Unable to generate an enum for type: %s\n", enumName);
                System.err.println(e.getMessage());
            }
            finally
            {
                out.append("}\n");
            }
        });
    }

    private String generateEnumDeclaration(final String name)
    {
        return "public enum " + name + "\n{\n";
    }

    private String generateEnumValues(final List<Value> allValues, final Type type)
    {
        return allValues
            .stream()
            .map((value) -> format("%s%s(%s)", INDENT, value.description(), literal(value, type)))
            .collect(joining(",\n"));
    }

    private String generateEnumBody(final String name, final Type type)
    {
        final Var representation = representation(type);

        return ";\n\n" +
               representation.field() +
               constructor(name, representation) +
               representation.getter();
    }

    private String generateEnumLookupMethod(final String name, final List<Value> allValues, final Type type)
    {
        if (hasGeneratedValueOf(type))
        {
            return "";
        }

        final String optionalCharArrayDecode = optionalCharArrayDecode(name, allValues, type);
        final String enumValidation = enumValidation(name, allValues, type);

        final Var representation = representation(type);

        final String cases = allValues
            .stream()
            .map((value) -> format("        case %s: return %s;\n", literal(value, type), value.description()))
            .collect(joining());

        return format(
            "%s" +
            "%s" +
            "    public static %s decode(%s)\n" +
            "    {\n" +
            "        switch(representation)\n" +
            "        {\n" +
            "%s" +
            "        default: throw new IllegalArgumentException(\"Unknown: \" + representation);\n" +
            "        }\n" +
            "    }\n",
            optionalCharArrayDecode,
            enumValidation,
            name,
            representation.methodArgsDeclaration(),
            cases);
    }

    private String enumValidation(final String typeName, final List<Value> allValues, final Type type)
    {
        switch (type)
        {
            case STRING:
                return "    public static boolean isValid(final char[] representation, final int length)\n" +
                       "    {\n" +
                       "        return charMap.containsKey(representation, length);\n" +
                       "    }\n";

            case MULTIPLEVALUESTRING:
            case MULTIPLESTRINGVALUE:
                return "    public static boolean isValid(final char[] representation, final int length)\n" +
                       "    {\n" +
                       "        int offset = 0;\n" +
                       "        for (int i = 0; i < length; i++)\n" +
                       "        {\n" +
                       "            if (representation[i] == ' ')\n" +
                       "            {\n" +
                       "                if (! charMap.containsKey(representation, offset, i - offset))\n" +
                       "                    return false;\n" +
                       "                offset = i + 1;\n" +
                       "            }\n" +
                       "        }\n" +
                       "        return charMap.containsKey(representation, offset, length - offset);\n" +
                       "    }\n";

            case MULTIPLECHARVALUE:
                final String multiCharValues = allValues
                    .stream()
                    .map(Field.Value::representation)
                    .map((repr) -> String.format("'%1$s'", repr))
                    .map((repr) -> String.format("        intSet.add(%1$s);\n", repr))
                    .collect(joining());

                return format(
                    "    private static final IntHashSet intSet = new IntHashSet(%2$s);\n" +
                    "    %1$s\n" +
                    "\n" +
                    "    public static boolean isValid(final char[] representation, final int length)\n" +
                    "    {\n" +
                    "        for (int i = 0; i < length; i+=2)\n" +
                    "        {\n" +
                    "            if (! intSet.contains(representation[i]))\n" +
                    "                return false;\n" +
                    "        }\n" +
                    "        return true;\n" +
                    "    }\n",
                    optionalStaticInit(multiCharValues),
                    ConstantGenerator.sizeHashSet(allValues));
            default:
                final String primitiveValues = allValues
                    .stream()
                    .map(value -> literal(value, type))
                    .map((repr) -> String.format("        intSet.add(%1$s);\n", repr))
                    .collect(joining());

                return format(
                    "    private static final IntHashSet intSet = new IntHashSet(%2$s);\n" +
                    "    %1$s\n" +
                    "\n" +
                    "    public static boolean isValid(final int representation)\n" +
                    "    {\n" +
                    "        return intSet.contains(representation);\n" +
                    "    }\n",
                    optionalStaticInit(primitiveValues),
                    ConstantGenerator.sizeHashSet(allValues));
        }
    }

    private String optionalCharArrayDecode(final String typeName, final List<Value> allValues, final Type type)
    {
        switch (type)
        {
            case STRING:
            case MULTIPLEVALUESTRING:
            case MULTIPLESTRINGVALUE:
                final String entries = allValues
                    .stream()
                    .map((v) -> format("        stringMap.put(%s, %s);\n", literal(v, type), v.description()))
                    .collect(joining());

                return format(
                    "    private static final CharArrayMap<%1$s> charMap;\n" +
                    "    static\n" +
                    "    {\n" +
                    "        final Map<String, %1$s> stringMap = new HashMap<>();\n" +
                    "%2$s" +
                    "        charMap = new CharArrayMap<>(stringMap);\n" +
                    "    }\n" +
                    "\n" +
                    "    public static %1$s decode(final char[] representation, final int length)\n" +
                    "    {\n" +
                    "        return charMap.get(representation, length);\n" +
                    "    }\n",
                    typeName,
                    entries);
            case MULTIPLECHARVALUE:

                return format(
                    "    public static %1$s decode(String representation)\n" +
                    "    {\n" +
                    "        return decode(representation.charAt(0));\n" +
                    "    }\n" +
                    "\n" +
                    "    public static %1$s decode(final char[] representation, final int length)\n" +
                    "    {\n" +
                    "        return decode(representation[0]);\n" +
                    "    }\n",
                    typeName);
            default:
                return "";
        }
    }

    private boolean hasGeneratedValueOf(final Type type)
    {
        switch (type)
        {
            case CURRENCY:
            case EXCHANGE:
            case COUNTRY:
            case LANGUAGE:
            case UTCTIMEONLY:
            case UTCDATEONLY:
            case MONTHYEAR:
                return true;

            default:
                return false;
        }
    }

    private Var representation(final Type type)
    {
        final String typeValue;
        final String argTypeValue;
        switch (type)
        {
            case STRING:
            case MULTIPLEVALUESTRING:
            case MULTIPLESTRINGVALUE:
            case CURRENCY:
            case EXCHANGE:
            case COUNTRY:
            case LANGUAGE:
            case UTCTIMEONLY:
            case UTCDATEONLY:
            case MONTHYEAR:
                argTypeValue = typeValue = "String";
                break;
            case MULTIPLECHARVALUE:
            case CHAR:
                typeValue = "char";
                argTypeValue = "int";
                break;
            default:
                argTypeValue = typeValue = "int";
        }

        return new Var(typeValue, argTypeValue, "representation");
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
            case DAYOFMONTH:
                // Validate that the representation genuinely is a parseable int
                Integer.parseInt(representation);
                return representation;

            case STRING:
            case MULTIPLEVALUESTRING:
            case MULTIPLESTRINGVALUE:
            case CURRENCY:
            case EXCHANGE:
            case COUNTRY:
            case LANGUAGE:
            case UTCTIMEONLY:
            case UTCDATEONLY:
            case MONTHYEAR:
                return '"' + representation + '"';

            case MULTIPLECHARVALUE:
            case CHAR:
                if (representation.length() > 1)
                {
                    throw new IllegalArgumentException(
                        representation + " has a length of 2 and thus won't fit into a char");
                }
                return "'" + representation + "'";

            default:
                throw new IllegalArgumentException(
                    "Unknown type for creating an enum from: " + type + " for value " + value.description());
        }
    }
}
