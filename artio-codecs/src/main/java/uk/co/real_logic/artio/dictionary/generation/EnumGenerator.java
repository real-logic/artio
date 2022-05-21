/*
 * Copyright 2013 Real Logic Limited.
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

import org.agrona.LangUtil;
import org.agrona.collections.IntHashSet;
import org.agrona.generation.OutputManager;
import uk.co.real_logic.artio.builder.CharRepresentable;
import uk.co.real_logic.artio.builder.IntRepresentable;
import uk.co.real_logic.artio.builder.StringRepresentable;
import uk.co.real_logic.artio.dictionary.CharArrayMap;
import uk.co.real_logic.artio.dictionary.CharArrayWrapper;
import uk.co.real_logic.artio.dictionary.Generated;
import uk.co.real_logic.artio.dictionary.ir.Dictionary;
import uk.co.real_logic.artio.dictionary.ir.Field;
import uk.co.real_logic.artio.dictionary.ir.Field.Type;
import uk.co.real_logic.artio.dictionary.ir.Field.Value;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static uk.co.real_logic.artio.dictionary.generation.CodecUtil.*;
import static uk.co.real_logic.artio.dictionary.generation.GenerationUtil.*;
import static uk.co.real_logic.sbe.generation.java.JavaUtil.formatClassName;

final class EnumGenerator
{
    static final String NULL_VAL_NAME = "NULL_VAL";
    public static final String NULL_VAL_CHAR_AS_STRING = Character.toString(ENUM_MISSING_CHAR);
    public static final String NULL_VAL_INT_AS_STRING = Integer.toString(ENUM_MISSING_INT);
    public static final String NULL_VAL_STRING = ENUM_MISSING_STRING;

    static final String UNKNOWN_NAME = "ARTIO_UNKNOWN";
    public static final String UNKNOWN_CHAR_AS_STRING = Character.toString(ENUM_UNKNOWN_CHAR);
    public static final String UNKNOWN_INT_AS_STRING = Integer.toString(ENUM_UNKNOWN_INT);
    public static final String UNKNOWN_STRING = ENUM_UNKNOWN_STRING;

    private final Dictionary dictionary;
    private final String builderPackage;
    private final OutputManager outputManager;

    static String enumName(final String name)
    {
        return formatClassName(name);
    }

    EnumGenerator(
        final Dictionary dictionary,
        final String builderPackage,
        final OutputManager outputManager)
    {
        this.dictionary = dictionary;
        this.builderPackage = builderPackage;
        this.outputManager = outputManager;
    }

    public void generate()
    {
        if (dictionary.hasSharedParent())
        {
            return;
        }

        dictionary
            .fields()
            .values()
            .stream()
            .filter(EnumGenerator::hasEnumGenerated)
            .forEach(this::generateEnum);
    }

    static boolean hasEnumGenerated(final Field field)
    {
        return field.isEnum() && field.type() != Type.BOOLEAN;
    }

    private void generateEnum(final Field field)
    {
        final String enumName = enumName(field.name());
        final Type type = field.type();
        final List<Value> values = field.values();
        final String nullValue;
        final String unknownValue;
        final String interfaceToImplement;
        final String interfaceToImport;

        if (isCharBased(type))
        {
            nullValue = NULL_VAL_CHAR_AS_STRING;
            unknownValue = UNKNOWN_CHAR_AS_STRING;
            interfaceToImplement = "Char";
            interfaceToImport = importFor(CharRepresentable.class);
        }
        else if (type.isIntBased())
        {
            nullValue = NULL_VAL_INT_AS_STRING;
            unknownValue = UNKNOWN_INT_AS_STRING;
            interfaceToImplement = "Int";
            interfaceToImport = importFor(IntRepresentable.class);
        }
        else if (type.isStringBased())
        {
            nullValue = NULL_VAL_STRING;
            unknownValue = UNKNOWN_STRING;
            interfaceToImplement = "String";
            interfaceToImport = importFor(StringRepresentable.class);
        }
        else
        {
            System.err.printf("Unable to generate an enum for type: %s. No sentinel defined for %s\n", enumName, type);
            return;
        }

        final List<Value> valuesWithSentinels = new ArrayList<>(values);
        valuesWithSentinels.add(new Value(nullValue, NULL_VAL_NAME));
        valuesWithSentinels.add(new Value(unknownValue, UNKNOWN_NAME));

        outputManager.withOutput(enumName, (out) ->
        {
            try
            {
                out.append(fileHeader(builderPackage));
                out.append(importFor(CharArrayMap.class));
                out.append(importFor(CharArrayWrapper.class));
                out.append(importFor(IntHashSet.class));
                out.append(importFor(Map.class));
                out.append(importFor(HashMap.class));
                out.append(interfaceToImport);
                out.append(importFor(Generated.class));
                out.append("\n" + GENERATED_ANNOTATION);
                out.append(generateEnumDeclaration(enumName, interfaceToImplement));

                out.append(generateEnumValues(valuesWithSentinels, type));

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

    private boolean isCharBased(final Type type)
    {
        return type == Type.CHAR;
    }

    private String generateEnumDeclaration(final String name, final String interfaceToImplement)
    {
        final String format = "public enum " + name + " implements %sRepresentable\n{\n";
        return String.format(format, interfaceToImplement);
    }

    private String generateEnumValues(final List<Value> allValues, final Type type)
    {
        return allValues
            .stream()
            .map((value) ->
            {
                String javadoc = "";
                final List<String> alternativeNames = value.alternativeNames();
                if (alternativeNames != null)
                {
                    javadoc = alternativeNames.stream().collect(
                        joining(", ", "/** Altnames: ", " */ "));
                }
                return format("%1$s%4$s%2$s(%3$s)", INDENT, value.description(), literal(value, type), javadoc);
            })
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
        final String enumValidation = enumValidation(allValues, type);

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
            "        default:\n" +
            "            return %s;\n" +
            "        }\n" +
            "    }\n",
            optionalCharArrayDecode,
            enumValidation,
            name,
            representation.methodArgsDeclaration(),
            cases,
            UNKNOWN_NAME);
    }

    private String enumValidation(final List<Value> allValues, final Type type)
    {
        switch (type)
        {
            case MULTIPLEVALUESTRING:
            case MULTIPLESTRINGVALUE:
            case MULTIPLECHARVALUE:
            case STRING:
            case CURRENCY:
            case EXCHANGE:
            case COUNTRY:
            case LANGUAGE:
                return "    public static boolean isValid(final CharArrayWrapper key)\n" +
                       "    {\n" +
                       "        return charMap.containsKey(key);\n" +
                       "    }\n";
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
            case MULTIPLECHARVALUE:
            case CURRENCY:
            case EXCHANGE:
            case COUNTRY:
            case LANGUAGE:

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
                    "    public static %1$s decode(final CharArrayWrapper key)\n" +
                    "    {\n" +
                            "        final %1$s value = charMap.get(key);\n" +
                            "        if (value == null)\n" +
                            "        {\n" +
                            "            return %3$s;\n" +
                            "        }\n" +
                            "        return value;\n" +
                    "    }\n",
                    typeName,
                    entries,
                    UNKNOWN_NAME);

            default:
                return "";
        }
    }

    private boolean hasGeneratedValueOf(final Type type)
    {
        switch (type)
        {
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
            case MULTIPLECHARVALUE:
            case CURRENCY:
            case EXCHANGE:
            case COUNTRY:
            case LANGUAGE:
            case UTCTIMEONLY:
            case UTCDATEONLY:
            case MONTHYEAR:
                argTypeValue = typeValue = "String";
                break;
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
                //noinspection ResultOfMethodCallIgnored
                Integer.parseInt(representation);
                return representation;

            case LONG:
                // Validate that the representation genuinely is a parseable int
                //noinspection ResultOfMethodCallIgnored
                Long.parseLong(representation);
                return representation;

            case STRING:
            case MULTIPLECHARVALUE:
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
