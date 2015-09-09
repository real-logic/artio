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

import uk.co.real_logic.agrona.collections.IntHashSet;
import uk.co.real_logic.agrona.generation.OutputManager;
import uk.co.real_logic.fix_gateway.dictionary.ir.Dictionary;
import uk.co.real_logic.fix_gateway.dictionary.ir.Field;

import java.util.Collection;

import static java.lang.Character.isUpperCase;
import static java.lang.Character.toUpperCase;
import static java.util.stream.Collectors.joining;
import static uk.co.real_logic.fix_gateway.dictionary.generation.GenerationUtil.fileHeader;
import static uk.co.real_logic.fix_gateway.dictionary.generation.GenerationUtil.importFor;

public class ConstantGenerator
{
    public static final String CLASS_NAME = "Constants";

    private static final String BODY =
        "public class " + CLASS_NAME + "\n" + "{\n\n";
    public static final String ALL_FIELDS = "ALL_FIELDS";

    private final Dictionary dictionary;
    private final String builderPackage;
    private final OutputManager outputManager;

    public ConstantGenerator(
        final Dictionary dictionary, final String builderPackage, final OutputManager outputManager)
    {
        this.dictionary = dictionary;
        this.builderPackage = builderPackage;
        this.outputManager = outputManager;
    }

    public void generate()
    {
        outputManager.withOutput(CLASS_NAME, out ->
        {
            out.append(fileHeader(builderPackage));
            out.append(importFor(IntHashSet.class));
            out.append(BODY);
            out.append(generateMessageTypes());
            out.append(generateFieldTags());
            out.append(generateFieldDictionary());
            out.append("}\n");
        });
    }

    private String generateFieldDictionary()
    {
        return generateFieldDictionary(fields(), ALL_FIELDS);
    }

    public static String generateFieldDictionary(final Collection<Field> fields, final String name)
    {
        final String addFields = fields
            .stream()
            .map((field) -> addField(field, name))
            .collect(joining());

        final int hashMapSize = sizeHashSet(fields);
        return String.format(
            "    public static final IntHashSet %3$s = new IntHashSet(%1$d, -1);\n\n" +
                "    static\n" +
                "    {\n" +
                "%2$s" +
                "    }\n\n",
            hashMapSize,
            addFields,
            name);
    }

    public static int sizeHashSet(final Collection<?> objects)
    {
        return objects.size() * 2;
    }

    private static String addField(final Field field, final String name)
    {
        return String.format(
            "        %1$s.add(%2$d);\n",
            name,
            field.number()
        );
    }

    private String generateMessageTypes()
    {
        return dictionary
            .messages()
            .stream()
            .map(message ->
            {
                final int type = message.packedType();
                return generateMessageTypeConstant(type) + generateIntConstant(message.name(), type);
            })
            .collect(joining());
    }

    private String generateFieldTags()
    {
        return fields()
            .stream()
            .map(field -> generateIntConstant(field.name(), field.number()))
            .collect(joining());
    }

    private Collection<Field> fields()
    {
        return dictionary
            .fields()
            .values();
    }

    private String generateMessageTypeConstant(final int messageType)
    {
        char[] chars;
        if (messageType > Byte.MAX_VALUE)
        {
            chars = new char[]{(char) (byte) messageType, (char) (byte) (messageType >>> 8)};
        }
        else
        {
            chars = new char[]{(char) (byte) messageType};
        }
        return String.format("    /** In Ascii - %1$s */\n", new String(chars));
    }

    private String generateIntConstant(final String name, final int number)
    {
        return String.format(
            "    public static final int %2$s = %1$d;\n\n",
            number,
            constantName(name));
    }

    private String constantName(String name)
    {
        name = name.replace("ID", "Id");
        return toUpperCase(name.charAt(0)) +
            name.substring(1)
                .chars()
                .mapToObj((codePoint) -> (isUpperCase(codePoint) ? "_" : "") + (char)toUpperCase(codePoint))
                .collect(joining());
    }

}
