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

import org.agrona.collections.IntHashSet;
import org.agrona.generation.OutputManager;
import uk.co.real_logic.artio.dictionary.CharArraySet;
import uk.co.real_logic.artio.dictionary.ir.Dictionary;
import uk.co.real_logic.artio.dictionary.ir.Field;

import java.util.Collection;

import static java.util.stream.Collectors.joining;
import static uk.co.real_logic.artio.dictionary.generation.DecoderGenerator.addField;
import static uk.co.real_logic.artio.dictionary.generation.GenerationUtil.fileHeader;
import static uk.co.real_logic.artio.dictionary.generation.GenerationUtil.importFor;

public class ConstantGenerator
{
    public static final String CLASS_NAME = "Constants";

    public static final String BODY = "public class " + CLASS_NAME + "\n" + "{\n\n";
    public static final String VERSION = "VERSION";

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
        outputManager.withOutput(CLASS_NAME, (out) ->
        {
            out.append(fileHeader(builderPackage));
            out.append(importFor(IntHashSet.class));
            out.append(importFor(CharArraySet.class));
            out.append(BODY);
            out.append(generateVersion());
            out.append(generateMessageTypes());
            out.append(generateFieldTags());
            out.append(generateAllFieldsDictionary());
            out.append("}\n");
        });
    }

    private String generateAllFieldsDictionary()
    {
        return generateFieldDictionary(dictionary.fields().values(), "ALL_FIELDS");
    }

    private String generateFieldDictionary(final Collection<Field> fields, final String name)
    {
        final String addFields = fields
            .stream()
            .map((field) -> addField(field, name))
            .collect(joining());

        final int hashMapSize = sizeHashSet(fields);
        return String.format(
            "    public static final IntHashSet %3$s = new IntHashSet(%1$d);\n" +
            "    static\n" +
            "    {\n" +
            "%2$s" +
            "    }\n\n",
            hashMapSize,
            addFields,
            name);
    }

    private String generateVersion()
    {
        return String.format(
            "    public static String VERSION = \"%s.%d.%d\";\n" +
            "    public static char[] VERSION_CHARS = VERSION.toCharArray();\n\n",
            dictionary.specType(),
            dictionary.majorVersion(),
            dictionary.minorVersion());
    }

    public static int sizeHashSet(final Collection<?> objects)
    {
        return objects.size() * 2;
    }

    private String generateMessageTypes()
    {
        return dictionary
            .messages()
            .stream()
            .map((message) ->
            {
                final int type = message.packedType();
                final String constantName = GenerationUtil.constantName(message.name()) + "_MESSAGE";
                final String stringConstantName = constantName + "_AS_STR";
                return generateMessageTypeConstant(stringConstantName, type) + generateIntConstant(constantName, type);
            })
            .collect(joining());
    }

    private String generateFieldTags()
    {
        return fields()
            .stream()
            .map(field -> generateIntConstant(GenerationUtil.constantName(field.name()), field.number()))
            .collect(joining());
    }

    private Collection<Field> fields()
    {
        return dictionary
            .fields()
            .values();
    }

    private String generateMessageTypeConstant(final String stringConstantName, final int messageType)
    {
        final char[] chars;
        if (messageType > Byte.MAX_VALUE)
        {
            chars = new char[]{ (char)(byte)messageType, (char)(byte)(messageType >>> 8) };
        }
        else
        {
            chars = new char[]{ (char)(byte)messageType };
        }

        return String.format(
            "    public static final String %1$s = \"%2$s\";\n",
            stringConstantName,
            new String(chars));
    }

    private String generateIntConstant(final String name, final int number)
    {
        return String.format(
            "    public static final int %1$s = %2$d;\n\n",
            name,
            number);
    }
}
