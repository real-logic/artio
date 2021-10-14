/*
 * Copyright 2021 Adaptive Financial Consulting Ltd.
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
package uk.co.real_logic.artio.dictionary;

import org.agrona.collections.Int2ObjectHashMap;
import uk.co.real_logic.artio.dictionary.ir.BaseType;
import uk.co.real_logic.artio.dictionary.ir.Dictionary;
import uk.co.real_logic.artio.dictionary.ir.Field;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.*;

public class CodecCollisionFinder
{
    private static final boolean PRINT_FIELD_TYPE_COLL = false;
    private static final boolean PRINT_FIELD_TYPE_COLL_IF_FIXABLE = false;

    // Same number, different name
    private static final boolean PRINT_FIELD_NUMBER_COLL = false;

    private static final boolean PRINT_ENUM_NON_ENUM_COLL = false;
    private static final boolean PRINT_ENUM_VALUE_COLL = false;

    private static final boolean PRINT_COMPONENT_GROUP_COLL = true;

    private static PrintStream out;

    public static void main(final String[] args) throws Exception
    {
        out = new PrintStream(new FileOutputStream("out.txt"));

        final File dir = new File(args[0]);
        final File[] dictionaryFiles = dir.listFiles((ignore, name) -> name.endsWith(".xml"));
        final DictionaryParser parser = new DictionaryParser(true);
        final Map<File, Dictionary> fileToDictionary = new HashMap<>();
        for (final File dictionaryXmlFile : dictionaryFiles)
        {
            out.println("Parsing " + dictionaryXmlFile.getName());
            try (FileInputStream in = new FileInputStream(dictionaryXmlFile))
            {
                final Dictionary dictionary = parser.parse(in, null);
                fileToDictionary.put(dictionaryXmlFile, dictionary);
            }
        }

        out.println("Analyzing Dictionaries ... ");

        findFieldCollisions(fileToDictionary);
        findComponentGroupCollisions(fileToDictionary);
    }

    private static void findComponentGroupCollisions(final Map<File, Dictionary> fileToDictionary)
    {
        if (!PRINT_COMPONENT_GROUP_COLL)
        {
            return;
        }

        final Set<String> groupNames = new HashSet<>();
        final Set<String> messageNames = new HashSet<>();
        final Set<String> componentNames = new HashSet<>();
        final Set<String> fieldNames = new HashSet<>();

        for (final Map.Entry<File, Dictionary> pair : fileToDictionary.entrySet())
        {
            final Dictionary dictionary = pair.getValue();

            dictionary.messages().forEach(msg ->
            {
                messageNames.add(msg.name());

                msg.allGroupsIncludingComponents().forEach(group -> groupNames.add(group.name()));
            });
            fieldNames.addAll(dictionary.fields().keySet());
            dictionary.components().values().forEach(msg -> componentNames.add(msg.name()));
        }

        out.println("Components & Groups: " + intersection(componentNames, groupNames));
        out.println("Components & Messages: " + intersection(componentNames, messageNames));
        out.println("Messages & Groups: " + intersection(messageNames, groupNames));

        // Field names can clash with message names but not group or component names
        out.println("Fields & Groups: " + intersection(fieldNames, groupNames));
        out.println("Fields & Components: " + intersection(fieldNames, componentNames));
    }

    private static Set<String> intersection(final Set<String> left, final Set<String> right)
    {
        final Set<String> commonNames = new HashSet<>(left);
        commonNames.retainAll(right);
        return commonNames;
    }

    private static void findFieldCollisions(final Map<File, Dictionary> fileToDictionary)
    {
        final Map<String, Field> allFields = new HashMap<>();
        final Int2ObjectHashMap<Map<String, Integer>> numberToField = new Int2ObjectHashMap<>();

        for (final Map.Entry<File, Dictionary> pair : fileToDictionary.entrySet())
        {
            final File file = pair.getKey();
            final Dictionary dictionary = pair.getValue();
            final Map<String, Field> fields = dictionary.fields();
            for (final Field field : fields.values())
            {
                checkNumberCollisions(numberToField, field);

                final String name = field.name();
                final Field oldField = allFields.get(name);
                if (oldField == null)
                {
                    allFields.put(name, field);
                }
                else
                {
                    final int number = field.number();
                    final boolean isEnum = field.isEnum();
                    final boolean oldEnum = oldField.isEnum();

                    final Field.Type type = field.type();
                    final Field.Type oldType = oldField.type();

                    final BaseType baseType = BaseType.from(type);
                    final BaseType oldBaseType = BaseType.from(oldType);

                    if (PRINT_FIELD_TYPE_COLL && !baseType.equals(oldBaseType))
                    {
                        // Can resolve this situation by combining on type and differentiating on number
                        if (PRINT_FIELD_TYPE_COLL_IF_FIXABLE ||
                            (!canCombine(baseType, oldBaseType) && number == oldField.number()))
                        {
                            out.println("Field - type collision for " + name);
                            out.println(field);
                            out.println(oldField);
                            out.println("In: " + file + "\n\n");
                        }
                    }

                    if (PRINT_ENUM_NON_ENUM_COLL && isEnum != oldEnum)
                    {
                        out.println("Enum - Non-enum collision for " + name);
                        out.println(field);
                        out.println(oldField);
                        out.println("In: " + file + "\n\n");
                    }
                    else if (PRINT_ENUM_VALUE_COLL && isEnum)
                    {
                        final List<Field.Value> values = field.values();
                        final List<Field.Value> oldValues = oldField.values();

                        for (final Field.Value value : values)
                        {
                            for (final Field.Value oldValue : oldValues)
                            {
                                final String representation = value.representation();
                                final String oldRepresentation = oldValue.representation();

                                final String description = value.description();
                                final String oldDescription = oldValue.description();

                                if (representation.equals(oldRepresentation) !=
                                    description.equals(oldDescription))
                                {
                                    out.println("Enum - Enum Value collision for " + name);
                                    out.println(value);
                                    out.println(oldValue);
                                    out.println("In: " + file + "\n\n");
                                }
                            }
                        }
                    }
                }
            }
        }

        printNumberCollisions(numberToField);
    }

    private static void printNumberCollisions(final Int2ObjectHashMap<Map<String, Integer>> numberToField)
    {
        if (PRINT_FIELD_NUMBER_COLL)
        {
            out.println("Field collision - same number, different name:");
            numberToField.forEach((number, nameToCount) ->
            {
                if (nameToCount.size() > 1)
                {
                    out.println("number = " + number);
                    nameToCount.forEach((name, count) ->
                    {
                        out.println("name = " + name + ", count = " + count);
                    });
                }
            });
        }
    }

    private static void checkNumberCollisions(
        final Int2ObjectHashMap<Map<String, Integer>> numberToField, final Field field)
    {
        if (PRINT_FIELD_NUMBER_COLL)
        {
            final int number = field.number();
            final String name = field.name();
            Map<String, Integer> nameToCount = numberToField.get(number);
            if (nameToCount == null)
            {
                nameToCount = new HashMap<>();
                nameToCount.put(name, 1);
                numberToField.put(number, nameToCount);
            }
            else
            {
                final Integer count = nameToCount.get(name);
                final int newCount = count == null ? 1 : count + 1;
                nameToCount.put(name, newCount);
            }
        }
    }

    private static boolean canCombine(final BaseType baseType, final BaseType baseType2)
    {
        return canCombineOrdered(baseType, baseType2) || canCombineOrdered(baseType2, baseType);
    }

    private static boolean canCombineOrdered(final BaseType baseType, final BaseType baseType2)
    {
        final boolean string2 = baseType2 == BaseType.STRING;
        final boolean int1 = baseType == BaseType.INT;
        final boolean char1 = baseType == BaseType.CHAR;

        return char1 && string2 ||
            int1 && string2 ||
            baseType == BaseType.TIMESTAMP && string2 ||
            baseType == BaseType.FLOAT && string2 ||
            int1 && baseType2 == BaseType.CHAR ||
            char1 && baseType2 == BaseType.BOOLEAN;
    }
}
