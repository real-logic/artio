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

import uk.co.real_logic.fix_gateway.dictionary.ir.DataDictionary;
import uk.co.real_logic.fix_gateway.dictionary.ir.Field;
import uk.co.real_logic.agrona.generation.OutputManager;

import java.io.IOException;
import java.io.Writer;
import java.util.List;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;
import static uk.co.real_logic.fix_gateway.dictionary.generation.GenerationUtil.*;

public final class EnumGenerator
{
    private final DataDictionary dictionary;
    private final OutputManager outputManager;

    public EnumGenerator(final DataDictionary dictionary, final OutputManager outputManager)
    {
        this.dictionary = dictionary;
        this.outputManager = outputManager;
    }

    public void generate()
    {
        dictionary.fields()
                  .values()
                  .stream()
                  .filter(Field::isEnum)
                  .forEach(this::generateEnum);

    }

    private void generateEnum(final Field field)
    {
        final String enumName = field.name();

        try (final Writer out = outputManager.createOutput(enumName))
        {
            out.append(fileHeader(PARENT_PACKAGE));
            out.append(generateEnumDeclaration(enumName));

            out.append(generateEnumValues(field.values()));

            out.append(generateEnumBody(enumName));
            out.append(generateEnumLookupMethod());

            out.append("}\n");
        }
        catch (IOException e)
        {
            // TODO: logging
            e.printStackTrace();
        }
    }

    private String generateEnumDeclaration(final String name)
    {
        return "public enum " + name + "\n{\n";
    }

    private String generateEnumValues(final List<Field.Value> allValues)
    {
        return allValues.stream()
                        .map(value -> format("%s%s('%s')", INDENT, value.description(), value.representation()))
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

    private String generateEnumLookupMethod()
    {
        return "";
    }

}
