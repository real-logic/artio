/*
 * Copyright 2015-2024 Real Logic Limited.
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
package uk.co.real_logic.artio.dictionary.ir;

import uk.co.real_logic.artio.dictionary.ir.Entry.Element;

import java.util.Map;
import java.util.Objects;

public final class Group extends Aggregate implements Element
{
    private final Entry numberField;

    public Group(final String name, final Entry numberField)
    {
        super(name);

        Objects.requireNonNull(numberField);
        this.numberField = numberField;
    }

    public Entry numberField()
    {
        return numberField;
    }

    public static Group of(final Field field, final Map<String, Field> fields)
    {
        final String name = field.name();
        final String normalisedName = name.startsWith("No") ? name.substring(2) : name;
        final String fieldName = name + "GroupCounter";
        final String groupName = normalisedName + "Group";
        final Field numberField = new Field(field.number(), fieldName, Field.Type.NUMINGROUP);

        fields.put(numberField.name(), numberField);

        return new Group(groupName, new Entry(false, numberField));
    }

    @Override
    public String toString()
    {
        return "Group{" +
            "numberField=" + numberField + super.toString();
    }
}
