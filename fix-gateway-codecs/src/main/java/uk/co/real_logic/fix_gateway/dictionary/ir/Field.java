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
package uk.co.real_logic.fix_gateway.dictionary.ir;

import uk.co.real_logic.fix_gateway.dictionary.ir.Entry.Element;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class Field implements Element
{
    private final int number;
    private final String name;
    private final Type type;
    private final List<Value> values;

    public static Field registerField(
        final Map<String, Field> nameToField,
        final int number,
        final String name,
        final Type type)
    {
        final Field field = new Field(number, name, type);
        nameToField.put(name, field);
        return field;
    }

    public Field(final int number, final String name, final Type type)
    {
        this.number = number;
        this.name = name;
        this.type = type;
        this.values = new ArrayList<>();
    }

    public Type type()
    {
        return type;
    }

    public String name()
    {
        return name;
    }

    public int number()
    {
        return number;
    }

    public List<Value> values()
    {
        return values;
    }

    public Field addValue(final String representation, final String description)
    {
        values().add(new Value(representation, description));
        return this;
    }

    public boolean isEnum()
    {
        return !values.isEmpty();
    }

    @Override
    public String toString()
    {
        return "EnumField{" +
            "number=" + number +
            ", name='" + name + '\'' +
            ", type=" + type +
            "values=" + values +
            '}';
    }

    public static enum Type
    {
        // int types
        INT,
        LENGTH,
        SEQNUM,
        NUMINGROUP,

        // float types
        FLOAT,
        PRICE,
        PRICEOFFSET,
        QTY,
        PERCENTAGE, // Percentage represented as a float
        AMT, // Float amount, not to be confused with boolean Y/N AMT

        CHAR,

        STRING,
        MULTIPLEVALUESTRING,

        CURRENCY, // String using ISO 4217 (3 chars)
        EXCHANGE, // String using ISO 10383 (2 chars)
        COUNTRY, // String using ISO 3166

        DATA,

        // Boolean types
        BOOLEAN,

        UTCTIMESTAMP, // YYYYMMDD-HH:MM:SS or YYYYMMDD-HH:MM:SS.sss
        UTCTIMEONLY, // HH:MM:SS or HH:MM:SS.sss
        UTCDATEONLY, // YYYYMMDD
        LOCALMKTDATE, // YYYYMMDD
        MONTHYEAR, // YYYYMM or YYYYMMDD or YYYYMMWW
    }

    // TODO: properly provide parsing support for:
    // UTCTIMEONLY
    // UTCDATEONLY
    // MONTHYEAR

    // TODO: provide lookup table support for:
    // CURRENCY
    // EXCHANGE
    // COUNTRY

    public static class Value
    {
        private final String representation;
        private final String description;

        public Value(final String representation, final String description)
        {
            this.representation = representation;
            this.description = description;
        }

        public String representation()
        {
            return representation;
        }

        public String description()
        {
            return description;
        }

        @Override
        public boolean equals(final Object o)
        {
            if (o == null || getClass() != o.getClass())
            {
                return false;
            }

            final Value value = (Value)o;
            return Objects.equals(representation, value.representation) && Objects.equals(description, value.description);
        }

        @Override
        public int hashCode()
        {
            int result = representation.hashCode();
            result = 31 * result + (description != null ? description.hashCode() : 0);
            return result;
        }

        @Override
        public String toString()
        {
            return "Value{" +
                "representation=" + representation +
                ", description='" + description + '\'' +
                '}';
        }
    }
}
