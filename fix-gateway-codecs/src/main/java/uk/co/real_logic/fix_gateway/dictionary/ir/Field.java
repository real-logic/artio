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

import uk.co.real_logic.agrona.Verify;
import uk.co.real_logic.fix_gateway.dictionary.ir.Entry.Element;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class Field implements Element
{
    private final int number;
    private final String name;
    private Type type;
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

    public void type(final Type type)
    {
        Verify.notNull(type, "type");
        this.type = type;
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

    public enum Type
    {
        // int types
        INT(false, true, false),
        LENGTH(false, true, false),
        SEQNUM(false, true, false),
        NUMINGROUP(false, true, false),
        DAYOFMONTH(false, true, false),

        // float types
        FLOAT(false, false, true),
        PRICE(false, false, true),
        PRICEOFFSET(false, false, true),
        QTY(false, false, true),
        PERCENTAGE(false, false, true), // Percentage represented as a float
        AMT(false, false, true), // Float amount, not to be confused with boolean Y/N AMT

        CHAR(false, false, false),

        STRING(true, false, false),
        MULTIPLEVALUESTRING(true, false, false),

        CURRENCY(true, false, false), // String using ISO 4217 (3 chars)
        EXCHANGE(true, false, false), // String using ISO 10383 (2 chars)
        COUNTRY(true, false, false), // String using ISO 3166

        DATA(false, false, false),

        // Boolean types
        BOOLEAN(false, false, false),

        UTCTIMESTAMP(false, false, false), // YYYYMMDD-HH:MM:SS or YYYYMMDD-HH:MM:SS.sss
        UTCTIMEONLY(true, false, false), // HH:MM:SS or HH:MM:SS.sss
        UTCDATEONLY(true, false, false), // YYYYMMDD
        LOCALMKTDATE(false, false, false), // YYYYMMDD
        MONTHYEAR(true, false, false); // YYYYMM or YYYYMMDD or YYYYMMWW

        private final boolean isStringBased;
        private final boolean isIntBased;
        private final boolean isFloatBased;

        Type(final boolean isStringBased, final boolean isIntBased, final boolean isFloatBased)
        {
            this.isStringBased = isStringBased;
            this.isIntBased = isIntBased;
            this.isFloatBased = isFloatBased;
        }

        public boolean isStringBased()
        {
            return isStringBased;
        }

        public boolean isIntBased()
        {
            return isIntBased;
        }

        public boolean isFloatBased()
        {
            return isFloatBased;
        }

        public static Type lookup(final String name)
        {
            // Renamed from FIX 4.2 to Fix 4.4 spec
            if ("UTCDATE".equals(name))
            {
                return UTCDATEONLY;
            }

            return valueOf(name);
        }
    }

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
