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

import java.util.ArrayList;
import java.util.List;

public class EnumField extends Field
{

    private static class Value
    {
        private final char representation;
        private final String description;

        private Value(final char representation, final String description)
        {
            this.representation = representation;
            this.description = description;
        }

        public char representation()
        {
            return representation;
        }

        public String description()
        {
            return description;
        }
    }

    private final List<Value> values;

    public EnumField(final int number, final String name, final Type type)
    {
        super(number, name, type);
        this.values = new ArrayList<>();
    }

    public List<Value> values()
    {
        return values;
    }

    @Override
    public String toString()
    {
        return "EnumField{" +
                "number=" + number() +
                ", name='" + name() + '\'' +
                ", type=" + type() +
                "values=" + values +
                '}';
    }
}
