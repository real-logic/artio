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

public class Message
{

    private final String name;
    private final char type;
    private final Category category;
    private final List<Field> requiredFields;
    private final List<Field> optionalFields;

    public Message(final String name, final char type, final Category category)
    {
        this.name = name;
        this.type = type;
        this.category = category;
        this.requiredFields = new ArrayList<>();
        this.optionalFields = new ArrayList<>();
    }

    public String name()
    {
        return name;
    }

    public char type()
    {
        return type;
    }

    public Category category()
    {
        return category;
    }

    public List<Field> requiredFields()
    {
        return requiredFields;
    }

    public List<Field> optionalFields()
    {
        return optionalFields;
    }

    @Override
    public String toString()
    {
        return "Message{" +
                "name='" + name + '\'' +
                ", type=" + type +
                ", category=" + category +
                ", requiredFields=" + requiredFields +
                ", optionalFields=" + optionalFields +
                '}';
    }
}
