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

public final class Message extends Aggregate
{
    private final int type;
    private final Category category;

    public Message(final String name, final int type, final Category category)
    {
        super(name);
        this.type = type;
        this.category = category;
    }

    public int type()
    {
        return type;
    }

    public Category category()
    {
        return category;
    }

    @Override
    public String toString()
    {
        return "Message{" +
                "name='" + name() + '\'' +
                ", type=" + type +
                ", category=" + category +
                ", entries=" + entries() +
                '}';
    }

}
