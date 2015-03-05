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

/**
 * A Aggregate is either a group, a message or a component.
 */
public abstract class Aggregate
{
    private final String name;
    private final List<Entry> entries;

    protected Aggregate(final String name)
    {
        this.name = name;
        entries = new ArrayList<>();
    }

    public String name()
    {
        return name;
    }

    public List<Entry> entries()
    {
        return entries;
    }

    public Aggregate optionalEntry(final Entry.Element element)
    {
        entries().add(Entry.optional(element));
        return this;
    }

    public Aggregate requiredEntry(final Entry.Element element)
    {
        entries().add(Entry.required(element));
        return this;
    }

    public String toString()
    {
        return getClass().getSimpleName() + "{" +
                "name='" + name + '\'' +
                ", entries=" + entries +
                '}';
    }

    public boolean hasField(final String msgType)
    {
        return entries().stream().anyMatch(e -> msgType.equals(e.element().name()));
    }
}
