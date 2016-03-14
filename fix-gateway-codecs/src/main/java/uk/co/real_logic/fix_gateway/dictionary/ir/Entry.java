/*
 * Copyright 2015-2016 Real Logic Ltd.
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

import java.util.function.BiFunction;
import java.util.function.Function;

public final class Entry
{
    private final boolean required;
    private Element element;

    public static Entry required(final Element element)
    {
        return new Entry(true, element);
    }

    public static Entry optional(final Element element)
    {
        return new Entry(false, element);
    }

    public <T> T match(
        final BiFunction<Entry, Field, ? extends T> withField,
        final BiFunction<Entry, Group, ? extends T> withGroup,
        final BiFunction<Entry, Component, ? extends T> withComponent)
    {
        if (element instanceof Field)
        {
            return withField.apply(this, (Field) element);
        }
        else if (element instanceof Group)
        {
            return withGroup.apply(this, (Group) element);
        }
        else if (element instanceof Component)
        {
            return withComponent.apply(this, (Component) element);
        }
        throw new IllegalStateException("Unknown element type: " + element);
    }

    public <T> T matchEntry(
        final Function<Entry, ? extends T> withField,
        final Function<Entry, ? extends T> withGroup,
        final Function<Entry, ? extends T> withComponent)
    {
        return match(
            (entry, field) -> withField.apply(entry),
            (entry, group) -> withGroup.apply(entry),
            (entry, component) -> withComponent.apply(entry));
    }

    /**
     * @param element nullable in the case of forward references
     */
    public Entry(final boolean required, final Element element)
    {
        this.required = required;
        this.element = element;
    }

    public boolean required()
    {
        return this.required;
    }

    public Element element()
    {
        return this.element;
    }

    public Entry element(final Element element)
    {
        Verify.notNull(element, "element");

        this.element = element;
        return this;
    }

    public boolean isField()
    {
        return element() instanceof Field;
    }

    public boolean isComponent()
    {
        return element() instanceof Component;
    }

    public boolean isGroup()
    {
        return element() instanceof Group;
    }

    @Override
    public String toString()
    {
        return "Entry{" +
                "required=" + required +
                ", element=" + element +
                '}';
    }

    public String name()
    {
        return element().name();
    }

    public interface Element
    {
        default boolean isEnumField()
        {
            return this instanceof Field && ((Field)this).isEnum();
        }

        String name();
    }
}
