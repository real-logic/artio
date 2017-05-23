/*
 * Copyright 2015-2017 Real Logic Ltd.
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
package uk.co.real_logic.fix_gateway.dictionary;

import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.collections.IntHashSet;
import uk.co.real_logic.fix_gateway.dictionary.ir.Dictionary;
import uk.co.real_logic.fix_gateway.dictionary.ir.Entry;
import uk.co.real_logic.fix_gateway.dictionary.ir.Field;

import java.util.function.Predicate;

/**
 * Dictionary for runtime validation by the generic parser.
 *
 * Essentially a map from ints to a set of ints.
 */
public final class IntDictionary
{
    private static final int MISSING_FIELD = -1;
    private static final int CAPACITY = 1024;

    private final Int2ObjectHashMap<IntHashSet> map;

    public static IntDictionary requiredFields(final Dictionary dictionary)
    {
        return fields(dictionary, Entry::required);
    }

    public static IntDictionary allFields(final Dictionary dictionary)
    {
        return fields(dictionary, (entry) -> true);
    }

    private static IntDictionary fields(final Dictionary dictionary, final Predicate<Entry> entryPredicate)
    {
        final IntDictionary fields = new IntDictionary();

        dictionary.messages().forEach(
            (message) ->
            {
                final int type = message.packedType();
                message.entries()
                    .stream()
                    .filter(entryPredicate)
                    .filter((entry) -> entry.element() instanceof Field)
                    .map((entry) -> (Field)entry.element())
                    .forEach((field) -> fields.put(type, field.number()));
            });

        return fields;
    }

    public IntDictionary()
    {
        map = new Int2ObjectHashMap<>();
    }

    public void put(final int key, final int value)
    {
        valuesOrDefault(key).add(value);
    }

    public void putAll(final int key, final int... valuesToAdd)
    {
        final IntHashSet values = valuesOrDefault(key);
        for (final int value : valuesToAdd)
        {
            values.add(value);
        }
    }

    private IntHashSet valuesOrDefault(final int key)
    {
        return map.computeIfAbsent(key, ignore -> new IntHashSet(CAPACITY));
    }

    public IntHashSet values(final int key)
    {
        return map.get(key);
    }

    public boolean contains(final int key, final int value)
    {
        final IntHashSet fields = values(key);
        return fields != null && fields.contains(value);
    }
}
