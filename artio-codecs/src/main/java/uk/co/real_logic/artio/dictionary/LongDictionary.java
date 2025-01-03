/*
 * Copyright 2015-2025 Real Logic Limited.
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
package uk.co.real_logic.artio.dictionary;

import java.util.function.Predicate;

import org.agrona.collections.IntHashSet;
import org.agrona.collections.Long2ObjectHashMap;


import uk.co.real_logic.artio.dictionary.ir.Dictionary;
import uk.co.real_logic.artio.dictionary.ir.Entry;
import uk.co.real_logic.artio.dictionary.ir.Field;

/**
 * Dictionary for runtime validation by the generic parser.
 *
 * Essentially a map from longs to a set of ints.
 */
public final class LongDictionary
{
    private static final int MISSING_FIELD = -1;
    private static final int CAPACITY = 1024;

    private final Long2ObjectHashMap<IntHashSet> map;

    public static LongDictionary requiredFields(final Dictionary dictionary)
    {
        return fields(dictionary, Entry::required);
    }

    public static LongDictionary allFields(final Dictionary dictionary)
    {
        return fields(dictionary, (entry) -> true);
    }

    private static LongDictionary fields(final Dictionary dictionary, final Predicate<Entry> entryPredicate)
    {
        final LongDictionary fields = new LongDictionary();

        dictionary.messages().forEach(
            (message) ->
            {
                final long type = message.packedType();
                message.entries()
                    .stream()
                    .filter(entryPredicate)
                    .filter((entry) -> entry.element() instanceof Field)
                    .map((entry) -> (Field)entry.element())
                    .forEach((field) -> fields.put(type, field.number()));
            });

        return fields;
    }

    public LongDictionary()
    {
        map = new Long2ObjectHashMap<>();
    }

    public void put(final long key, final int value)
    {
        valuesOrDefault(key).add(value);
    }

    public void putAll(final long key, final int... valuesToAdd)
    {
        final IntHashSet values = valuesOrDefault(key);
        for (final int value : valuesToAdd)
        {
            values.add(value);
        }
    }

    private IntHashSet valuesOrDefault(final long key)
    {
        return map.computeIfAbsent(key, ignore -> new IntHashSet(CAPACITY));
    }

    public IntHashSet values(final long key)
    {
        return map.get(key);
    }

    public boolean contains(final long key, final int value)
    {
        final IntHashSet fields = values(key);
        return fields != null && fields.contains(value);
    }
}
