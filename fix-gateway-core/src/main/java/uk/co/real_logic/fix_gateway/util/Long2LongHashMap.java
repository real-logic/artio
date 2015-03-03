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
package uk.co.real_logic.fix_gateway.util;

import uk.co.real_logic.agrona.BitUtil;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

/**
 * .
 */
public class Long2LongHashMap implements Map<Long, Long>
{

    private final int capacity;
    private final int mask;
    private final long[] entries;
    private final long tombstone;

    private int size = 0;

    public Long2LongHashMap(final int initialCapacity, final long tombstone)
    {
        this.tombstone = tombstone;
        capacity = BitUtil.findNextPositivePowerOfTwo(initialCapacity);
        mask = capacity - 1;
        entries = new long[capacity * 2];
        Arrays.fill(entries, tombstone);
    }

    /**
     * {@inheritDoc}
     */
    public int size()
    {
        return size;
    }

    /**
     * {@inheritDoc}
     */
    public boolean isEmpty()
    {
        return size() == 0;
    }

    public long get(final long key)
    {
        final long[] entries = this.entries;

        int index = hash(key);

        long candidateKey;
        while ((candidateKey = entries[index]) != tombstone)
        {
            if (candidateKey == key)
            {
                return entries[index + 1];
            }

            index = (index + 2) & mask;
        }

        return tombstone;
    }

    public long put(final long key, final long value)
    {
        long oldValue = tombstone;
        int index = hash(key);

        long candidateKey;
        while ((candidateKey = entries[index]) != tombstone)
        {
            if (candidateKey == key)
            {
                oldValue = entries[index + 1];
                break;
            }

            index = (index + 2) & mask;
        }

        if (oldValue == tombstone)
        {
            ++size;
            entries[index] = key;
        }

        entries[index + 1] = value;

        return oldValue;
    }

    private int hash(final long key)
    {
        int hash = (int)key ^ (int)(key >>> 32);
        hash = (hash << 1) - (hash << 8);
        return (hash & mask) * 2;
    }

    /**
     * Primitive specialised forEach implementation.
     *
     * NB: Renamed from forEach to avoid overloading on parameter types of lambda
     * expression, which doesn't interplay well with type inference in lambda expressions.
     *
     * @param consumer
     */
    public void longForEach(final LongLongConsumer consumer)
    {
        final long[] entries = this.entries;
        for (int i = 0; i < entries.length; i += 2)
        {
            final long key = entries[i];
            if (key != tombstone)
            {
                consumer.accept(entries[i], entries[i + 1]);
            }
        }
    }

    /**
     * Long primitive specialised containsKey.
     *
     * @param key
     * @return
     */
    public boolean containsKey(long key)
    {
        return get(key) != tombstone;
    }

    // ---------------- Boxed Versions Below ----------------

    /**
     * {@inheritDoc}
     */
    public Long get(final Object key)
    {
        return get((long) key);
    }

    /**
     * {@inheritDoc}
     */
    public Long put(final Long key, final Long value)
    {
        return put(key.longValue(), value.longValue());
    }

    /**
     * {@inheritDoc}
     */
    public void forEach(BiConsumer<? super Long, ? super Long> action)
    {
       longForEach(action::accept);
    }

    /**
     * {@inheritDoc}
     */
    public boolean containsKey(Object key)
    {
        return containsKey((long) key);
    }

    // ---------------- Unimplemented Versions Below ----------------

    /**
     * {@inheritDoc}
     */
    public Long remove(final Object key)
    {
        return remove((long) key);
    }

    public long remove(final long key)
    {
        return 0;
    }

    /**
     * {@inheritDoc}
     */
    public boolean containsValue(Object value)
    {
        return false;
    }

    /**
     * {@inheritDoc}
     */
    public void putAll(Map<? extends Long, ? extends Long> map)
    {

    }

    /**
     * {@inheritDoc}
     */
    public void clear()
    {
        Arrays.fill(entries, tombstone);
        size = 0;
    }

    /**
     * {@inheritDoc}
     */
    public Set<Long> keySet()
    {
        throw new UnsupportedOperationException("Not implemented");
    }

    /**
     * {@inheritDoc}
     */
    public Collection<Long> values()
    {
        throw new UnsupportedOperationException("Not implemented");
    }

    /**
     * {@inheritDoc}
     */
    public Set<Entry<Long, Long>> entrySet()
    {
        throw new UnsupportedOperationException("Not implemented");
    }
}
