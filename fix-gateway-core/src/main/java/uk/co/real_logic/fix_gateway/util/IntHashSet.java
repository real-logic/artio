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

import java.util.*;
import java.util.function.Predicate;

/**
 * Simple fixed-size int hashset for validating tags.
 */
public class IntHashSet implements Set<Integer>
{
    private final int[] values;
    private final IntIterator iterator;
    private final int mask;
    private final int missingValue;

    private int size;

    public IntHashSet(final int capacity, final int missingValue)
    {
        size = 0;
        this.missingValue = missingValue;
        final int capacity1 = BitUtil.findNextPositivePowerOfTwo(capacity);
        mask = capacity1 - 1;
        values = new int[capacity1];
        for (int i = 0; i < capacity1; i++)
        {
            values[i] = missingValue;
        }
        // NB: references values in the constructor, so must be assigned after values
        iterator = new IntIterator();
    }

    public boolean add(final Integer value)
    {
        return add(value.intValue());
    }

    /**
     * Primitive specialised overload of {@link this#add(Integer)}
     *
     * @param value the value to add
     * @return true if the collection has changed, false otherwise
     */
    public boolean add(final int value)
    {
        int index = hash(value);

        while (values[index] != missingValue)
        {
            if (values[index] == value)
            {
                return false;
            }

            index = ++index & mask;
        }

        values[index] = value;
        size++;
        return true;
    }

    public boolean remove(final Object value)
    {
        return value instanceof Integer
            && remove(((Integer) value).intValue());
    }

    public boolean remove(final int value)
    {
        int index = hash(value);

        while (values[index] != missingValue)
        {
            if (values[index] == value)
            {
                values[index] = missingValue;
                return true;
            }

            index = ++index & mask;
        }

        return false;
    }

    public boolean contains(final Object value)
    {
        return value instanceof Integer
            && contains(((Integer) value).intValue());
    }

    public boolean contains(final int value)
    {
        int index = hash(value);

        while (values[index] != missingValue)
        {
            if (values[index] == value)
            {
                return true;
            }

            index = ++index & mask;
        }

        return false;
    }

    private int hash(final int value)
    {
        final int hash = (value << 1) - (value << 8);
        return hash & mask;
    }

    public int size()
    {
        return size;
    }

    public boolean isEmpty()
    {
        return size() == 0;
    }

    public void clear()
    {
        final int[] values = this.values;
        final int length = values.length;
        for (int i = 0; i < length; i++)
        {
            values[i] = missingValue;
        }
        size = 0;
    }

    @SuppressWarnings("unchecked")
    public <T> T[] toArray(final T[] ignore)
    {
        return (T[]) (Object) Arrays.copyOf(values, values.length);
    }

    public boolean addAll(final Collection<? extends Integer> coll)
    {
        return conjunction(coll, x -> add(x));
    }

    public boolean containsAll(final Collection<?> coll)
    {
        return conjunction(coll, this::contains);
    }

    public boolean removeAll(final Collection<?> coll)
    {
        return conjunction(coll, this::remove);
    }

    private <T> boolean conjunction(final Collection<T> collection, final Predicate<T> predicate)
    {
        Objects.requireNonNull(collection);

        boolean acc = false;
        for(T t : collection)
        {
            // Deliberate strict evaluation
            acc |= predicate.test(t);
        }
        return acc;
    }

    // --- Unimplemented below here

    public Iterator<Integer> iterator()
    {
        iterator.reset();
        return iterator;
    }

    private class IntIterator implements Iterator<Integer>
    {
        final int[] values = IntHashSet.this.values;

        private int position;

        public boolean hasNext()
        {
            final int[] values = this.values;
            while (position < values.length)
            {
                if (values[position] != missingValue)
                {
                    return true;
                }

                position++;
            }

            return false;
        }

        public Integer next()
        {
            final int value = values[position];
            position++;
            return value;
        }

        public void reset()
        {
            position = 0;
        }
    }

    public boolean retainAll(final Collection<?> coll)
    {
        throw new UnsupportedOperationException("Not implemented");
    }

    public Object[] toArray()
    {
        throw new UnsupportedOperationException("Not Implemented");
    }
}
