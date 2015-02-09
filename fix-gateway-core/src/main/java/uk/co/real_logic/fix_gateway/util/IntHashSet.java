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

/**
 * Simple fixed-size int hashset for validating tags.
 */
// TODO: enhance to proper collection
public class IntHashSet
{
    private final int[] values;
    private final int mask;
    private final int missingValue;

    public IntHashSet(final int capacity, final int missingValue)
    {
        this.missingValue = missingValue;
        final int capacity1 = BitUtil.findNextPositivePowerOfTwo(capacity);
        mask = capacity1 - 1;
        values = new int[capacity1];
        for (int i = 0; i < capacity1; i++)
        {
            values[i] = missingValue;
        }
    }

    public void add(final int value)
    {
        int index = hash(value);

        while (values[index] != missingValue)
        {
            if (values[index] == value)
            {
                break;
            }

            index = ++index & mask;
        }

        values[index] = value;
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
}
