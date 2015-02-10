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

import org.junit.Test;

import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IntHashSetTest
{

    private IntHashSet obj = new IntHashSet(100, -1);

    @Test
    public void initiallyContainsNoElements() throws Exception
    {
        for (int i = 0; i < 10_000; i++)
        {
            assertFalse(obj.contains(i));
        }
    }

    @Test
    public void initiallyContainsNoBoxedElements()
    {
        for (int i = 0; i < 10_000; i++)
        {
            assertFalse(obj.contains(Integer.valueOf(i)));
        }
    }

    @Test
    public void containsAddedBoxedElement()
    {
        assertTrue(obj.add(1));

        assertTrue(obj.contains(1));
    }

    @Test
    public void addingAnElementTwiceDoesNothing()
    {
        assertTrue(obj.add(1));

        assertFalse(obj.add(1));
    }

    @Test
    public void containsAddedBoxedElements()
    {
        assertTrue(obj.add(1));
        assertTrue(obj.add(Integer.valueOf(2)));

        assertTrue(obj.contains(Integer.valueOf(1)));
        assertTrue(obj.contains(2));
    }

    @Test
    public void removingAnElementFromAnEmptyListDoesNothing()
    {
        assertFalse(obj.remove(0));
    }

    @Test
    public void removingAPresentElementRemovesIt()
    {
        assertTrue(obj.add(1));

        assertTrue(obj.remove(1));

        assertFalse(obj.contains(1));
    }

    @Test
    public void sizeIsInitiallyZero()
    {
        assertEquals(0, obj.size());
    }

    @Test
    public void sizeIncrementsWithNumberOfAddedElements()
    {
        obj.add(1);
        obj.add(2);

        assertEquals(2, obj.size());
    }

    @Test
    public void sizeContainsNumberOfNewElements()
    {
        obj.add(1);
        obj.add(1);

        assertEquals(1, obj.size());
    }

    @Test
    public void iteratorsListElements()
    {
        obj.add(1);
        obj.add(2);

        assertIteratorHasElements();
    }

    @Test
    public void iteratorsStartFromTheBeginningEveryTime()
    {
        iteratorsListElements();

        assertIteratorHasElements();
    }

    private void assertIteratorHasElements()
    {
        final Iterator<Integer> it = obj.iterator();
        assertTrue(it.hasNext());
        assertEquals(Integer.valueOf(1), it.next());
        assertTrue(it.hasNext());
        assertEquals(Integer.valueOf(2), it.next());
        assertFalse(it.hasNext());
    }

}
