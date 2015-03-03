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
import org.mockito.InOrder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;

public class Long2LongHashMapTest
{
    public static final long MISSING_VALUE = -1L;

    private Long2LongHashMap map = new Long2LongHashMap(10, MISSING_VALUE);

    @Test
    public void shouldInitiallyBeEmpty()
    {
        assertEquals(0, map.size());
        assertTrue(map.isEmpty());
    }

    @Test
    public void getShouldReturnMissingValueWhenEmpty()
    {
        assertEquals(MISSING_VALUE, map.get(1L));
    }

    @Test
    public void getShouldReturnMissingValueWhenThereIsNoElement()
    {
        map.put(1L, 1L);

        assertEquals(MISSING_VALUE, map.get(2L));
    }

    @Test
    public void getShouldReturnPutValues()
    {
        map.put(1L, 1L);

        assertEquals(1L, map.get(1L));
    }

    @Test
    public void putShouldReturnOldValue()
    {
        map.put(1L, 1L);

        assertEquals(1L, map.put(1L, 2L));
    }

    @Test
    public void clearShouldResetSize()
    {
        map.put(1L, 1L);
        map.put(100L, 100L);

        map.clear();

        assertEquals(0, map.size());
        assertTrue(map.isEmpty());
    }

    @Test
    public void clearShouldRemoveValues()
    {
        map.put(1L, 1L);
        map.put(100L, 100L);

        map.clear();

        assertEquals(MISSING_VALUE, map.get(1L));
        assertEquals(MISSING_VALUE, map.get(100L));
    }

    @Test
    public void forEachShouldLoopOverEveryElement()
    {
        map.put(1L, 1L);
        map.put(100L, 100L);

        final LongLongConsumer mockConsumer = mock(LongLongConsumer.class);
        map.longForEach(mockConsumer);

        final InOrder inOrder = inOrder(mockConsumer);
        inOrder.verify(mockConsumer).accept(1L, 1L);
        inOrder.verify(mockConsumer).accept(100L, 100L);
        inOrder.verifyNoMoreInteractions();
    }

    @Test
    public void shouldNotContainKeyOfAMissingKey()
    {
        assertFalse(map.containsKey(1L));
    }

    @Test
    public void shouldContainKeyOfAPresentKey()
    {
        map.put(1L, 1L);

        assertTrue(map.containsKey(1L));
    }

    // TODO: resize
}
