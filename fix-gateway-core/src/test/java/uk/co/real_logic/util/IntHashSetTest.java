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
package uk.co.real_logic.util;

import org.junit.Test;
import uk.co.real_logic.fix_gateway.util.IntHashSet;

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
    public void containsAddedElement() throws Exception
    {
        obj.add(1);

        assertTrue(obj.contains(1));
    }

    @Test
    public void containsAddedElements() throws Exception
    {
        obj.add(1);
        obj.add(2);

        assertTrue(obj.contains(1));
        assertTrue(obj.contains(2));
    }
}
