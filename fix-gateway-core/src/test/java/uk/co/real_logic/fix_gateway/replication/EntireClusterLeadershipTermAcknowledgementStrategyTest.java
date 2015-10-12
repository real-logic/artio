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
package uk.co.real_logic.fix_gateway.replication;

import org.junit.Test;
import uk.co.real_logic.agrona.collections.Long2LongHashMap;

import static org.junit.Assert.assertEquals;

public class EntireClusterLeadershipTermAcknowledgementStrategyTest
{

    private EntireClusterLeadershipTermAcknowledgementStrategy strategy =
        new EntireClusterLeadershipTermAcknowledgementStrategy();

    @Test
    public void shouldAcknowledgeZeroForEmptyMap()
    {
        final Long2LongHashMap sessionIdToPosition = new Long2LongHashMap(-1L);

        final long ackedTerm = strategy.findAckedTerm(sessionIdToPosition);

        assertEquals(0, ackedTerm);
    }

    @Test
    public void shouldAcknowledgeSolePosition()
    {
        final long position = 1;

        final Long2LongHashMap sessionIdToPosition = new Long2LongHashMap(-1L);
        sessionIdToPosition.put(1, position);

        final long ackedTerm = strategy.findAckedTerm(sessionIdToPosition);

        assertEquals(position, ackedTerm);
    }

    @Test
    public void shouldAcknowledgeLowestPosition()
    {
        final Long2LongHashMap sessionIdToPosition = new Long2LongHashMap(-1L);
        sessionIdToPosition.put(1, 1);
        sessionIdToPosition.put(2, 2);
        sessionIdToPosition.put(3, 3);

        final long ackedTerm = strategy.findAckedTerm(sessionIdToPosition);

        assertEquals(1, ackedTerm);
    }

}
