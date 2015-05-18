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

import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.aeron.Subscription;
import uk.co.real_logic.agrona.collections.IntHashSet;
import uk.co.real_logic.agrona.collections.Long2LongHashMap;

import static org.mockito.Mockito.mock;

public class CoordinatorTest
{
    private IntHashSet followers = new IntHashSet(10, -1);
    private Subscription subscription = mock(Subscription.class);
    private Coordinator coordinator;

    @Before
    public void setUp()
    {
        followers.add(1);
        followers.add(2);
        followers.add(3);
        coordinator = new Coordinator(Long2LongHashMap::minValue, followers, subscription);
    }

    @Test
    public void shouldPollToQuorumPosition()
    {
        // TODO
    }
}
