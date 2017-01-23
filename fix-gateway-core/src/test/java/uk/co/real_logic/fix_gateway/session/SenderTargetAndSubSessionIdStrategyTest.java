/*
 * Copyright 2015-2016 Real Logic Ltd.
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
package uk.co.real_logic.fix_gateway.session;

import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;

import java.util.Set;

import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static uk.co.real_logic.fix_gateway.session.SenderAndTargetSessionIdStrategyTest.IDS;
import static uk.co.real_logic.fix_gateway.session.SessionIdStrategy.INSUFFICIENT_SPACE;

public class SenderTargetAndSubSessionIdStrategyTest
{

    private SenderTargetAndSubSessionIdStrategy strategy = new SenderTargetAndSubSessionIdStrategy();

    @Test
    public void differentIdsDoNotClash()
    {
        final Set<Object> compositeKeys =
            IDS.stream().flatMap((sender) ->
                IDS.stream().flatMap((senderSub) ->
                    IDS.stream().map(
                        (target) -> strategy.onInitiateLogon(sender, senderSub, null, target, null, null))))
            .collect(toSet());

        assertThat(compositeKeys, hasSize(IDS.size() * IDS.size() * IDS.size()));
    }

    @Test
    public void theSameIdIsEqual()
    {
        IDS.forEach((sender) ->
            IDS.forEach((senderSub) ->
                IDS.forEach((target) ->
                {
                    final Object first = strategy.onInitiateLogon(sender, senderSub, null, target, null, null);
                    final Object second = strategy.onInitiateLogon(sender, senderSub, null, target, null, null);
                    assertEquals(first, second);
                    assertEquals(first.hashCode(), second.hashCode());
                })));
    }

    @Test
    public void savesAndLoadsACompositeKey()
    {
        final AtomicBuffer buffer = new UnsafeBuffer(new byte[1024]);
        final CompositeKey key = strategy.onInitiateLogon("SIGMAX", "LEH_LZJ02", null, "ABC_DEFG04", null, null);

        final int length = strategy.save(key, buffer, 1);

        assertThat(length, greaterThan(0));

        final Object loadedKey = strategy.load(buffer, 1, length);

        assertEquals(key, loadedKey);
    }

    @Test
    public void validatesSpaceInBufferOnSave()
    {
        final AtomicBuffer buffer = new UnsafeBuffer(new byte[5]);
        final CompositeKey key = strategy.onInitiateLogon("SIGMAX", "LEH_LZJ02", null, "ABC_DEFG04", null, null);

        final int length = strategy.save(key, buffer, 1);

        assertEquals(INSUFFICIENT_SPACE, length);
    }
}
