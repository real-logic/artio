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
package uk.co.real_logic.fix_gateway.session;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static java.util.stream.Collectors.toSet;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class SenderAndTargetSessionIdStrategyTest
{
    public static final List<String> IDS = Arrays.asList("SIGMAX", "ABC_DEFG04", "LEH_LZJ02");

    private SenderAndTargetSessionIdStrategy strategy = new SenderAndTargetSessionIdStrategy();

    @Test
    public void differentIdsDoNotClash()
    {
        final Set<Object> compositeKeys = IDS.stream()
            .flatMap((sender) ->
                IDS.stream()
                   .map((target) -> strategy.onInitiatorLogon(sender, null, null, target)))
            .collect(toSet());

        assertThat(compositeKeys, hasSize(IDS.size() * IDS.size()));
    }

    @Test
    public void theSameIdIsEqual()
    {
        IDS.forEach((sender) ->
            IDS.forEach((target) ->
            {
                final Object first = strategy.onInitiatorLogon(sender, null, null, target);
                final Object second = strategy.onInitiatorLogon(sender, null, null, target);
                assertEquals(first, second);
                assertEquals(first.hashCode(), second.hashCode());
            }));
    }

    @Test
    public void savesAndLoadsACompositeKey()
    {
        // TODO
    }

    @Test
    public void validatesSpaceInBufferOnSave()
    {
        // TODO
    }

}
