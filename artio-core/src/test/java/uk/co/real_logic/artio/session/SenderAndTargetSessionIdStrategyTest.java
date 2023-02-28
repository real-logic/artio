/*
 * Copyright 2015-2023 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.co.real_logic.artio.session;

import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;
import uk.co.real_logic.artio.decoder.SessionHeaderDecoder;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static java.util.stream.Collectors.toSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static uk.co.real_logic.artio.dictionary.SessionConstants.SENDER_COMP_ID;
import static uk.co.real_logic.artio.dictionary.SessionConstants.TARGET_COMP_ID;
import static uk.co.real_logic.artio.session.SessionIdStrategy.INSUFFICIENT_SPACE;

@SuppressWarnings("Indentation")
public class SenderAndTargetSessionIdStrategyTest
{
    public static final List<String> IDS = Arrays.asList("SIGMAX", "ABC_DEFG04", "LEH_LZJ02");

    private final SenderAndTargetSessionIdStrategy strategy = new SenderAndTargetSessionIdStrategy();

    @Test
    public void differentIdsDoNotClash()
    {
        final Set<Object> compositeKeys = IDS.stream()
            .flatMap((sender) ->
                IDS.stream()
                   .map((target) -> strategy.onInitiateLogon(sender, null, null, target, null, null)))
            .collect(toSet());

        assertThat(compositeKeys, hasSize(IDS.size() * IDS.size()));
    }

    @Test
    public void theSameIdIsEqual()
    {
        IDS.forEach((sender) ->
            IDS.forEach((target) ->
            {
                final Object first = strategy.onInitiateLogon(sender, null, null, target, null, null);
                final Object second = strategy.onInitiateLogon(sender, null, null, target, null, null);
                assertEquals(first, second);
                assertEquals(first.hashCode(), second.hashCode());
            }));
    }

    @Test
    public void savesAndLoadsACompositeKey()
    {
        final AtomicBuffer buffer = new UnsafeBuffer(new byte[1024]);
        final CompositeKey key = strategy.onInitiateLogon("SIGMAX", null, null, "ABC_DEFG04", null, null);

        final int length = strategy.save(key, buffer, 1);

        assertThat(length, greaterThan(0));

        final Object loadedKey = strategy.load(buffer, 1, length);

        assertEquals(key, loadedKey);
    }

    @Test
    public void validatesSpaceInBufferOnSave()
    {
        final AtomicBuffer buffer = new UnsafeBuffer(new byte[5]);
        final CompositeKey key = strategy.onInitiateLogon("SIGMAX", null, null, "ABC_DEFG04", null, null);

        final int length = strategy.save(key, buffer, 1);

        assertEquals(INSUFFICIENT_SPACE, length);
    }

    @Test
    public void testValidation()
    {
        final CompositeKey localKey = strategy.onInitiateLogon("FOO", null, null, "BAR", null, null);
        final SessionHeaderDecoder receivedHeader = mock(SessionHeaderDecoder.class);

        final Object[][] testVector = new Object[][]{
            { "BAR", "FOO", 0 },
            { "X", "FOO", SENDER_COMP_ID },
            { "BAR", "X", TARGET_COMP_ID },
        };

        for (final Object[] row : testVector)
        {
            final String senderCompId = (String)row[0];
            final String targetCompId = (String)row[1];
            final Integer expected = (Integer)row[2];

            when(receivedHeader.senderCompID()).thenReturn(senderCompId.toCharArray());
            when(receivedHeader.senderCompIDLength()).thenReturn(senderCompId.length());
            when(receivedHeader.targetCompID()).thenReturn(targetCompId.toCharArray());
            when(receivedHeader.targetCompIDLength()).thenReturn(targetCompId.length());

            assertEquals(expected.intValue(), strategy.validateCompIds(localKey, receivedHeader));
        }
    }
}
