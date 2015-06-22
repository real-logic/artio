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
package uk.co.real_logic.fix_gateway.auth;

import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.fix_gateway.decoder.HeaderDecoder;
import uk.co.real_logic.fix_gateway.decoder.LogonDecoder;

import java.util.Arrays;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SenderIdAuthenticationStrategyTest
{
    private SenderIdAuthenticationStrategy authenticationStrategy = new SenderIdAuthenticationStrategy(
        Arrays.asList("ab", "a"));

    private char[] examples = "abcdef".toCharArray();
    private HeaderDecoder headerDecoder = mock(HeaderDecoder.class);
    private LogonDecoder logonDecoder = mock(LogonDecoder.class);

    @Before
    public void setUp()
    {
        when(logonDecoder.header()).thenReturn(headerDecoder);
        when(headerDecoder.senderCompID()).thenReturn(examples);
    }

    @Test
    public void shouldAcceptValidId()
    {
        lengthOf(1);
        assertTrue(authenticationStrategy.authenticate(logonDecoder));

        lengthOf(2);
        assertTrue(authenticationStrategy.authenticate(logonDecoder));
    }

    @Test
    public void shouldRejectInvalidId()
    {
        lengthOf(3);
        assertFalse(authenticationStrategy.authenticate(logonDecoder));

        lengthOf(4);
        assertFalse(authenticationStrategy.authenticate(logonDecoder));
    }

    private void lengthOf(final int length)
    {
        when(headerDecoder.senderCompIDLength()).thenReturn(length);
    }
}
