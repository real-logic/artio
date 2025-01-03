/*
 * Copyright 2015-2025 Real Logic Limited.
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
package uk.co.real_logic.artio.validation;

import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.artio.decoder.HeaderDecoder;

import java.util.Arrays;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SenderCompIdValidationStrategyTest
{
    private final SenderCompIdValidationStrategy authenticationStrategy =
        new SenderCompIdValidationStrategy(Arrays.asList("ab", "a"));

    private final char[] examples = "abcdef".toCharArray();
    private final HeaderDecoder headerDecoder = mock(HeaderDecoder.class);

    @Before
    public void setUp()
    {
        when(headerDecoder.senderCompID()).thenReturn(examples);
    }

    @Test
    public void shouldAcceptValidId()
    {
        lengthOf(1);
        assertTrue(authenticationStrategy.validate(headerDecoder));

        lengthOf(2);
        assertTrue(authenticationStrategy.validate(headerDecoder));
    }

    @Test
    public void shouldRejectInvalidId()
    {
        lengthOf(3);
        assertFalse(authenticationStrategy.validate(headerDecoder));

        lengthOf(4);
        assertFalse(authenticationStrategy.validate(headerDecoder));
    }

    private void lengthOf(final int length)
    {
        when(headerDecoder.senderCompIDLength()).thenReturn(length);
    }
}
