/*
 * Copyright 2020 Monotonic Ltd.
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
package uk.co.real_logic.artio.engine.logger;

import org.agrona.concurrent.UnsafeBuffer;
import org.junit.Test;
import uk.co.real_logic.artio.messages.FixMessageDecoder;
import uk.co.real_logic.artio.messages.FixMessageEncoder;

import java.util.regex.Pattern;

import static org.junit.Assert.*;

public class FixMessagePredicateTest
{
    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[1024]);
    private final FixMessageEncoder encoder = new FixMessageEncoder()
        .wrap(buffer, 0)
        .body("123ABC123");
    private final FixMessageDecoder decoder = new FixMessageDecoder()
        .wrap(buffer, 0, encoder.sbeBlockLength(), encoder.sbeSchemaVersion());

    private final FixMessagePredicate middleAbc = FixMessagePredicates.bodyMatches(Pattern.compile(".*ABC.*"));
    private final FixMessagePredicate starts123 = FixMessagePredicates.bodyMatches(Pattern.compile("123.*"));
    private final FixMessagePredicate startsAbc = FixMessagePredicates.bodyMatches(Pattern.compile("abc.*"));

    @Test
    public void andShouldBeCompositional()
    {
        assertTrue(middleAbc.and(starts123).test(decoder));
    }

    @Test
    public void orShouldBeCompositional()
    {
        assertTrue(startsAbc.or(starts123).test(decoder));
    }
}
