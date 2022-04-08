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
import uk.co.real_logic.artio.dictionary.FixDictionary;
import uk.co.real_logic.artio.messages.FixMessageDecoder;
import uk.co.real_logic.artio.messages.FixMessageEncoder;

import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static uk.co.real_logic.artio.engine.logger.FixMessagePredicates.targetCompIdOf;
import static uk.co.real_logic.artio.engine.logger.FixMessagePredicates.whereHeader;

public class FixMessagePredicateTest
{
    private static final String COMPOSITION_EG = "123ABC123";

    private static final String HEADER_EG = "8=FIX.4.4\0019=110\00135=A\00149=initiator\00156=acceptor\00134=1" +
        "\00152=20210915-14:21:55.490\00198=0\001108=10\001141=N\001553=bob\001554=***\00135002=0\00135003=0" +
        "\00110=047\001";

    private final UnsafeBuffer buffer = new UnsafeBuffer(new byte[1024]);
    private final FixMessageEncoder encoder = new FixMessageEncoder()
        .wrap(buffer, 0);
    private final FixMessageDecoder decoder = new FixMessageDecoder()
        .wrap(buffer, 0, encoder.sbeBlockLength(), encoder.sbeSchemaVersion());

    private final FixMessagePredicate middleAbc = FixMessagePredicates.bodyMatches(Pattern.compile(".*ABC.*"));
    private final FixMessagePredicate starts123 = FixMessagePredicates.bodyMatches(Pattern.compile("123.*"));
    private final FixMessagePredicate startsAbc = FixMessagePredicates.bodyMatches(Pattern.compile("abc.*"));

    @Test
    public void andShouldBeCompositional()
    {
        encoder.body(COMPOSITION_EG);

        assertTrue(middleAbc.and(starts123).test(decoder));
    }

    @Test
    public void orShouldBeCompositional()
    {
        encoder.body(COMPOSITION_EG);

        assertTrue(startsAbc.or(starts123).test(decoder));
    }

    @Test
    public void shouldMatchHeader()
    {
        encoder.body(HEADER_EG);

        assertTargetCompId("acceptor", true);
    }

    @Test
    public void shouldMatchHeaderExactly()
    {
        encoder.body(HEADER_EG);

        assertTargetCompId("accept", false);
        assertTargetCompId("acceptor2", false);
    }

    private void assertTargetCompId(final String targetCompIdOf, final boolean expected)
    {
        final FixMessagePredicate predicate =
            whereHeader(FixDictionary.of(FixDictionary.findDefault()), targetCompIdOf(targetCompIdOf));
        assertEquals(expected, predicate.test(decoder));
    }
}
