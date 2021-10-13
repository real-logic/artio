/*
 * Copyright 2021 Monotonic Ltd.
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

import org.junit.Test;
import uk.co.real_logic.artio.decoder.SessionHeaderDecoder;
import uk.co.real_logic.artio.dictionary.FixDictionary;

import java.util.function.Predicate;

import static org.junit.Assert.*;
import static uk.co.real_logic.artio.engine.logger.FixMessagePredicates.*;

public class ArchiveScanPlannerTest
{
    @Test
    public void shouldGeneratePlan()
    {
        final String session = "session";
        final long epochStartTimeInNs = 100;
        final long epochEndTimeInNs = 200;

        final FixMessagePredicate timeFilter = from(epochStartTimeInNs).and(to(epochEndTimeInNs));
        final FixDictionary fixDictionary = FixDictionary.of(FixDictionary.findDefault());
        final Predicate<SessionHeaderDecoder> sessionFilter = targetCompIdOf(session).or(senderCompIdOf(session));
        final FixMessagePredicate predicate = whereHeader(fixDictionary, sessionFilter).and(timeFilter);
        final FixMessageConsumer consumer = (message, buffer, offset, length, header) ->
        {
        };
        final FixMessageConsumer queryPredicate = filterBy(consumer, predicate);

        final IndexQuery indexQuery = ArchiveScanPlanner.extractIndexQuery(queryPredicate);
        assertEquals(epochStartTimeInNs, indexQuery.beginTimestampInclusive());
        assertEquals(epochEndTimeInNs, indexQuery.endTimestampExclusive());
    }
}
