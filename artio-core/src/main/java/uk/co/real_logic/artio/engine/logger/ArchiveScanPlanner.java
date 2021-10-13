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

import uk.co.real_logic.artio.engine.logger.FixMessagePredicates.FilterBy;
import uk.co.real_logic.artio.engine.logger.FixMessagePredicates.From;
import uk.co.real_logic.artio.engine.logger.FixMessagePredicates.To;

final class ArchiveScanPlanner
{
    static IndexQuery extractIndexQuery(final FixMessageConsumer fixHandler)
    {
        // need a filter in order to optimise the scan
        if (!(fixHandler instanceof FilterBy))
        {
            return null;
        }

        final FixMessagePredicate queryPredicate = ((FilterBy)fixHandler).predicate;
        return extractIndexQuery(queryPredicate);
    }

    private static IndexQuery extractIndexQuery(final FixMessagePredicate queryPredicate)
    {
        final IndexQuery indexQuery = new IndexQuery();
        extractIndexQuery(queryPredicate, indexQuery);
        return indexQuery.needed() ? indexQuery : null;
    }

    private static void extractIndexQuery(final FixMessagePredicate predicate, final IndexQuery indexQuery)
    {
        // NB: range returned by the index plan still needs filtering afterwards to ensure correctness
        if (predicate instanceof CompositeFixMessagePredicate)
        {
            final CompositeFixMessagePredicate and = (CompositeFixMessagePredicate)predicate;
            extractIndexQuery(and.left(), indexQuery);
            extractIndexQuery(and.right(), indexQuery);
        }
        else if (predicate instanceof From)
        {
            final From from = (From)predicate;
            indexQuery.from(from.beginTimestampInclusive());
        }
        else if (predicate instanceof To)
        {
            final To to = (To)predicate;
            indexQuery.to(to.endTimestampExclusive());
        }
    }
}
