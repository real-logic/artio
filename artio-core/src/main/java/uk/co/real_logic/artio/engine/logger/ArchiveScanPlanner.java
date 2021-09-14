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

import uk.co.real_logic.artio.decoder.SessionHeaderDecoder;
import uk.co.real_logic.artio.engine.SessionInfo;
import uk.co.real_logic.artio.engine.logger.FixMessagePredicates.From;
import uk.co.real_logic.artio.engine.logger.FixMessagePredicates.HeaderMatches;
import uk.co.real_logic.artio.engine.logger.FixMessagePredicates.To;

import java.util.function.Predicate;

class ArchiveScanPlanner
{
    static Predicate<SessionInfo> extractSessionPlan(final FixMessagePredicate predicate)
    {
        if (predicate instanceof FixMessageAnd)
        {
            final FixMessageAnd and = (FixMessageAnd)predicate;
            final Predicate<SessionInfo> left = extractSessionPlan(and.left());
            final Predicate<SessionInfo> right = extractSessionPlan(and.right());
            if (left == null)
            {
                return right;
            }
            else if (right == null)
            {
                return left;
            }
            else
            {
                return new And<>(left, right);
            }
        }
        else if (predicate instanceof FixMessageOr)
        {
            final FixMessageOr or = (FixMessageOr)predicate;
            final Predicate<SessionInfo> left = extractSessionPlan(or.left());
            final Predicate<SessionInfo> right = extractSessionPlan(or.right());

            // Don't support an Or between a session predicate and something not matching a header
            if (left == null || right == null)
            {
                throw new IllegalArgumentException("Unoptimizable due to: " + or);
            }
            else
            {
                return new Or<>(left, right);
            }
        }
        else if (predicate instanceof FixMessagePredicates.WhereHeader)
        {
            final FixMessagePredicates.WhereHeader whereHeader = (FixMessagePredicates.WhereHeader)predicate;
            return extractSessionPlan(whereHeader.matches());
        }
        else
        {
            return null;
        }
    }

    private static Predicate<SessionInfo> extractSessionPlan(final Predicate<SessionHeaderDecoder> headerPredicate)
    {
        if (headerPredicate instanceof And)
        {
            final And<SessionHeaderDecoder> and = (And<SessionHeaderDecoder>)headerPredicate;
            final Predicate<SessionInfo> left = extractSessionPlan(and.left);
            final Predicate<SessionInfo> right = extractSessionPlan(and.right());
            if (left == null)
            {
                return right;
            }
            else if (right == null)
            {
                return left;
            }
            else
            {
                return new And<>(left, right);
            }
        }
        else if (headerPredicate instanceof Or)
        {
            final Or<SessionHeaderDecoder> or = (Or<SessionHeaderDecoder>)headerPredicate;
            final Predicate<SessionInfo> left = extractSessionPlan(or.left);
            final Predicate<SessionInfo> right = extractSessionPlan(or.right());
            // Don't support an Or between a session predicate and something not optimizable
            if (left == null || right == null)
            {
                throw new IllegalArgumentException("Unoptimizable due to: " + or);
            }
            else
            {
                return new Or<>(left, right);
            }
        }
        else if (headerPredicate instanceof HeaderMatches)
        {
            final HeaderMatches headerMatches = (HeaderMatches)headerPredicate;
            if (headerMatches.headerField == HeaderField.NOT_OPTIMISED)
            {
                return null;
            }

            return new SessionHeaderMatcher(headerMatches.headerField, headerMatches.value);
        }
        else
        {
            return null;
        }
    }

    public static IndexQuery extractIndexQuery(final FixMessagePredicate queryPredicate)
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
