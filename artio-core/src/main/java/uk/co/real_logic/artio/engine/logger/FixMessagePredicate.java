/*
 * Copyright 2015-2023 Real Logic Limited., Monotonic Ltd.
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

import uk.co.real_logic.artio.messages.FixMessageDecoder;

/**
 * A criteria for filtering fix messages.
 *
 * Predicates should not be shared between different concurrent calls to <code>scan()</code>.
 *
 * @see FixMessagePredicates for different useful implementations
 */
@FunctionalInterface
public interface FixMessagePredicate
{
    boolean test(FixMessageDecoder message);

    /**
     * Invoked when a scan begins, can be used to reset any internal state.
     */
    default void reset()
    {
    }

    default FixMessagePredicate and(final FixMessagePredicate other)
    {
        return new FixMessageAnd(this, other);
    }

    default FixMessagePredicate or(final FixMessagePredicate other)
    {
        return new FixMessageOr(this, other);
    }
}
