/*
 * Copyright 2015-2016 Real Logic Ltd.
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
package uk.co.real_logic.fix_gateway.engine.framer;

import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import org.agrona.collections.Long2ObjectHashMap;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.ABORT;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;

class RetryManager
{
    private final Long2ObjectHashMap<Transaction> correlationIdToTransactions = new Long2ObjectHashMap<>();
    private List<Transaction> polledTransactions = new ArrayList<>();

    Action retry(final long correlationId)
    {
        final Transaction transaction = correlationIdToTransactions.get(correlationId);
        if (transaction == null)
        {
            return null;
        }

        return attempt(correlationId, transaction);
    }

    Action firstAttempt(final long correlationId, final Transaction transaction)
    {
        correlationIdToTransactions.put(correlationId, transaction);

        return attempt(correlationId, transaction);
    }

    private Action attempt(final long correlationId, final Transaction transaction)
    {
        final Action action = transaction.attempt();
        if (action != ABORT)
        {
            correlationIdToTransactions.remove(correlationId);
        }
        return action;
    }

    void schedule(final Transaction transaction)
    {
        polledTransactions.add(transaction);
    }

    int attemptSteps()
    {
        return removeIf(polledTransactions, step -> step.attempt() == CONTINUE);
    }

    // TODO: move to agrona version when 0.5.5 is released
    private static <T> int removeIf(final List<T> values, final Predicate<T> predicate)
    {
        int size = values.size();
        int total = 0;

        for (int i = 0; i < size;)
        {
            final T value = values.get(i);
            if (predicate.test(value))
            {
                values.remove(i);
                total++;
                size--;
            }
            else
            {
                i++;
            }
        }

        return total;
    }
}
