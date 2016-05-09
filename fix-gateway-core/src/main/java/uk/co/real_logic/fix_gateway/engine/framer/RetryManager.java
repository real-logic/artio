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

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.ABORT;

public class RetryManager
{
    private final Long2ObjectHashMap<Transaction> correlationIdToRetry = new Long2ObjectHashMap<>();
    private List<Step> steps = new ArrayList<>();

    public Action retry(final long correlationId)
    {
        final Transaction transaction = correlationIdToRetry.get(correlationId);
        if (transaction == null)
        {
            return null;
        }

        return attempt(correlationId, transaction);
    }

    public Action firstAttempt(final long correlationId, final Transaction transaction)
    {
        correlationIdToRetry.put(correlationId, transaction);

        return attempt(correlationId, transaction);
    }

    private Action attempt(final long correlationId, final Transaction transaction)
    {
        final Action action = transaction.attempt();
        if (action != ABORT)
        {
            correlationIdToRetry.remove(correlationId);
        }
        return action;
    }

    public void schedule(final Step step)
    {
        steps.add(step);
    }

    public int attemptSteps()
    {
        return steps.removeIf(step -> step.attempt() >= 0) ? 1 : 0;
    }
}
