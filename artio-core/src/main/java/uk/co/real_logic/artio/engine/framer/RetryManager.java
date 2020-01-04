/*
 * Copyright 2015-2020 Real Logic Limited.
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
package uk.co.real_logic.artio.engine.framer;

import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import org.agrona.collections.Long2ObjectHashMap;

import java.util.ArrayList;
import java.util.List;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.ABORT;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;
import static org.agrona.collections.CollectionUtil.removeIf;

class RetryManager implements AutoCloseable
{
    private final Long2ObjectHashMap<Continuation> correlationIdToTransactions = new Long2ObjectHashMap<>();
    private final List<Continuation> continuations = new ArrayList<>();

    Action retry(final long correlationId)
    {
        final Continuation continuation = correlationIdToTransactions.get(correlationId);
        if (continuation == null)
        {
            return null;
        }

        return attempt(correlationId, continuation);
    }

    Action firstAttempt(final long correlationId, final Continuation continuation)
    {
        correlationIdToTransactions.put(correlationId, continuation);

        return attempt(correlationId, continuation);
    }

    private Action attempt(final long correlationId, final Continuation continuation)
    {
        final Action action = continuation.attemptToAction();
        if (action != ABORT)
        {
            correlationIdToTransactions.remove(correlationId);
        }
        return action;
    }

    void schedule(final Continuation continuation)
    {
        continuations.add(continuation);
    }

    int attemptSteps()
    {
        return removeIf(continuations, step -> step.attemptToAction() == CONTINUE);
    }

    public void close()
    {
        continuations.forEach(Continuation::close);
        correlationIdToTransactions.values().forEach(Continuation::close);
    }
}
