/*
 * Copyright 2015-2017 Real Logic Ltd.
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
package uk.co.real_logic.artio.engine.framer;

import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import org.agrona.collections.Long2ObjectHashMap;

import java.util.ArrayList;
import java.util.List;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.ABORT;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;
import static org.agrona.collections.CollectionUtil.removeIf;

class RetryManager
{
    private final Long2ObjectHashMap<UnitOfWork> correlationIdToTransactions = new Long2ObjectHashMap<>();
    private final List<Continuation> polledUnitOfWorks = new ArrayList<>();

    Action retry(final long correlationId)
    {
        final UnitOfWork unitOfWork = correlationIdToTransactions.get(correlationId);
        if (unitOfWork == null)
        {
            return null;
        }

        return attempt(correlationId, unitOfWork);
    }

    Action firstAttempt(final long correlationId, final UnitOfWork unitOfWork)
    {
        correlationIdToTransactions.put(correlationId, unitOfWork);

        return attempt(correlationId, unitOfWork);
    }

    private Action attempt(final long correlationId, final UnitOfWork unitOfWork)
    {
        final Action action = unitOfWork.attemptToAction();
        if (action != ABORT)
        {
            correlationIdToTransactions.remove(correlationId);
        }
        return action;
    }

    void schedule(final Continuation unitOfWork)
    {
        polledUnitOfWorks.add(unitOfWork);
    }

    int attemptSteps()
    {
        return removeIf(polledUnitOfWorks, step -> step.attemptToAction() == CONTINUE);
    }
}
