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
import uk.co.real_logic.fix_gateway.Pressure;

import java.util.Arrays;
import java.util.List;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.BREAK;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;

class Transaction
{
    private final List<Continuation> continuationList;

    private int index = 0;

    Transaction(final Continuation... continuations)
    {
        this(Arrays.asList(continuations));
    }

    Transaction(final List<Continuation> continuationList)
    {
        this.continuationList = continuationList;
    }

    Action attempt()
    {
        for (final int size = continuationList.size(); index < size; index++)
        {
            final Continuation continuation = continuationList.get(index);
            final Action action = Pressure.apply(continuation.attempt());

            if (action == BREAK)
            {
                return BREAK;
            }
        }

        return CONTINUE;
    }
}
