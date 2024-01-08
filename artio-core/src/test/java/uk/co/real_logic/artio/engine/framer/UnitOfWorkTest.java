/*
 * Copyright 2015-2024 Real Logic Limited.
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

import org.junit.Test;

import static io.aeron.Publication.BACK_PRESSURED;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.ABORT;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;
import static org.junit.Assert.assertEquals;

public class UnitOfWorkTest
{
    private long firstStep = 1;
    private final UnitOfWork unitOfWork = new UnitOfWork(() -> firstStep, () -> 1);

    @Test
    public void shouldContinueUnitOfWorkWhenComplete()
    {
        continuedAttempt();
    }

    @Test
    public void shouldAbortUnitOfWorkWhenInComplete()
    {
        firstStep = BACK_PRESSURED;
        abortedAttempt();

        firstStep = 1;
        continuedAttempt();
    }

    @Test
    public void shouldRetryAbortedUnitOfWorkUntilDone()
    {
        firstStep = BACK_PRESSURED;
        abortedAttempt();

        abortedAttempt();

        abortedAttempt();

        firstStep = 1;
        continuedAttempt();
    }

    private void continuedAttempt()
    {
        assertEquals(CONTINUE, unitOfWork.attemptToAction());
    }

    private void abortedAttempt()
    {
        assertEquals(ABORT, unitOfWork.attemptToAction());
    }

}
