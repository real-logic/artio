/*
 * Copyright 2015-2025 Real Logic Limited.
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

import io.aeron.logbuffer.ControlledFragmentHandler;
import org.junit.Test;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.ABORT;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;
import static org.mockito.Mockito.*;

public class RetryManagerTest
{
    private static final long CORRELATION_ID = 7357572283568605721L;

    private final UnitOfWork unitOfWork = mock(UnitOfWork.class);
    private final RetryManager retryManager = new RetryManager();

    @Test
    public void shouldNotRetryIfExecutedFirstTime()
    {
        firstAttempt();

        retry();

        numberOfAttempts(1);
    }

    @Test
    public void shouldRetryWhenAborted()
    {
        attemptResultsIn(ABORT);

        firstAttempt();

        retry();

        numberOfAttempts(2);
    }

    @Test
    public void shouldRemoveWhenSucceeded()
    {
        attemptResultsIn(ABORT);

        firstAttempt();

        attemptResultsIn(CONTINUE);

        retry();

        retry();

        numberOfAttempts(2);
    }

    private void firstAttempt()
    {
        retryManager.firstAttempt(CORRELATION_ID, unitOfWork);
    }

    private void retry()
    {
        retryManager.retry(CORRELATION_ID);
    }

    private void attemptResultsIn(final ControlledFragmentHandler.Action action)
    {
        when(unitOfWork.attemptToAction()).thenReturn(action);
    }

    private void numberOfAttempts(final int wantedNumberOfInvocations)
    {
        verify(unitOfWork, times(wantedNumberOfInvocations)).attemptToAction();
    }

}
