/*
 * Copyright 2015-2018 Real Logic Ltd, Adaptive Financial Consulting Ltd.
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
package uk.co.real_logic.artio.engine;

import org.agrona.ErrorHandler;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.BusySpinIdleStrategy;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.Mockito.*;
import static uk.co.real_logic.artio.Timing.assertEventuallyTrue;

public class LowResourceEngineSchedulerTest
{
    private Agent framer = mock(Agent.class);
    private Agent archivingAgent = mock(Agent.class);
    private Agent monitoringAgent = mock(Agent.class);
    private EngineConfiguration configuration = mock(EngineConfiguration.class);
    private Agent conductorAgent = mock(Agent.class);
    private ErrorHandler mockErrorHandler = mock(ErrorHandler.class);
    private RecordingCoordinator recordingCoordinator = mock(RecordingCoordinator.class);

    @Test
    public void shouldPrintErrorIfRepeatedlyThrown() throws Exception
    {
        when(configuration.framerIdleStrategy()).thenReturn(new BusySpinIdleStrategy());
        when(configuration.threadFactory()).thenReturn(Thread::new);
        when(framer.doWork()).thenThrow(IOException.class);

        try (EngineScheduler scheduler = new LowResourceEngineScheduler())
        {
            scheduler.launch(
                configuration,
                mockErrorHandler,
                framer,
                archivingAgent,
                monitoringAgent,
                conductorAgent,
                recordingCoordinator);

            assertEventuallyTrue(
                "Failed to invoke monitoring agent",
                () -> verify(monitoringAgent, atLeastOnce()).doWork()
            );
        }
    }
}
