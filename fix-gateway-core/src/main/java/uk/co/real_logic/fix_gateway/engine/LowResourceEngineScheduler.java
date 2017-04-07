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
package uk.co.real_logic.fix_gateway.engine;

import org.agrona.ErrorHandler;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.CompositeAgent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.agrona.concurrent.AgentRunner.startOnThread;

/**
 * A scheduler that schedules all engine and library agents onto a single thread.
 */
public class LowResourceEngineScheduler implements EngineScheduler
{
    private AgentRunner runner;

    public void launch(
        final EngineConfiguration configuration,
        final ErrorHandler errorHandler,
        final Agent framer,
        final Agent archivingAgent,
        final Agent monitoringAgent)
    {
        if (runner != null)
        {
            EngineScheduler.fail();
        }

        final List<Agent> agents = new ArrayList<>();
        Collections.addAll(agents, framer, archivingAgent, monitoringAgent);
        agents.removeIf(Objects::isNull);

        runner = new AgentRunner(
            configuration.framerIdleStrategy(),
            errorHandler,
            null,
                new CompositeAgent(agents));
        startOnThread(runner);
    }

    public void close()
    {
        runner.close();
    }
}
