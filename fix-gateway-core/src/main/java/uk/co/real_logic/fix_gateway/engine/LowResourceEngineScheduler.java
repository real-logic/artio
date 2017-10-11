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
package uk.co.real_logic.fix_gateway.engine;

import io.aeron.Aeron;
import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.CompositeAgent;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.agrona.concurrent.AgentRunner.startOnThread;

/**
 * A scheduler that schedules all engine agents onto a single thread.
 *
 * Can also (optionally schedule the media driver's agent onto the same thread.
 */
public class LowResourceEngineScheduler implements EngineScheduler
{
    private final AgentInvoker driverAgentInvoker;

    private AgentRunner runner;

    public LowResourceEngineScheduler()
    {
        this(null);
    }

    public LowResourceEngineScheduler(final AgentInvoker driverAgentInvoker)
    {
        this.driverAgentInvoker = driverAgentInvoker;
    }

    public void launch(
        final EngineConfiguration configuration,
        final ErrorHandler errorHandler,
        final Agent framer,
        final Agent archivingAgent,
        final Agent monitoringAgent,
        final Agent conductorAgent)
    {
        if (runner != null)
        {
            EngineScheduler.fail();
        }

        final List<Agent> agents = new ArrayList<>();
        Collections.addAll(agents, monitoringAgent, framer, archivingAgent, conductorAgent);
        if (driverAgentInvoker != null)
        {
            agents.add(driverAgentInvoker.agent());
        }

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
        EngineScheduler.awaitRunnerStart(runner);

        CloseHelper.close(runner);
    }

    public void configure(final Aeron.Context aeronContext)
    {
        aeronContext.useConductorAgentInvoker(true);

        if (driverAgentInvoker != null)
        {
            aeronContext.driverAgentInvoker(driverAgentInvoker);
        }
    }
}
