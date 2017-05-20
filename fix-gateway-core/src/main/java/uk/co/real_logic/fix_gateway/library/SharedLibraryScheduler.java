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
package uk.co.real_logic.fix_gateway.library;

import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.CompositeAgent;
import uk.co.real_logic.fix_gateway.engine.EngineScheduler;

import java.util.ArrayList;
import java.util.List;

/**
 * Share the monitoring thread over multiple instances of a library.
 *
 * NB: closes all monitoring agents when the first agent is closed. Uses the error handler and
 * idle strategy of whichever agent is last for the agent runner itself.
 */
public class SharedLibraryScheduler implements LibraryScheduler
{
    private static final int AGENTS_PER_LIBRARY = 2;

    private final int libraryCount;
    private final List<Agent> agents = new ArrayList<>();

    private AgentRunner runner;

    public SharedLibraryScheduler(final int libraryCount)
    {
        this.libraryCount = libraryCount;
    }

    public synchronized void launch(
        final LibraryConfiguration configuration,
        final ErrorHandler errorHandler,
        final Agent monitoringAgent)
    {
        if (runner != null)
        {
            EngineScheduler.fail();
        }

        final Agent conductorAgent = configuration.conductorAgent();

        agents.add(monitoringAgent);
        agents.add(conductorAgent);

        if ((AGENTS_PER_LIBRARY * libraryCount) == agents.size())
        {
            runner = new AgentRunner(
                configuration.monitoringThreadIdleStrategy(), errorHandler, null, new CompositeAgent(agents));
        }
    }

    public boolean useConductorAgentInvoker()
    {
        return true;
    }

    public synchronized void close()
    {
        CloseHelper.close(runner);
    }
}
