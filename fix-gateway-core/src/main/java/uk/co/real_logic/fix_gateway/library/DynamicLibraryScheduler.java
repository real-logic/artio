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
package uk.co.real_logic.fix_gateway.library;

import io.aeron.Aeron;
import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentRunner;

/**
 * Share the monitoring thread over multiple instances of a library.
 */
public class DynamicLibraryScheduler implements LibraryScheduler
{
    private final DynamicCompositeAgent dynamicAgent = new DynamicCompositeAgent("Monitoring+ClientConductor");

    // GuardedBy synchronized launch + close
    private final Int2ObjectHashMap<Agent> libraryIdToDelegateAgent = new Int2ObjectHashMap<>();

    private AgentRunner runner;

    public synchronized void launch(
        final LibraryConfiguration configuration,
        final ErrorHandler errorHandler,
        final Agent monitoringAgent)
    {
        if (runner == null)
        {
            // We shouldn't reach this default error handler because we catch exceptions in the CombinedAgent below.
            runner = new AgentRunner(
                    configuration.monitoringThreadIdleStrategy(), (e) -> e.printStackTrace(), null, dynamicAgent);
            AgentRunner.startOnThread(runner);
        }

        final int libraryId = configuration.libraryId();
        final Agent conductorAgent = configuration.conductorAgent();
        final Agent combinedAgent = new CombinedAgent(libraryId, monitoringAgent, conductorAgent, errorHandler);

        libraryIdToDelegateAgent.put(libraryId, combinedAgent);

        dynamicAgent.add(combinedAgent);
    }

    public synchronized void close(final int libraryId)
    {
        dynamicAgent.remove(libraryIdToDelegateAgent.remove(libraryId));

        if (libraryIdToDelegateAgent.isEmpty())
        {
            CloseHelper.close(runner);
            runner = null;
        }
    }

    public void configure(final Aeron.Context aeronContext)
    {
        aeronContext
            .useConductorAgentInvoker(true);
    }

    private static final class CombinedAgent implements Agent
    {
        private final Agent monitoringAgent;
        private final Agent clientConductorAgent;
        private final ErrorHandler errorHandler;
        private final String roleName;

        private CombinedAgent(
            final int libraryId,
            final Agent monitoringAgent,
            final Agent clientConductorAgent,
            final ErrorHandler errorHandler)
        {
            this.roleName = "[Library:" + libraryId + ":monitoring+conductor]";
            this.monitoringAgent = monitoringAgent;
            this.clientConductorAgent = clientConductorAgent;
            this.errorHandler = errorHandler;
        }

        public int doWork() throws Exception
        {
            int count = 0;

            try
            {
                count += monitoringAgent.doWork();
                count += clientConductorAgent.doWork();
            }
            catch (final Throwable throwable)
            {
                System.out.println("Here...");
                errorHandler.onError(throwable);
            }

            return count;
        }

        public String roleName()
        {
            return roleName;
        }
    }
}
