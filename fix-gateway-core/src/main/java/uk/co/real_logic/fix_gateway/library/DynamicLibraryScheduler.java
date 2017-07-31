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
    private final Int2ObjectHashMap<Agent> libraryIdToMonitoring = new Int2ObjectHashMap<>();
    private final Int2ObjectHashMap<Agent> libraryIdToClientConductor = new Int2ObjectHashMap<>();
    private AgentRunner runner;

    public synchronized void launch(
            final LibraryConfiguration configuration,
            final ErrorHandler errorHandler,
            final Agent monitoringAgent, final Agent conductorAgent)
    {
        if (runner == null)
        {
            runner = new AgentRunner(
                    configuration.monitoringThreadIdleStrategy(), errorHandler, null, dynamicAgent);
            AgentRunner.startOnThread(runner);
        }

        final int libraryId = configuration.libraryId();

        libraryIdToMonitoring.put(libraryId, monitoringAgent);
        libraryIdToClientConductor.put(libraryId, conductorAgent);

        dynamicAgent.add(monitoringAgent);
        dynamicAgent.add(conductorAgent);
    }

    public synchronized void close(final int libraryId)
    {
        dynamicAgent.remove(libraryIdToMonitoring.remove(libraryId));
        dynamicAgent.remove(libraryIdToClientConductor.remove(libraryId));

        if (libraryIdToMonitoring.isEmpty())
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

}
