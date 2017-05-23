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
import org.agrona.collections.ArrayUtil;
import org.agrona.collections.Int2ObjectHashMap;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;

import java.util.Queue;

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
        final Agent monitoringAgent)
    {
        if (runner == null)
        {
            runner = new AgentRunner(
                    configuration.monitoringThreadIdleStrategy(), errorHandler, null, dynamicAgent);
            AgentRunner.startOnThread(runner);
        }

        final int libraryId = configuration.libraryId();
        final Agent conductorAgent = configuration.conductorAgent();

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

    private final class DynamicCompositeAgent implements Agent
    {
        private final Queue<Command> commandQueue = new ManyToOneConcurrentArrayQueue<>(10);
        private final String roleName;

        private Agent[] agents = new Agent[0];

        private DynamicCompositeAgent(final String roleName)
        {
            this.roleName = roleName;
        }

        private void add(final Agent agent)
        {
            perform(new Command(agent, true));
        }

        private void remove(final Agent agent)
        {
            perform(new Command(agent, false));
        }

        private void perform(final Command command)
        {
            while (!commandQueue.offer(command))
            {
                Thread.yield();
            }

            Thread.yield();

            while (!command.done)
            {
                Thread.yield();
            }
        }

        public int doWork() throws Exception
        {
            int operations = pollCommands();

            final Agent[] agents = this.agents;
            for (int i = 0; i < agents.length; i++)
            {
                operations += agents[i].doWork();
            }

            return operations;
        }

        private int pollCommands()
        {
            final Command command = commandQueue.poll();
            if (command != null)
            {
                if (command.isAdd)
                {
                    agents = ArrayUtil.add(agents, command.agent);
                }
                else
                {
                    agents = ArrayUtil.remove(agents, command.agent);

                    command.agent.onClose();
                }

                command.done = true;

                return 1;
            }

            return 0;
        }

        public String roleName()
        {
            return roleName;
        }
    }

    private final class Command
    {
        private final Agent agent;
        private final boolean isAdd;
        private volatile boolean done = false;

        private Command(final Agent agent, final boolean isAdd)
        {
            this.agent = agent;
            this.isAdd = isAdd;
        }
    }
}
