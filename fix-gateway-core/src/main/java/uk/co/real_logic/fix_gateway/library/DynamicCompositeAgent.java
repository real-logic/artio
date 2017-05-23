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

import org.agrona.collections.ArrayUtil;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.ManyToOneConcurrentArrayQueue;

import java.util.Queue;

public final class DynamicCompositeAgent implements Agent
{
    private final Queue<Command> commandQueue = new ManyToOneConcurrentArrayQueue<>(10);
    private final String roleName;

    private Agent[] agents = new Agent[0];

    public DynamicCompositeAgent(final String roleName)
    {
        this.roleName = roleName;
    }

    public void add(final Agent agent)
    {
        perform(new Command(agent, true));
    }

    public void remove(final Agent agent)
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
            } else
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
