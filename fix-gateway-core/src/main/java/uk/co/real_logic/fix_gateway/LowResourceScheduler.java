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
package uk.co.real_logic.fix_gateway;

import org.agrona.ErrorHandler;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.CompositeAgent;
import uk.co.real_logic.fix_gateway.engine.EngineConfiguration;
import uk.co.real_logic.fix_gateway.engine.EngineScheduler;
import uk.co.real_logic.fix_gateway.library.LibraryConfiguration;
import uk.co.real_logic.fix_gateway.library.LibraryScheduler;

import static uk.co.real_logic.fix_gateway.CommonConfiguration.backoffIdleStrategy;

/**
 * A scheduler that schedules all engine and library agents onto a single thread.
 */
public class LowResourceScheduler implements EngineScheduler, LibraryScheduler
{
    private AgentRunner runner;

    private ErrorHandler errorHandler;
    private Agent framer;
    private Agent archivingAgent;
    private Agent engineMonitoringAgent;
    private Agent libraryMonitoringAgent;

    public synchronized void launch(
        final LibraryConfiguration configuration,
        final ErrorHandler errorHandler,
        final Agent monitoringAgent)
    {
        if (libraryMonitoringAgent != null)
        {
            EngineScheduler.fail();
        }

        this.libraryMonitoringAgent = monitoringAgent;
        launchIfInitialised();
    }

    public synchronized void launch(
        final EngineConfiguration configuration,
        final ErrorHandler errorHandler,
        final Agent framer,
        final Agent archivingAgent,
        final Agent monitoringAgent)
    {
        if (this.framer != null)
        {
            EngineScheduler.fail();
        }

        this.errorHandler = errorHandler;
        this.framer = framer;
        this.archivingAgent = archivingAgent;
        this.engineMonitoringAgent = monitoringAgent;
        launchIfInitialised();
    }

    private void launchIfInitialised()
    {
        if (libraryMonitoringAgent != null && engineMonitoringAgent != null)
        {
            runner = new AgentRunner(
                backoffIdleStrategy(), errorHandler, null, new CompositeAgent(
                    framer, archivingAgent, engineMonitoringAgent, libraryMonitoringAgent));
        }
    }

    public synchronized void close()
    {
        runner.close();
    }
}
