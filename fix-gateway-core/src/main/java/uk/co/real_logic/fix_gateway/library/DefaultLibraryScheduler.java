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

import io.aeron.Aeron;
import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.CompositeAgent;
import uk.co.real_logic.fix_gateway.engine.EngineScheduler;

import static org.agrona.concurrent.AgentRunner.startOnThread;
import static uk.co.real_logic.fix_gateway.CommonConfiguration.backoffIdleStrategy;

public class DefaultLibraryScheduler implements LibraryScheduler
{
    private AgentRunner monitoringRunner;

    public void launch(
        final LibraryConfiguration configuration,
        final ErrorHandler errorHandler,
        final Agent monitoringAgent)
    {
        if (monitoringRunner != null)
        {
            EngineScheduler.fail();
        }

        if (monitoringAgent != null)
        {
            final Agent conductorAgent = configuration.conductorAgent();

            monitoringRunner = new AgentRunner(
                backoffIdleStrategy(),
                errorHandler,
                null,
                new CompositeAgent(monitoringAgent, conductorAgent));
            startOnThread(monitoringRunner);
        }
    }

    public void configure(final Aeron.Context aeronContext)
    {
        aeronContext.useConductorAgentInvoker(true);
    }

    public void close()
    {
        CloseHelper.close(monitoringRunner);
    }
}
