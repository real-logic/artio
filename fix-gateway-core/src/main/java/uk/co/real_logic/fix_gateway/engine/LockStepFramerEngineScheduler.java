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
import org.agrona.ErrorHandler;
import org.agrona.LangUtil;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentInvoker;
import org.agrona.concurrent.AgentRunner;
import uk.co.real_logic.fix_gateway.dictionary.generation.Exceptions;

import static org.agrona.concurrent.AgentRunner.startOnThread;
import static uk.co.real_logic.fix_gateway.CommonConfiguration.backoffIdleStrategy;

public class LockStepFramerEngineScheduler implements EngineScheduler
{
    private AgentRunner archivingRunner;
    private AgentRunner monitoringRunner;
    private AgentInvoker framerInvoker;

    public void launch(
        final EngineConfiguration configuration,
        final ErrorHandler errorHandler,
        final Agent framer,
        final Agent archivingAgent,
        final Agent monitoringAgent,
        final Agent conductorAgent)
    {
        framerInvoker = new AgentInvoker(errorHandler, null, framer);
        framerInvoker.start();

        if (archivingRunner != null)
        {
            EngineScheduler.fail();
        }

        archivingRunner = new AgentRunner(
            configuration.archiverIdleStrategy(), errorHandler, null, archivingAgent);

        startOnThread(archivingRunner);

        if (monitoringAgent != null)
        {
            monitoringRunner = new AgentRunner(
                backoffIdleStrategy(), errorHandler, null, monitoringAgent);
            startOnThread(monitoringRunner);
        }
    }

    public int invokeFramer()
    {
        try
        {
            return framerInvoker.invoke();
        }
        catch (final Exception e)
        {
            LangUtil.rethrowUnchecked(e);
            return 0;
        }
    }

    public void close()
    {
        Exceptions.closeAll(framerInvoker, archivingRunner, monitoringRunner);
    }

    public void configure(final Aeron.Context aeronContext)
    {
    }

}
