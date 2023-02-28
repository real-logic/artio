/*
 * Copyright 2015-2023 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.co.real_logic.artio;

import io.aeron.driver.MediaDriver;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.SigInt;
import org.agrona.concurrent.status.AtomicCounter;
import uk.co.real_logic.artio.library.FixLibrary;
import uk.co.real_logic.artio.library.LibraryConfiguration;

import java.util.concurrent.atomic.AtomicBoolean;

public final class SampleUtil
{
    public static FixLibrary blockingConnect(final LibraryConfiguration configuration)
    {
        final FixLibrary library = FixLibrary.connect(configuration);
        while (!library.isConnected())
        {
            library.poll(1);
            Thread.yield();
        }
        return library;
    }

    public static void runAgentUntilSignal(
        final Agent agent, final MediaDriver mediaDriver) throws InterruptedException
    {
        final AtomicCounter errorCounter =
            mediaDriver.context().countersManager().newCounter("exchange_agent_errors");
        final AgentRunner runner = new AgentRunner(
            CommonConfiguration.backoffIdleStrategy(),
            Throwable::printStackTrace,
            errorCounter,
            agent);

        final Thread thread = AgentRunner.startOnThread(runner);

        final AtomicBoolean running = new AtomicBoolean(true);
        SigInt.register(() -> running.set(false));

        while (running.get())
        {
            Thread.sleep(100);
        }

        thread.join();
    }
}
