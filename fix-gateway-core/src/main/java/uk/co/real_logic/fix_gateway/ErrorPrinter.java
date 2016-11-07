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

import org.agrona.concurrent.*;
import org.agrona.concurrent.errors.ErrorConsumer;
import org.agrona.concurrent.errors.ErrorLogReader;
import uk.co.real_logic.fix_gateway.engine.EngineConfiguration;

import static uk.co.real_logic.fix_gateway.CommonConfiguration.DEFAULT_NAME_PREFIX;

public class ErrorPrinter implements Agent
{
    public static void main(String[] args)
    {
        final EngineConfiguration configuration = new EngineConfiguration();
        configuration.libraryAeronChannel("").conclude();
        final MonitoringFile monitoringFile = new MonitoringFile(false, configuration);
        final ErrorPrinter printer = new ErrorPrinter(monitoringFile.errorBuffer(), DEFAULT_NAME_PREFIX);
        final IdleStrategy idleStrategy = new BackoffIdleStrategy(1, 1, 1000, 1_000_000);
        final AgentRunner runner = new AgentRunner(idleStrategy, Throwable::printStackTrace, null, printer);
        runner.run();
    }

    private final ErrorConsumer errorConsumer =
        (observationCount, firstObservationTimestamp, lastObservationTimestamp, encodedException) ->
        {
            System.err.println(encodedException);
            System.err.println();
        };
    private final AtomicBuffer errorBuffer;
    private final String agentNamePrefix;

    private long lastSeenErrorTime = 0L;

    public ErrorPrinter(final AtomicBuffer errorBuffer, final String agentNamePrefix)
    {
        this.errorBuffer = errorBuffer;
        this.agentNamePrefix = agentNamePrefix;
    }

    public int doWork() throws Exception
    {
        final long time = System.nanoTime();
        if (time > lastSeenErrorTime)
        {
            final int errors = ErrorLogReader.read(errorBuffer, errorConsumer, lastSeenErrorTime);
            if (errors > 0)
            {
                lastSeenErrorTime = time;
            }
            return errors;
        }

        return 0;
    }

    public String roleName()
    {
        return agentNamePrefix + "Error Printer";
    }
}
