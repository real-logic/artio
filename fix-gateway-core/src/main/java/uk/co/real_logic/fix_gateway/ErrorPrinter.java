/*
 * Copyright 2015 Real Logic Ltd.
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

import uk.co.real_logic.agrona.concurrent.Agent;
import uk.co.real_logic.agrona.concurrent.AgentRunner;
import uk.co.real_logic.agrona.concurrent.BackoffIdleStrategy;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;

import java.util.List;

public class ErrorPrinter implements Agent
{
    private final ErrorBuffer buffer;

    private long lastTimeInMS = 0L;

    public static void main(String[] args)
    {
        final StaticConfiguration configuration = new StaticConfiguration();
        final MonitoringFile monitoringFile = new MonitoringFile(false, configuration);
        final ErrorPrinter printer = new ErrorPrinter(monitoringFile);
        final IdleStrategy idleStrategy = new BackoffIdleStrategy(1, 1, 1000, 1_000_000);
        final AgentRunner runner = new AgentRunner(idleStrategy, Throwable::printStackTrace, null, printer);
        runner.run();
    }

    public ErrorPrinter(final MonitoringFile monitoringFile)
    {
        buffer = new ErrorBuffer(monitoringFile.errorBuffer());
    }

    public int doWork() throws Exception
    {
        final long time = System.currentTimeMillis();
        final List<String> errors = buffer.errorsSince(lastTimeInMS);
        errors.forEach(System.err::println);
        lastTimeInMS = time;
        return errors.size();
    }

    public String roleName()
    {
        return "Error Printer";
    }
}
