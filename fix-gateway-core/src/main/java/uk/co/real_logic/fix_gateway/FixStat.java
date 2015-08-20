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

import uk.co.real_logic.agrona.concurrent.AtomicBuffer;
import uk.co.real_logic.agrona.concurrent.CountersManager;
import uk.co.real_logic.agrona.concurrent.SigInt;
import uk.co.real_logic.fix_gateway.engine.EngineConfiguration;

import java.time.LocalTime;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * FixStat prints out the counter statistics from an operational fix gateway.
 * <p>
 * Takes the same configuration properties as the gateway, for example:
 *
 * -Dfix.counters.file=/tmp/fix-client/counters
 */
public class FixStat
{
    public static void main(String[] args) throws InterruptedException
    {
        final EngineConfiguration configuration = new EngineConfiguration();
        try (final MonitoringFile monitoringFile = new MonitoringFile(false, configuration))
        {
            final CountersManager countersManager = monitoringFile.createCountersManager();
            final AtomicBuffer countersBuffer = monitoringFile.countersBuffer();

            final AtomicBoolean running = new AtomicBoolean(true);
            SigInt.register(() -> running.set(false));

            while (running.get())
            {
                System.out.print("\033[H\033[2J");
                System.out.format("%1$tH:%1$tM:%1$tS - Fix Stat\n", LocalTime.now());
                System.out.println("=========================");

                countersManager.forEach(
                    (id, label) ->
                    {
                        final int offset = CountersManager.counterOffset(id);
                        final long value = countersBuffer.getLongVolatile(offset);

                        System.out.format("%3d: %,20d - %s\n", id, value, label);
                    });

                Thread.sleep(1000);
            }
        }
    }
}
