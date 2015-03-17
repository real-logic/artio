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

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.function.BooleanSupplier;

import static org.junit.Assert.fail;

public final class Timing
{
    private static final long DEFAULT_TIMEOUT = hasDebuggerAttached() ? Long.MAX_VALUE : 200;

    public static void assertEventuallyTrue(
            final String message,
            final BooleanSupplier condition) throws InterruptedException
    {
        assertEventuallyTrue(message, condition, DEFAULT_TIMEOUT);
    }

    public static void assertEventuallyTrue(
        final String message,
        final BooleanSupplier condition,
        final long timeout) throws InterruptedException
    {
        final long startTime = System.currentTimeMillis();

        while ((System.currentTimeMillis() - startTime) < timeout)
        {
            if (condition.getAsBoolean())
            {
                return;
            }

            Thread.sleep(10);
        }

        fail(message);
    }

    private static boolean hasDebuggerAttached()
    {
        RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        String jvmArguments = runtimeMXBean.getInputArguments().toString();
        return jvmArguments.contains("-agentlib:jdwp");
    }

}
