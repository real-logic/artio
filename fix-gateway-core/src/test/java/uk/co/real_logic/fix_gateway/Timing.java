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

import org.agrona.LangUtil;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.IntSupplier;

import static org.junit.Assert.fail;

public final class Timing
{
    private static final long DEFAULT_TIMEOUT = hasDebuggerAttached() ? Long.MAX_VALUE : 5_000;

    public static void assertEventuallyTrue(final String message, final BooleanSupplier condition)
    {
        assertEventuallyTrue(message, condition, DEFAULT_TIMEOUT, 100);
    }

    public static void assertEventuallyEquals(
        final String message, final int expectedCount, final IntSupplier supplier)
    {
        final AtomicInteger count = new AtomicInteger(0);
        assertEventuallyTrue(message, () -> count.addAndGet(supplier.getAsInt()) >= expectedCount);
    }

    public static void assertEventuallyTrue(
        final String message,
        final Runnable runnable)
    {
        assertEventuallyTrue(message, runnable, DEFAULT_TIMEOUT);
    }

    public static void assertEventuallyTrue(
        final String message,
        final Runnable runnable,
        final long timeoutMs)
    {
        assertEventuallyTrue(message,
            () ->
            {
                try
                {
                    runnable.run();
                    return true;
                }
                catch (Throwable e)
                {
                    return false;
                }
            },
            timeoutMs, 100);
    }

    public static void assertEventuallyTrue(
        final String message,
        final BooleanSupplier condition,
        final long timeout,
        final long intervalMs)
    {
        final long startTime = System.currentTimeMillis();

        while ((System.currentTimeMillis() - startTime) < timeout)
        {
            if (condition.getAsBoolean())
            {
                return;
            }

            try
            {
                Thread.sleep(intervalMs);
            }
            catch (final InterruptedException ex)
            {
                LangUtil.rethrowUnchecked(ex);
            }
        }

        fail(message);
    }

    private static boolean hasDebuggerAttached()
    {
        final RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        final String jvmArguments = runtimeMXBean.getInputArguments().toString();

        return jvmArguments.contains("-agentlib:jdwp");
    }
}
