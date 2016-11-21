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

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

import static org.junit.Assert.fail;

public final class Timing
{
    private static final long DEFAULT_TIMEOUT_IN_MS = hasDebuggerAttached() ? Long.MAX_VALUE : 5_000;

    public static <T> T withTimeout(
        final String message,
        final Supplier<Optional<T>> supplier,
        final long timeoutInMs)
    {
        final long endTime = System.currentTimeMillis() + timeoutInMs;

        do
        {
            final Optional<T> value = supplier.get();
            if (value.isPresent())
            {
                return value.get();
            }

            Thread.yield();
        }
        while (System.currentTimeMillis() < endTime);

        fail(message);
        // Never run:
        throw new Error();
    }

    public static void assertEventuallyTrue(final String message, final BooleanSupplier condition)
    {
        assertEventuallyTrue(message, condition, DEFAULT_TIMEOUT_IN_MS);
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
        assertEventuallyTrue(message, runnable, DEFAULT_TIMEOUT_IN_MS);
    }

    public static void assertEventuallyTrue(
        final String message,
        final Runnable runnable,
        final long timeoutInMs)
    {
        assertEventuallyTrue(message,
            () ->
            {
                try
                {
                    runnable.run();
                    return true;
                }
                catch (final Throwable ignore)
                {
                    return false;
                }
            },
            timeoutInMs
        );
    }

    public static void assertEventuallyTrue(
        final String message,
        final BooleanSupplier condition,
        final long timeout)
    {
        final long endTime = System.currentTimeMillis() + timeout;

        do
        {
            if (condition.getAsBoolean())
            {
                return;
            }

            Thread.yield();
        }
        while (System.currentTimeMillis() < endTime);

        fail(message);
    }

    private static boolean hasDebuggerAttached()
    {
        final RuntimeMXBean runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        final String jvmArguments = runtimeMXBean.getInputArguments().toString();

        return jvmArguments.contains("-agentlib:jdwp");
    }
}
