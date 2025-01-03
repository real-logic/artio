/*
 * Copyright 2015-2025 Real Logic Limited.
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

import uk.co.real_logic.artio.dictionary.generation.Exceptions;

import java.util.Optional;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

import static org.agrona.SystemUtil.isDebuggerAttached;
import static org.junit.Assert.fail;

public final class Timing
{
    // Long timeout, but one that doesn't cause long overflow.
    public static final long DEFAULT_TIMEOUT_IN_MS = isDebuggerAttached() ? Integer.MAX_VALUE : 20_000;

    public static <T> T withTimeout(final String message, final Supplier<Optional<T>> supplier, final long timeoutInMs)
    {
        return withTimeout(() -> message, supplier, timeoutInMs);
    }

    public static <T> T withTimeout(
        final Supplier<String> message, final Supplier<Optional<T>> supplier, final long timeoutInMs)
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

        fail(message.get());
        // Never run:
        throw new Error();
    }

    public static void assertEventuallyTrue(final String message, final BooleanSupplier condition)
    {
        assertEventuallyTrue(message, condition, DEFAULT_TIMEOUT_IN_MS);
    }

    public static void assertEventuallyTrue(
        final String message,
        final BooleanSupplier condition,
        final long timeoutInMs)
    {
        assertEventuallyTrue(
            () -> message,
            condition,
            timeoutInMs,
            Exceptions::printStackTracesForAllThreads);
    }

    public static void assertEventuallyTrue(final String message, final Block runnable)
    {
        assertEventuallyTrue(message, runnable, DEFAULT_TIMEOUT_IN_MS);
    }

    public static void assertEventuallyTrue(final String message, final Block runnable, final long timeoutInMs)
    {
        assertEventuallyTrue(() -> message, runnable, timeoutInMs);
    }

    public static void assertEventuallyTrue(
        final Supplier<String> message, final Block runnable, final long timeoutInMs)
    {
        final long endTime = System.currentTimeMillis() + timeoutInMs;

        Throwable lastThrowable;

        do
        {
            try
            {
                runnable.run();
                return;
            }
            catch (final Throwable ex)
            {
                lastThrowable = ex;
            }

            Thread.yield();
        }
        while (System.currentTimeMillis() < endTime);

        final AssertionError error = new AssertionError(message.get());
        error.addSuppressed(lastThrowable);
        throw error;
    }

    public static void assertEventuallyTrue(
        final Supplier<String> message,
        final BooleanSupplier condition,
        final long timeout,
        final Runnable failureCleanup)
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

        failureCleanup.run();

        fail(message.get());
    }

    public interface Block
    {
        void run() throws Exception;
    }
}
