/*
 * Copyright 2015-2020 Real Logic Limited.
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
package uk.co.real_logic.artio.system_tests;

import org.agrona.LangUtil;
import uk.co.real_logic.artio.Reply;
import uk.co.real_logic.artio.Timing;
import uk.co.real_logic.artio.builder.Encoder;
import uk.co.real_logic.artio.engine.LockStepFramerEngineScheduler;
import uk.co.real_logic.artio.library.FixLibrary;
import uk.co.real_logic.artio.library.ILink3Connection;
import uk.co.real_logic.artio.library.LibraryConfiguration;
import uk.co.real_logic.artio.session.Session;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import java.util.function.Predicate;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertEquals;
import static uk.co.real_logic.artio.Reply.State.COMPLETED;
import static uk.co.real_logic.artio.Timing.DEFAULT_TIMEOUT_IN_MS;
import static uk.co.real_logic.artio.Timing.assertEventuallyTrue;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.LIBRARY_LIMIT;

public class TestSystem
{
    private final List<FixLibrary> libraries;
    private final List<Runnable> operations;
    private final LockStepFramerEngineScheduler scheduler;

    public TestSystem(final LockStepFramerEngineScheduler scheduler, final FixLibrary... libraries)
    {
        this.scheduler = scheduler;
        this.libraries = new ArrayList<>();
        this.operations = new ArrayList<>();
        Collections.addAll(this.libraries, libraries);
    }

    public TestSystem(final FixLibrary... libraries)
    {
        this(null, libraries);
    }

    public void poll()
    {
        if (scheduler != null)
        {
            scheduler.invokeFramer();
            scheduler.invokeFramer();
        }
        libraries.forEach((library) -> library.poll(LIBRARY_LIMIT));
        operations.forEach(Runnable::run);
    }

    public void addOperation(final Runnable operation)
    {
        operations.add(operation);
    }

    public void removeOperation(final Runnable operation)
    {
        operations.remove(operation);
    }

    public void close(final FixLibrary library)
    {
        library.close();
        remove(library);
    }

    public void remove(final FixLibrary library)
    {
        libraries.remove(library);
    }

    public FixLibrary add(final FixLibrary library)
    {
        libraries.add(library);
        return library;
    }

    public FixLibrary connect(final LibraryConfiguration configuration)
    {
        final FixLibrary library = FixLibrary.connect(configuration);
        try
        {
            add(library);
            awaitConnected(library);

            return library;
        }
        catch (final Exception e)
        {
            library.close();
            LangUtil.rethrowUnchecked(e);
            return library;
        }
    }

    public void awaitConnected(final FixLibrary library)
    {
        assertThat(libraries, hasItem(library));
        assertEventuallyTrue(
            () -> "Unable to connect to engine",
            () ->
            {
                poll();

                return library.isConnected();
            },
            DEFAULT_TIMEOUT_IN_MS,
            () -> close(library));
    }

    public void awaitCompletedReplies(final Reply<?>... replies)
    {
        for (final Reply<?> reply : replies)
        {
            awaitReply(reply);
            assertEquals(reply.toString(), COMPLETED, reply.state());
        }
    }

    public <T> Reply<T> awaitReply(final Reply<T> reply)
    {
        assertEventuallyTrue(
            () -> "No reply from: " + reply,
            () ->
            {
                poll();

                return !reply.isExecuting();
            },
            DEFAULT_TIMEOUT_IN_MS,
            () ->
            {
            });

        return reply;
    }

    public FixMessage awaitMessageOf(final FakeOtfAcceptor otfAcceptor, final String messageType)
    {
        return awaitMessageOf(otfAcceptor, messageType, msg -> true);
    }

    public FixMessage awaitMessageOf(
        final FakeOtfAcceptor otfAcceptor, final String messageType, final Predicate<FixMessage> predicate)
    {
        return Timing.withTimeout("Never received " + messageType, () ->
        {
            poll();

            return otfAcceptor.receivedMessage(messageType).filter(predicate).findFirst();
        },
        Timing.DEFAULT_TIMEOUT_IN_MS);
    }

    public void awaitReceivedSequenceNumber(final Session session, final int sequenceNumber)
    {
        Timing.assertEventuallyTrue(session + " Never get to " + sequenceNumber, () ->
        {
            poll();

            return session.lastReceivedMsgSeqNum() == sequenceNumber;
        });
    }

    public void send(final Session session, final Encoder encoder)
    {
        awaitSend("Unable to send " + encoder.getClass().getSimpleName(), () -> session.trySend(encoder));
    }

    public void awaitSend(final String message, final LongSupplier operation)
    {
        await(message, () -> operation.getAsLong() > 0);
    }

    public void await(final String message, final BooleanSupplier predicate)
    {
        assertEventuallyTrue(
            message,
            () ->
            {
                poll();
                return predicate.getAsBoolean();
            });
    }

    public void awaitBlocking(final Runnable operation)
    {
        awaitBlocking(() ->
        {
            operation.run();
            return null;
        });
    }

    public <T> T awaitBlocking(final Callable<T> operation)
    {
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        final Future<T> future = executor.submit(operation);

        while (!future.isDone())
        {
            poll();

            Thread.yield();
        }

        try
        {
            return future.get();
        }
        catch (final InterruptedException | ExecutionException e)
        {
            LangUtil.rethrowUnchecked(e);
        }

        return null;
    }

    public void awaitUnbind(final ILink3Connection session)
    {
        await("Failed to unbind session", () -> session.state() == ILink3Connection.State.UNBOUND);
    }
}
