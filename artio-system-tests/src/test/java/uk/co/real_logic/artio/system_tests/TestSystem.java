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
package uk.co.real_logic.artio.system_tests;

import uk.co.real_logic.artio.Reply;
import uk.co.real_logic.artio.Timing;
import uk.co.real_logic.artio.builder.Encoder;
import uk.co.real_logic.artio.engine.LockStepFramerEngineScheduler;
import uk.co.real_logic.artio.library.FixLibrary;
import uk.co.real_logic.artio.library.LibraryConfiguration;
import uk.co.real_logic.artio.session.Session;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static uk.co.real_logic.artio.FixMatchers.isConnected;
import static uk.co.real_logic.artio.Reply.State.COMPLETED;
import static uk.co.real_logic.artio.Timing.DEFAULT_TIMEOUT_IN_MS;
import static uk.co.real_logic.artio.Timing.assertEventuallyTrue;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.LIBRARY_LIMIT;

public class TestSystem
{
    private final List<FixLibrary> libraries;
    private final LockStepFramerEngineScheduler scheduler;

    public TestSystem(final LockStepFramerEngineScheduler scheduler, final FixLibrary... libraries)
    {
        this.scheduler = scheduler;
        this.libraries = new ArrayList<>();
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
    }

    public void assertConnected()
    {
        libraries.forEach((library) -> assertThat(library, isConnected()));
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
        add(library);
        assertEventuallyTrue(
            () -> "Unable to connect to engine",
            () ->
            {
                poll();

                return library.isConnected();
            },
            DEFAULT_TIMEOUT_IN_MS,
            () -> close(library));

        return library;
    }

    public void awaitCompletedReplies(final Reply<?>... replies)
    {
        for (final Reply<?> reply : replies)
        {
            awaitReply(reply);
            assertEquals(COMPLETED, reply.state());
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
        return Timing.withTimeout("Never received " + messageType, () ->
        {
            poll();

            return otfAcceptor.hasReceivedMessage(messageType).findFirst();
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
        assertEventuallyTrue(
            "Unable to send " + encoder.getClass().getSimpleName(),
            () ->
            {
                poll();
                return session.send(encoder) > 0;
            });
    }
}
