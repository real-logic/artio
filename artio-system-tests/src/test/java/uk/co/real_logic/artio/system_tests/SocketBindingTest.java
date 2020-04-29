/*
 * Copyright 2015-2020 Adaptive Financial Consulting Ltd.
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

import org.junit.Test;
import uk.co.real_logic.artio.Reply;
import uk.co.real_logic.artio.library.FixLibrary;

import java.io.IOException;
import java.net.ConnectException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static uk.co.real_logic.artio.Timing.assertEventuallyTrue;
import static uk.co.real_logic.artio.messages.InitialAcceptedSessionOwner.SOLE_LIBRARY;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.*;

public class SocketBindingTest extends AbstractMessageBasedAcceptorSystemTest
{

    @Test
    public void shouldHaveIdempotentBind() throws IOException
    {
        setup(true, true);

        completeBind();

        try (FixConnection ignore = FixConnection.initiate(port))
        {
        }

        completeUnbind();
        completeBind();
        completeBind();

        try (FixConnection ignore = FixConnection.initiate(port))
        {
        }
    }

    @Test
    public void shouldHaveIdempotentUnbind() throws IOException
    {
        setup(true, true);

        completeUnbind();
        completeUnbind();

        cannotConnect();
    }

    @Test
    public void shouldReturnErrorWhenBindingWithoutAddress()
    {
        setup(true, false, false);

        final Reply<?> reply = engine.bind();
        SystemTestUtil.awaitReply(reply);
        assertEquals(reply.toString(), Reply.State.ERRORED, reply.state());

        assertEquals("Missing address: EngineConfiguration.bindTo()", reply.error().getMessage());
    }

    @Test
    public void shouldNotDisconnectWhenUnbinding() throws IOException
    {
        setup(true, true);

        try (FixConnection connection = FixConnection.initiate(port))
        {
            logon(connection);
            completeUnbind();
            connection.logoutAndAwaitReply();
        }
    }

    @Test
    public void shouldAllowBindingToBeDeferred() throws IOException
    {
        setup(true, false);
        cannotConnect();
    }

    @Test
    public void shouldUnbindTcpPortWhenRequested() throws IOException
    {
        setup(true, true);

        try (FixConnection ignore = FixConnection.initiate(port))
        {
        }

        completeUnbind();

        cannotConnect();
    }

    @Test
    public void shouldRebindTcpPortWhenRequested() throws IOException
    {
        setup(true, true);

        completeUnbind();

        completeBind();

        try (FixConnection ignore = FixConnection.initiate(port))
        {
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldValidateSoleLibraryModeConfiguration()
    {
        setupSoleLibrary(true);
    }

    @Test
    public void shouldBindConnectionOnceSoleLibraryIsActive() throws IOException
    {
        setupSoleLibrary(false);

        final FakeOtfAcceptor fakeOtfAcceptor = new FakeOtfAcceptor();
        final FakeHandler fakeHandler = new FakeHandler(fakeOtfAcceptor);
        try (FixLibrary library = newAcceptingLibrary(fakeHandler))
        {
            try (FixConnection connection = FixConnection.initiate(port))
            {
            }
        }
    }

    @Test
    public void shouldUnBindConnectionOnceSoleLibraryTimesout() throws IOException
    {
        setupSoleLibrary(false);

        final FakeOtfAcceptor fakeOtfAcceptor = new FakeOtfAcceptor();
        final FakeHandler fakeHandler = new FakeHandler(fakeOtfAcceptor);
        try (FixLibrary library = newAcceptingLibrary(fakeHandler))
        {
        }

        awaitLibraryDisconnect();

        cannotConnect();
    }

    private void awaitLibraryDisconnect()
    {
        assertEventuallyTrue(
            () -> "libraries haven't disconnected yet",
            () ->
            {
                return libraries(engine).size() == 1;
            },
            AWAIT_TIMEOUT,
            () ->
            {
            }
        );
    }

    private void setupSoleLibrary(final boolean shouldBind)
    {
        setup(true, shouldBind, true, SOLE_LIBRARY);
    }

    private void completeBind()
    {
        final Reply<?> bindReply = engine.bind();
        SystemTestUtil.awaitReply(bindReply);
        assertEquals(bindReply.toString(), Reply.State.COMPLETED, bindReply.state());
    }

    private void completeUnbind()
    {
        final Reply<?> unbindReply = engine.unbind();
        SystemTestUtil.awaitReply(unbindReply);
        assertEquals(unbindReply.toString(), Reply.State.COMPLETED, unbindReply.state());
    }

    private void cannotConnect() throws IOException
    {
        try
        {
            FixConnection.initiate(port);
        }
        catch (final ConnectException ignore)
        {
            return;
        }

        fail("expected ConnectException");
    }

}
