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

import static org.junit.Assert.*;
import static uk.co.real_logic.artio.messages.InitialAcceptedSessionOwner.SOLE_LIBRARY;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.*;

public class SocketBindingTest extends AbstractMessageBasedAcceptorSystemTest
{

    @Test
    public void shouldHaveIdempotentBind() throws IOException
    {
        setup(true, true);

        completeBind();

        assertConnectable();

        completeUnbind();
        completeBind();
        completeBind();

        assertConnectable();
    }

    @Test
    public void shouldHaveIdempotentUnbind() throws IOException
    {
        setup(true, true);

        completeUnbind();
        completeUnbind();

        assertCannotConnect();
    }

    @Test
    public void shouldReturnErrorWhenBindingWithoutAddress()
    {
        setup(true, false, false);

        final Reply<?> reply = engine.bind();
        awaitReply(reply);
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
    public void shouldDisconnectWhenRequestedWithUnbinding() throws IOException
    {
        setup(true, true);

        try (FixConnection connection = FixConnection.initiate(port))
        {
            logon(connection);
            completeUnbind(true);
            connection.readLogout();
            assertCannotConnect();
        }

        completeBind();
        assertConnectable();
    }

    @Test
    public void shouldAllowBindingToBeDeferred() throws IOException
    {
        setup(true, false);
        assertCannotConnect();
    }

    @Test
    public void shouldUnbindTcpPortWhenRequested() throws IOException
    {
        setup(true, true);

        assertConnectable();

        completeUnbind();

        assertCannotConnect();
    }

    @Test
    public void shouldRebindTcpPortWhenRequested() throws IOException
    {
        setup(true, true);

        completeUnbind();

        completeBind();

        assertConnectable();
    }

    @Test(expected = IllegalArgumentException.class)
    public void shouldValidateBindOnStartupConfiguration()
    {
        setup(true, true, false);
    }

    @Test
    public void shouldNotBindConnectionOnceSoleLibraryIsActiveWhenUnboundBefore() throws IOException
    {
        setupSoleLibrary(false);

        final FakeOtfAcceptor fakeOtfAcceptor = new FakeOtfAcceptor();
        final FakeHandler fakeHandler = new FakeHandler(fakeOtfAcceptor);
        try (FixLibrary library = newAcceptingLibrary(fakeHandler, nanoClock))
        {
            assertCannotConnect();

            completeBind();

            assertConnectable();
        }
    }

    @Test
    public void shouldBindConnectionOnceSoleLibraryIsActiveWhenBoundAtStartup() throws IOException
    {
        setupSoleLibrary(true);

        final FakeOtfAcceptor fakeOtfAcceptor = new FakeOtfAcceptor();
        final FakeHandler fakeHandler = new FakeHandler(fakeOtfAcceptor);
        try (FixLibrary library = newAcceptingLibrary(fakeHandler, nanoClock))
        {
            assertConnectable();
        }
    }

    @Test
    public void shouldBindConnectionOnceSoleLibraryIsActiveWhenBoundBefore() throws IOException
    {
        setupSoleLibrary(false);

        completeBind();

        // Cannot connect yet as there's no library
        assertCannotConnect();

        final FakeOtfAcceptor fakeOtfAcceptor = new FakeOtfAcceptor();
        final FakeHandler fakeHandler = new FakeHandler(fakeOtfAcceptor);
        try (FixLibrary library = newAcceptingLibrary(fakeHandler, nanoClock))
        {
            assertConnectable();
        }
    }

    @Test
    public void shouldUnBindConnectionOnceSoleLibraryTimesout() throws IOException
    {
        setupSoleLibrary(false);

        final FakeOtfAcceptor fakeOtfAcceptor = new FakeOtfAcceptor();
        final FakeHandler fakeHandler = new FakeHandler(fakeOtfAcceptor);
        try (FixLibrary library = newAcceptingLibrary(fakeHandler, nanoClock))
        {
        }

        awaitLibraryDisconnect(engine);

        assertCannotConnect();
    }

    @Test
    public void shouldDisconnectWhenRequestedWithUnbindingSoleLibrary() throws IOException
    {
        setupSoleLibrary(false);

        completeBind();

        // Cannot connect yet as there's no library
        assertCannotConnect();

        final FakeOtfAcceptor fakeOtfAcceptor = new FakeOtfAcceptor();
        final FakeHandler fakeHandler = new FakeHandler(fakeOtfAcceptor);
        try (FixLibrary library = newAcceptingLibrary(fakeHandler, nanoClock))
        {
            try (FixConnection connection = FixConnection.initiate(port))
            {
                completeUnbind(true);
                assertFalse(connection.isConnected());
            }
        }
    }

    private void setupSoleLibrary(final boolean shouldBind)
    {
        setup(true, shouldBind, true, SOLE_LIBRARY);
    }

    private void completeBind()
    {
        final Reply<?> bindReply = Reply.await(engine.bind());
        assertEquals(bindReply.toString(), Reply.State.COMPLETED, bindReply.state());
    }

    private void completeUnbind()
    {
        completeUnbind(false);
    }

    private void completeUnbind(final boolean disconnect)
    {
        final Reply<?> unbindReply = Reply.await(engine.unbind(disconnect));
        assertEquals(unbindReply.toString(), Reply.State.COMPLETED, unbindReply.state());
    }

    private void assertCannotConnect() throws IOException
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

    private void assertConnectable() throws IOException
    {
        try (FixConnection ignore = FixConnection.initiate(port))
        {
        }
    }

}
