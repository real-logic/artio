/*
 * Copyright 2015-2020 Real Logic Limited, Adaptive Financial Consulting Ltd.
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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.artio.Reply;
import uk.co.real_logic.artio.builder.Encoder;
import uk.co.real_logic.artio.builder.RejectEncoder;
import uk.co.real_logic.artio.decoder.AbstractLogonDecoder;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.library.LibraryConfiguration;
import uk.co.real_logic.artio.session.Session;
import uk.co.real_logic.artio.validation.AuthenticationProxy;
import uk.co.real_logic.artio.validation.AuthenticationStrategy;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static uk.co.real_logic.artio.Constants.LOGON_MESSAGE_AS_STR;
import static uk.co.real_logic.artio.TestFixtures.launchMediaDriver;
import static uk.co.real_logic.artio.Timing.assertEventuallyTrue;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.*;

public class AsyncAuthenticatorTest extends AbstractGatewayToGatewaySystemTest
{
    private static final long LINGER_TIMEOUT_IN_MS = 500L;
    private static final long AUTHENTICATION_TIMEOUT_IN_MS = 500L;
    private final ControllableAuthenticationStrategy auth = new ControllableAuthenticationStrategy();

    @Before
    public void launch()
    {
        mediaDriver = launchMediaDriver();

        final EngineConfiguration acceptingConfig = acceptingConfig(port, ACCEPTOR_ID, INITIATOR_ID);
        acceptingConfig.deleteLogFileDirOnStart(true);
        acceptingConfig.printErrorMessages(false);
        acceptingConfig.authenticationStrategy(auth);
        acceptingConfig.authenticationTimeoutInMs(AUTHENTICATION_TIMEOUT_IN_MS);

        acceptingEngine = FixEngine.launch(acceptingConfig);
        initiatingEngine = launchInitiatingEngine(libraryAeronPort);

        final LibraryConfiguration acceptingLibraryConfig = acceptingLibraryConfig(acceptingHandler);
        acceptingLibrary = connect(acceptingLibraryConfig);
        initiatingLibrary = newInitiatingLibrary(libraryAeronPort, initiatingHandler);
        testSystem = new TestSystem(acceptingLibrary, initiatingLibrary);
    }

    @Test
    public void shouldConnectedAcceptedAuthentications()
    {
        final Reply<Session> reply = acquireAuthProxy();

        auth.accept();
        completeConnectInitiatingSession(reply);
        messagesCanBeExchanged();
        assertInitiatingSequenceIndexIs(0);
    }

    @Test
    public void logonsCanBeRejected()
    {
        final Reply<Session> reply = acquireAuthProxy();

        auth.reject();

        assertDisconnectRejected(reply);
    }

    @Test
    public void logonsCanBeRejectedWithCustomMessages()
    {
        final Reply<Session> reply = acquireAuthProxy();

        final RejectEncoder rejectEncoder = newRejectEncoder();

        final long startTime = System.currentTimeMillis();
        auth.reject(rejectEncoder, LINGER_TIMEOUT_IN_MS);

        assertDisconnectRejected(reply);
        final long rejectTime = System.currentTimeMillis() - startTime;
        assertThat(rejectTime, greaterThanOrEqualTo(LINGER_TIMEOUT_IN_MS));
        assertThat(rejectTime, lessThan(2 * LINGER_TIMEOUT_IN_MS));

        final EngineConfiguration config = initiatingEngine.configuration();
        final List<String> messages = getMessagesFromArchive(config, config.inboundLibraryStream());
        assertThat(messages, hasSize(1));
        final String rejectMessage = messages.get(0);
        assertThat(rejectMessage, containsString("372=A\00158=Invalid Logon"));
        assertThat(rejectMessage, containsString("35=3\00149=acceptor\00156=initiator\00134=1"));
    }

    @Test(expected = NullPointerException.class)
    public void rejectWithEncoderMustProvideAnEncoder()
    {
        acquireAuthProxy();

        try
        {
            auth.reject(null, LINGER_TIMEOUT_IN_MS);
        }
        finally
        {
            // Test optimisation.
            auth.reject();
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void lingerTimeoutShouldBeValid()
    {
        acquireAuthProxy();

        final RejectEncoder encoder = newRejectEncoder();

        try
        {
            auth.reject(encoder, -1);
        }
        finally
        {
            // Test optimisation.
            auth.reject();
        }
    }

    @Test
    public void invalidEncoderShouldStillDisconnect()
    {
        final Reply<Session> reply = acquireAuthProxy();

        final RejectEncoder invalidEncoder = new RejectEncoder();

        auth.reject(invalidEncoder, 0);

        assertDisconnectRejected(reply);
    }

    @Test
    public void shouldDisconnectSessionsWhenAuthStrategyFails()
    {
        auth.throwWhenInvoked = true;

        final Reply<Session> reply = acquireAuthProxy();

        assertDisconnectRejected(reply);
    }

    @Test
    public void messagesCanBeSentFromInitiatorToAcceptorAfterFailedAuthenticationAttempt()
    {
        final Reply<Session> invalidReply = acquireAuthProxy();

        auth.reject();

        completeFailedSession(invalidReply);

        auth.reset();

        final Reply<Session> validReply = acquireAuthProxy();

        auth.accept();
        completeConnectInitiatingSession(validReply);
        messagesCanBeExchanged();
        assertInitiatingSequenceIndexIs(1);
    }

    @Test
    public void shouldOnlyUseFirstMethodCall()
    {
        final Reply<Session> reply = acquireAuthProxy();

        auth.accept();

        completeConnectInitiatingSession(reply);

        try
        {
            auth.reject();
            fail("Should not allow a reject after an accept");
        }
        catch (final IllegalStateException e)
        {
            // Deliberately blank
        }

        messagesCanBeExchanged();
        assertInitiatingSequenceIndexIs(0);
    }

    @Test
    public void shouldDisconnectPendingAuthenticationAfterTimeout()
    {
        final long start = System.currentTimeMillis();
        final Reply<Session> reply = acquireAuthProxy();
        assertDisconnectRejected(reply);
        final long duration = System.currentTimeMillis() - start;
        assertThat(duration, is(greaterThanOrEqualTo(AUTHENTICATION_TIMEOUT_IN_MS)));
        assertThat(duration, is(lessThan(TEST_REPLY_TIMEOUT_IN_MS)));
    }

    @After
    public void teardown()
    {
        auth.verifyNoBlockingCalls();
    }

    private RejectEncoder newRejectEncoder()
    {
        final RejectEncoder rejectEncoder = new RejectEncoder();
        rejectEncoder.refMsgType(LOGON_MESSAGE_AS_STR);
        rejectEncoder.refSeqNum(1);
        rejectEncoder.text("Invalid Logon");
        return rejectEncoder;
    }

    private void assertDisconnectRejected(final Reply<Session> reply)
    {
        testSystem.awaitReply(reply);
        assertEquals(reply.toString(), Reply.State.ERRORED, reply.state());
        assertThat(reply.error().getMessage(), containsString("UNABLE_TO_LOGON: Disconnected before session active"));
    }

    private Reply<Session> acquireAuthProxy()
    {
        final Reply<Session> reply = initiate(initiatingLibrary, port, INITIATOR_ID, ACCEPTOR_ID);

        assertEventuallyTrue("failed to receive auth proxy", () ->
        {
            testSystem.poll();
            return auth.hasAuthenticateBeenInvoked();
        }, TEST_REPLY_TIMEOUT_IN_MS);

        assertEquals(Reply.State.EXECUTING, reply.state());

        return reply;
    }

    private static class ControllableAuthenticationStrategy implements AuthenticationStrategy
    {
        private volatile boolean throwWhenInvoked;
        private volatile boolean blockingAuthenticateCalled;
        private volatile AuthenticationProxy authProxy;

        public void authenticateAsync(final AbstractLogonDecoder logon, final AuthenticationProxy authProxy)
        {
            this.authProxy = authProxy;

            assertThat(authProxy.remoteAddress(), containsString("127.0.0.1"));

            if (throwWhenInvoked)
            {
                throw new RuntimeException("Broken application code");
            }
        }

        public boolean authenticate(final AbstractLogonDecoder logon)
        {
            blockingAuthenticateCalled = true;

            throw new UnsupportedOperationException();
        }

        void verifyNoBlockingCalls()
        {
            assertFalse(blockingAuthenticateCalled);
        }

        void accept()
        {
            authProxy.accept();
        }

        void reject()
        {
            authProxy.reject();
        }

        void reject(final Encoder encoder, final long lingerTimeoutInMs)
        {
            authProxy.reject(encoder, lingerTimeoutInMs);
        }

        boolean hasAuthenticateBeenInvoked()
        {
            return authProxy != null;
        }

        void reset()
        {
            authProxy = null;
        }
    }
}
