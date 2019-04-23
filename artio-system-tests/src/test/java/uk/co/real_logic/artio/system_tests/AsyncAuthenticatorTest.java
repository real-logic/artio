package uk.co.real_logic.artio.system_tests;

import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.artio.Reply;
import uk.co.real_logic.artio.decoder.LogonDecoder;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.library.LibraryConfiguration;
import uk.co.real_logic.artio.session.Session;
import uk.co.real_logic.artio.validation.AuthenticationProxy;
import uk.co.real_logic.artio.validation.AuthenticationStrategy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static uk.co.real_logic.artio.TestFixtures.launchMediaDriver;
import static uk.co.real_logic.artio.Timing.assertEventuallyTrue;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.*;

public class AsyncAuthenticatorTest extends AbstractGatewayToGatewaySystemTest
{
    public static final int DEFAULT_TIMEOUT_IN_MS = 1_000;
    private final FakeConnectHandler fakeConnectHandler = new FakeConnectHandler();
    private final MyAuthenticationStrategy auth = new MyAuthenticationStrategy();

    @Before
    public void launch()
    {
        delete(ACCEPTOR_LOGS);

        mediaDriver = launchMediaDriver();

        final EngineConfiguration acceptingConfig = acceptingConfig(port, ACCEPTOR_ID, INITIATOR_ID);
        acceptingConfig.authenticationStrategy(auth);

        acceptingEngine = FixEngine.launch(acceptingConfig);
        initiatingEngine = launchInitiatingEngine(libraryAeronPort);

        final LibraryConfiguration acceptingLibraryConfig = acceptingLibraryConfig(acceptingHandler);
        acceptingLibraryConfig.libraryConnectHandler(fakeConnectHandler);
        acceptingLibrary = connect(acceptingLibraryConfig);
        initiatingLibrary = newInitiatingLibrary(libraryAeronPort, initiatingHandler);
        testSystem = new TestSystem(acceptingLibrary, initiatingLibrary);
    }

    @Test
    public void messagesCanBeSentFromInitiatorToAcceptor()
    {
        final Reply<Session> reply = initiate(initiatingLibrary, port, INITIATOR_ID, ACCEPTOR_ID);

        assertEventuallyTrue("failed to receive auth proxy", () ->
        {
            testSystem.poll();
            return auth.hasAuthenticateBeenInvoked();
        }, DEFAULT_TIMEOUT_IN_MS);

        assertEquals(Reply.State.EXECUTING, reply.state());

        auth.accept();

        completeConnectSessions(reply);

        messagesCanBeExchanged();

        assertInitiatingSequenceIndexIs(0);

        auth.verifyNoBlockingCalls();
    }

    private class MyAuthenticationStrategy implements AuthenticationStrategy
    {
        volatile boolean blockingAuthenticateCalled;
        private volatile AuthenticationProxy authProxy;

        public void authenticateAsync(final LogonDecoder logon, final AuthenticationProxy authProxy)
        {
            this.authProxy = authProxy;
        }

        public boolean authenticate(final LogonDecoder logon)
        {
            blockingAuthenticateCalled = true;

            throw new UnsupportedOperationException();
        }

        public void verifyNoBlockingCalls()
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

        boolean hasAuthenticateBeenInvoked()
        {
            return authProxy != null;
        }
    }
}

