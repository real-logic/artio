package uk.co.real_logic.artio.system_tests;

import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.artio.Constants;
import uk.co.real_logic.artio.Reply;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.library.LibraryConfiguration;
import uk.co.real_logic.artio.session.Session;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toList;
import static org.junit.Assert.*;
import static uk.co.real_logic.artio.Constants.RESEND_REQUEST_MESSAGE_AS_STR;
import static uk.co.real_logic.artio.TestFixtures.launchMediaDriver;
import static uk.co.real_logic.artio.library.SessionConfiguration.AUTOMATIC_INITIAL_SEQUENCE_NUMBER;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.*;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.connect;
import static uk.co.real_logic.artio.validation.PersistenceLevel.INDEXED;

public class NonRestartingPersistentSequenceNumberGatewayToGatewayTest extends AbstractGatewayToGatewaySystemTest
{
    @Before
    public void setup()
    {
        deleteLogs();

        mediaDriver = launchMediaDriver();

        final EngineConfiguration acceptingConfig = acceptingConfig(port, ACCEPTOR_ID, INITIATOR_ID);
        acceptingConfig.sessionPersistenceStrategy(logon -> INDEXED);
        /*acceptingConfig.printStartupWarnings(PRINT_ERROR_MESSAGES);
        acceptingConfig.printErrorMessages(PRINT_ERROR_MESSAGES);*/
        acceptingEngine = FixEngine.launch(acceptingConfig);

        final EngineConfiguration initiatingConfig = initiatingConfig(libraryAeronPort);
        /*initiatingConfig.printStartupWarnings(PRINT_ERROR_MESSAGES);
        initiatingConfig.printErrorMessages(PRINT_ERROR_MESSAGES);*/
        initiatingEngine = FixEngine.launch(initiatingConfig);

        final LibraryConfiguration acceptingLibraryConfig = acceptingLibraryConfig(acceptingHandler);
        acceptingLibrary = connect(acceptingLibraryConfig);

        final LibraryConfiguration initiatingLibraryConfig =
            initiatingLibraryConfig(libraryAeronPort, initiatingHandler);
        initiatingLibrary = connect(initiatingLibraryConfig);

        testSystem = new TestSystem(acceptingLibrary, initiatingLibrary);

        final Reply<Session> reply = connectPersistentSessions(
            AUTOMATIC_INITIAL_SEQUENCE_NUMBER, AUTOMATIC_INITIAL_SEQUENCE_NUMBER, false);
        assertEquals("Repy failed: " + reply, Reply.State.COMPLETED, reply.state());
        initiatingSession = reply.resultIfPresent();
        acquireAcceptingSession();
    }

    // Reproduction of a bug whereby an invalid sequence number was stored after a logon
    @Test
    public void shouldMaintainCorrectStateWhenLoggingOutMessage()
    {
        messagesCanBeExchanged();

        disconnectSessions();

        assertSequenceFromInitToAcceptAt(3, 3);

        initiatingSession = null;
        acceptingSession = null;

        Reply<Session> reply = connectPersistentSessions(
            1, 4, false);

        assertDisconnectedAtLogon(reply);

        // Previously this passed because the sequence got reset.
        reply = connectPersistentSessions(
            2, 4, false);
        assertDisconnectedAtLogon(reply);

        reply = connectPersistentSessions(4, 5, false);
        initiatingSession = reply.resultIfPresent();

        assertEquals(Reply.State.COMPLETED, reply.state());
        assertNotNull(initiatingSession);

        final Optional<FixMessage> possibleResendRequest =
            initiatingOtfAcceptor.hasReceivedMessage(RESEND_REQUEST_MESSAGE_AS_STR).findFirst();

        assertFalse("Found illegal resend request: " + possibleResendRequest.toString(),
            possibleResendRequest.isPresent());
    }

    private void assertDisconnectedAtLogon(final Reply<Session> reply)
    {
        assertEquals(Reply.State.ERRORED, reply.state());
        assertThat(reply.error().getMessage(), Matchers.containsString("Disconnected before session active"));
    }
}
