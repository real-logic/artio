package uk.co.real_logic.artio.system_tests;

import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.artio.Reply;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.library.LibraryConfiguration;
import uk.co.real_logic.artio.library.SessionConfiguration;
import uk.co.real_logic.artio.session.Session;

import static org.junit.Assert.assertEquals;
import static uk.co.real_logic.artio.Constants.LOGOUT_MESSAGE_AS_STR;
import static uk.co.real_logic.artio.TestFixtures.launchMediaDriver;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.*;
import static uk.co.real_logic.artio.validation.SessionPersistenceStrategy.alwaysPersistent;

public class EnableLastMsgSeqNumProcessedTest extends AbstractGatewayToGatewaySystemTest
{
    @Before
    public void launch()
    {
        mediaDriver = launchMediaDriver();

        final EngineConfiguration acceptingConfig = acceptingConfig(port, ACCEPTOR_ID, INITIATOR_ID);
        acceptingConfig.deleteLogFileDirOnStart(true);
        acceptingConfig.acceptedEnableLastMsgSeqNumProcessed(true);
        acceptingConfig.sessionPersistenceStrategy(alwaysPersistent());

        acceptingEngine = FixEngine.launch(acceptingConfig);
        initiatingEngine = launchInitiatingEngine(libraryAeronPort);

        final LibraryConfiguration acceptingLibraryConfig = acceptingLibraryConfig(acceptingHandler);
        acceptingLibrary = connect(acceptingLibraryConfig);
        initiatingLibrary = newInitiatingLibrary(libraryAeronPort, initiatingHandler);
        testSystem = new TestSystem(acceptingLibrary, initiatingLibrary);

        final Reply<Session> reply = connectSession();
        completeConnectInitiatingSession(reply);
    }

    private Reply<Session> connectSession()
    {
        final SessionConfiguration config = SessionConfiguration.builder()
            .address("localhost", port)
            .credentials(USERNAME, PASSWORD)
            .senderCompId(INITIATOR_ID)
            .targetCompId(ACCEPTOR_ID)
            .sequenceNumbersPersistent(true)
            .initialSentSequenceNumber(1)
            .enableLastMsgSeqNumProcessed(true)
            .build();

        return initiatingLibrary.initiate(config);
    }

    @Test
    public void lastMsgSeqNumProcessedUpdatedByEngine()
    {
        messagesCanBeExchanged();

        messagesCanBeExchanged();

        assertLastInitiatorReceivedMsgSeqNumProcessed(2);
    }

    @Test
    public void lastMsgSeqNumProcessedUpdatedByLibrary()
    {
        acquireAcceptingSession();

        messagesCanBeExchanged();

        messagesCanBeExchanged();

        assertLastInitiatorReceivedMsgSeqNumProcessed(2);
        assertEquals(2, acceptingOtfAcceptor.lastReceivedMsgSeqNumProcessed());
    }

    @Test
    public void lastMsgSeqNumProcessedCorrectInLowSequenceNumberLogout()
    {
        messagesCanBeExchanged();
        messagesCanBeExchanged();

        assertLastInitiatorReceivedMsgSeqNumProcessed(2);

        acquireAcceptingSession();
        disconnectSessions();

        clearMessages();

        final Reply<Session> reply = connectSession();
        testSystem.awaitReply(reply);
        testSystem.awaitMessageOf(initiatingOtfAcceptor, LOGOUT_MESSAGE_AS_STR);
        assertLastInitiatorReceivedMsgSeqNumProcessed(4);
    }

    private void assertLastInitiatorReceivedMsgSeqNumProcessed(final int expected)
    {
        assertEquals(expected, initiatingOtfAcceptor.lastReceivedMsgSeqNumProcessed());
    }

}
