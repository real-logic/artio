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
import static uk.co.real_logic.artio.TestFixtures.launchMediaDriver;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.*;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.newInitiatingLibrary;

public class EnableLastMsgSeqNumProcessedTest extends AbstractGatewayToGatewaySystemTest
{
    private final FakeConnectHandler fakeConnectHandler = new FakeConnectHandler();

    @Before
    public void launch()
    {
        deleteLogs();

        mediaDriver = launchMediaDriver();

        final EngineConfiguration acceptingConfig = acceptingConfig(port, ACCEPTOR_ID, INITIATOR_ID);
        acceptingConfig.acceptedEnableLastMsgSeqNumProcessed(true);

        acceptingEngine = FixEngine.launch(acceptingConfig);
        initiatingEngine = launchInitiatingEngine(libraryAeronPort);

        final LibraryConfiguration acceptingLibraryConfig = acceptingLibraryConfig(acceptingHandler);
        acceptingLibraryConfig.libraryConnectHandler(fakeConnectHandler);
        acceptingLibrary = connect(acceptingLibraryConfig);
        initiatingLibrary = newInitiatingLibrary(libraryAeronPort, initiatingHandler);
        testSystem = new TestSystem(acceptingLibrary, initiatingLibrary);

        final SessionConfiguration config = SessionConfiguration.builder()
            .address("localhost", port)
            .credentials(USERNAME, PASSWORD)
            .senderCompId(INITIATOR_ID)
            .targetCompId(ACCEPTOR_ID)
            .enableLastMsgSeqNumProcessed(true)
            .build();

        final Reply<Session> reply = initiatingLibrary.initiate(config);
        completeConnectSessions(reply);
    }

    @Test
    public void lastMsgSeqNumProcessedUpdatedByEngine()
    {
        messagesCanBeExchanged();

        messagesCanBeExchanged();

        assertEquals(2, initiatingOtfAcceptor.lastReceivedMsgSeqNumProcessed());
    }

    @Test
    public void lastMsgSeqNumProcessedUpdatedByLibrary()
    {
        acquireAcceptingSession();

        messagesCanBeExchanged();

        messagesCanBeExchanged();

        assertEquals(2, initiatingOtfAcceptor.lastReceivedMsgSeqNumProcessed());
        assertEquals(2, acceptingOtfAcceptor.lastReceivedMsgSeqNumProcessed());
    }

}
