package uk.co.real_logic.fix_gateway.system_tests;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.fix_gateway.DebugLogger;
import uk.co.real_logic.fix_gateway.LogTag;
import uk.co.real_logic.fix_gateway.Reply;
import uk.co.real_logic.fix_gateway.messages.SessionReplyStatus;

import static org.junit.Assert.assertEquals;
import static uk.co.real_logic.fix_gateway.CommonConfiguration.DEFAULT_REPLY_TIMEOUT_IN_MS;
import static uk.co.real_logic.fix_gateway.TestFixtures.cleanupMediaDriver;
import static uk.co.real_logic.fix_gateway.TestFixtures.launchMediaDriver;
import static uk.co.real_logic.fix_gateway.dictionary.generation.Exceptions.closeAll;
import static uk.co.real_logic.fix_gateway.system_tests.SystemTestUtil.*;

public class MultipleLibrarySystemTest extends AbstractGatewayToGatewaySystemTest
{
    @Before
    public void launch()
    {
        delete(ACCEPTOR_LOGS);

        mediaDriver = launchMediaDriver();

        launchAcceptingEngine();
        initiatingEngine = launchInitiatingEngine(libraryAeronPort);
        initiatingLibrary = newInitiatingLibrary(libraryAeronPort, initiatingHandler);
        testSystem = new TestSystem(acceptingLibrary, initiatingLibrary);

        connectSessions();
    }

    @Test
    public void shouldEnableLibraryConnectionsOneAfterAnother()
    {
        for (int i = 0; i < 20; i++)
        {
            DebugLogger.log(LogTag.FIX_TEST, "Iteration: " + i);

            acceptingLibrary = newAcceptingLibrary(acceptingHandler);

            while (!acceptingLibrary.isConnected())
            {
                pollLibraries();

                Thread.yield();
            }

            acquireAcceptingSession();

            final Reply<SessionReplyStatus> reply = acceptingLibrary.releaseToGateway(
                acceptingSession, DEFAULT_REPLY_TIMEOUT_IN_MS);
            awaitLibraryReply(acceptingLibrary, initiatingLibrary, reply);

            assertEquals(SessionReplyStatus.OK, reply.resultIfPresent());
            acceptingLibrary.close();
        }
    }

    @After
    public void shutdown()
    {
        closeAll(
            initiatingLibrary,
            acceptingLibrary,
            initiatingEngine,
            acceptingEngine,
            () -> cleanupMediaDriver(mediaDriver));
    }
}
