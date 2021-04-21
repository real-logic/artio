package uk.co.real_logic.artio.system_tests;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.artio.DebugLogger;
import uk.co.real_logic.artio.LogTag;
import uk.co.real_logic.artio.Reply;
import uk.co.real_logic.artio.messages.SessionReplyStatus;

import static org.junit.Assert.assertEquals;
import static uk.co.real_logic.artio.CommonConfiguration.DEFAULT_REPLY_TIMEOUT_IN_MS;
import static uk.co.real_logic.artio.TestFixtures.cleanupMediaDriver;
import static uk.co.real_logic.artio.TestFixtures.launchMediaDriver;
import static uk.co.real_logic.artio.dictionary.generation.Exceptions.closeAll;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.*;

public class MultipleLibrarySystemTest extends AbstractGatewayToGatewaySystemTest
{
    @Before
    public void launch()
    {
        delete(ACCEPTOR_LOGS);

        mediaDriver = launchMediaDriver();

        launchAcceptingEngine();
        initiatingEngine = launchInitiatingEngine(libraryAeronPort, nanoClock);
        initiatingLibrary = newInitiatingLibrary(libraryAeronPort, initiatingHandler, nanoClock);
        testSystem = new TestSystem(initiatingLibrary);

        connectSessions();
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldEnableLibraryConnectionsOneAfterAnother()
    {
        for (int i = 0; i < 10; i++)
        {
            DebugLogger.log(LogTag.FIX_TEST, "Iteration: " + i);

            acceptingLibrary = testSystem.add(newAcceptingLibrary(acceptingHandler, nanoClock));

            while (!acceptingLibrary.isConnected())
            {
                testSystem.poll();

                Thread.yield();
            }

            acquireAcceptingSession();

            final Reply<SessionReplyStatus> reply = testSystem.awaitReply(acceptingLibrary.releaseToGateway(
                acceptingSession, DEFAULT_REPLY_TIMEOUT_IN_MS));

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
