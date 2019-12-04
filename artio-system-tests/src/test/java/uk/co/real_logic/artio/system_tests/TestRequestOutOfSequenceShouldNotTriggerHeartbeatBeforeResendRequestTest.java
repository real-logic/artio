package uk.co.real_logic.artio.system_tests;

import static io.aeron.CommonContext.IPC_CHANNEL;
import static org.agrona.CloseHelper.close;
import static org.junit.Assert.assertEquals;
import static uk.co.real_logic.artio.TestFixtures.cleanupMediaDriver;
import static uk.co.real_logic.artio.TestFixtures.launchMediaDriver;
import static uk.co.real_logic.artio.TestFixtures.unusedPort;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.ACCEPTOR_LOGS;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.acceptorMonitoringFile;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.delete;

import java.io.IOException;
import java.util.concurrent.locks.LockSupport;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import io.aeron.archive.ArchivingMediaDriver;
import uk.co.real_logic.artio.decoder.LogonDecoder;
import uk.co.real_logic.artio.decoder.ResendRequestDecoder;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.validation.SessionPersistenceStrategy;

@Ignore
public class TestRequestOutOfSequenceShouldNotTriggerHeartbeatBeforeResendRequestTest
{
    private int port = unusedPort();

    private ArchivingMediaDriver mediaDriver;
    private FixEngine engine;

    @Before
    public void setUp()
    {
        mediaDriver = launchMediaDriver();

        delete(ACCEPTOR_LOGS);
        final EngineConfiguration config = new EngineConfiguration()
            .bindTo("localhost", port)
            .libraryAeronChannel(IPC_CHANNEL)
            .monitoringFile(acceptorMonitoringFile("engineCounters"))
            .logFileDir(ACCEPTOR_LOGS)
            .sessionPersistenceStrategy(SessionPersistenceStrategy.alwaysPersistent());
        engine = FixEngine.launch(config);
    }

    @Test
    public void testRequestWithIncorrectSeqNumShouldNotTriggerHeartbeat() throws IOException
    {
        try (FixConnection connection = FixConnection.initiate(port))
        {
            connection.logon(true);
            final LogonDecoder logonReply = connection.readLogonReply();
            assertEquals(1, logonReply.header().msgSeqNum());

            connection.msgSeqNum(3);
            connection.testRequest("firstRequest");
            // TODO unexpected behaviour
            // uncommenting this line makes the test pass, but the first received message should be a resend request
            // connection.readMessage(new HeartbeatDecoder());
            // await resend request
            final ResendRequestDecoder resendRequest = connection.readMessage(new ResendRequestDecoder());
            assertEquals(2, resendRequest.beginSeqNo());
            assertEquals(0, resendRequest.endSeqNo());

            LockSupport.parkNanos(500);

            assertEquals(0, connection.pollData());

            connection.logoutAndAwaitReply();
        }
    }

    @After
    public void tearDown()
    {
        close(engine);
        cleanupMediaDriver(mediaDriver);
    }
}