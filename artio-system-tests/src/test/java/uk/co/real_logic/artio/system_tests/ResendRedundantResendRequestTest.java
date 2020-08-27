package uk.co.real_logic.artio.system_tests;

import io.aeron.archive.ArchivingMediaDriver;
import org.junit.After;
import org.junit.Test;
import uk.co.real_logic.artio.decoder.LogonDecoder;
import uk.co.real_logic.artio.decoder.ResendRequestDecoder;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.validation.SessionPersistenceStrategy;

import java.io.IOException;
import java.util.concurrent.locks.LockSupport;

import static io.aeron.CommonContext.IPC_CHANNEL;
import static org.agrona.CloseHelper.close;
import static org.junit.Assert.assertEquals;
import static uk.co.real_logic.artio.TestFixtures.*;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.*;

public class ResendRedundantResendRequestTest
{
    private final int port = unusedPort();

    private ArchivingMediaDriver mediaDriver;
    private FixEngine engine;

    private void setup(final boolean resendRedundantResendRequest)
    {
        mediaDriver = launchMediaDriver();

        delete(ACCEPTOR_LOGS);
        final EngineConfiguration config = new EngineConfiguration()
            .bindTo("localhost", port)
            .libraryAeronChannel(IPC_CHANNEL)
            .monitoringFile(acceptorMonitoringFile("engineCounters"))
            .logFileDir(ACCEPTOR_LOGS)
            .sessionPersistenceStrategy(SessionPersistenceStrategy.alwaysPersistent())
            .acceptedSessionSendRedundantResendRequests(resendRedundantResendRequest);
        engine = FixEngine.launch(config);
    }

    @Test
    public void shouldNotSendRedundantResendRequestsByDefault() throws IOException
    {
        setup(false);

        try (FixConnection connection = FixConnection.initiate(port))
        {
            connection.msgSeqNum(3);
            connection.logon(false);
            final LogonDecoder logonReply = connection.readLogonReply();
            assertEquals(1, logonReply.header().msgSeqNum());

            // await resend request
            final ResendRequestDecoder resendRequest = connection.readMessage(new ResendRequestDecoder());
            assertEquals(1, resendRequest.beginSeqNo());
            assertEquals(0, resendRequest.endSeqNo());

            // reply with first message of resend
            connection.sendExecutionReport(1, true);

            // send new message
            connection.sendExecutionReport(4, false);

            // reply with last message of resend
            connection.sendExecutionReport(2, true);

            // check I've not received another resend request

            // exchange test request / heartbeat
            final String testReqId = "thisIsATest";
            connection.msgSeqNum(5);
            connection.sendTestRequest(testReqId);
            connection.readHeartbeat(testReqId);

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
