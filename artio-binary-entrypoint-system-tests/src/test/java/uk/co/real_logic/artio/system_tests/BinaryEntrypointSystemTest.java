/*
 * Copyright 2021 Monotonic Ltd.
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

import b3.entrypoint.fixp.sbe.*;
import io.aeron.archive.ArchivingMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import uk.co.real_logic.artio.Reply;
import uk.co.real_logic.artio.binary_entrypoint.BinaryEntryPointContext;
import uk.co.real_logic.artio.binary_entrypoint.BinaryEntrypointConnection;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.engine.ILink3RetransmitHandler;
import uk.co.real_logic.artio.engine.LowResourceEngineScheduler;
import uk.co.real_logic.artio.fixp.FixPConnection;
import uk.co.real_logic.artio.library.FixLibrary;
import uk.co.real_logic.artio.library.LibraryConfiguration;
import uk.co.real_logic.artio.messages.SessionReplyStatus;

import java.io.IOException;

import static io.aeron.CommonContext.IPC_CHANNEL;
import static java.util.Collections.singletonList;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static uk.co.real_logic.artio.TestFixtures.*;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.CLIENT_LOGS;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.TEST_REPLY_TIMEOUT_IN_MS;

public class BinaryEntrypointSystemTest
{
    private final int port = unusedPort();

    private ArchivingMediaDriver mediaDriver;
    private TestSystem testSystem;
    private FixEngine engine;
    private FixLibrary library;

    private final ErrorHandler errorHandler = mock(ErrorHandler.class);
    private final ILink3RetransmitHandler retransmitHandler = mock(ILink3RetransmitHandler.class);
    private final FakeFixPConnectionExistsHandler connectionExistsHandler = new FakeFixPConnectionExistsHandler();
    private final FakeBinaryEntrypointConnectionHandler connectionHandler = new FakeBinaryEntrypointConnectionHandler(
        notAppliedResponse ->
        {
        });
    private final FakeFixPConnectionAcquiredHandler connectionAcquiredHandler = new FakeFixPConnectionAcquiredHandler(
        connectionHandler);
    private final FakeFixPAuthenticationStrategy fixPAuthenticationStrategy = new FakeFixPAuthenticationStrategy();

    private BinaryEntrypointConnection connection;

    @Before
    public void setUp()
    {
        mediaDriver = launchMediaDriver();

        testSystem = new TestSystem();

        final EngineConfiguration engineConfig = new EngineConfiguration()
            .logFileDir(CLIENT_LOGS)
            .scheduler(new LowResourceEngineScheduler())
            .libraryAeronChannel(IPC_CHANNEL)
//            .errorHandlerFactory(errorBuffer -> errorHandler)
//            .monitoringAgentFactory(MonitoringAgentFactory.none())
            .fixPAuthenticationStrategy(fixPAuthenticationStrategy)
            .fixPRetransmitHandler(retransmitHandler)
            .acceptBinaryEntryPoint()
            .bindTo("localhost", port)
            .deleteLogFileDirOnStart(true);

        engine = FixEngine.launch(engineConfig);

        final LibraryConfiguration libraryConfig = new LibraryConfiguration()
            .libraryAeronChannels(singletonList(IPC_CHANNEL))
            .replyTimeoutInMs(TEST_REPLY_TIMEOUT_IN_MS)
            .fixPConnectionExistsHandler(connectionExistsHandler)
            .fixPConnectionAcquiredHandler(connectionAcquiredHandler);

//        libraryConfig
//            .errorHandlerFactory(errorBuffer -> errorHandler)
//            .monitoringAgentFactory(MonitoringAgentFactory.none());
        library = testSystem.connect(libraryConfig);
    }

    @Test
    public void shouldEstablishConnectionAtBeginningOfWeek() throws IOException
    {
        try (BinaryEntrypointClient client = establishNewConnection())
        {
            clientTerminatesSession(client);
        }
    }

    @Test
    public void shouldSupportAcceptorTerminateConnection() throws IOException
    {
        try (BinaryEntrypointClient client = establishNewConnection())
        {
            connection.terminate(TerminationCode.FINISHED);
            assertEquals(FixPConnection.State.UNBINDING, connection.state());

            client.readTerminate();
            client.writeTerminate();

            client.assertDisconnected();
            assertConnectionDisconnected();
        }
    }

    @Test
    public void shouldExchangeBusinessMessage() throws IOException
    {
        try (BinaryEntrypointClient client = establishNewConnection())
        {
            client.writeNewOrderSingle();

            assertReceivesOrder();

            client.readExecutionReportNew();
        }
    }

    @Test
    public void shouldRejectConnectionsIfAuthenticationFails() throws IOException
    {
        fixPAuthenticationStrategy.reject();

        connectionRejected(NegotiationRejectCode.CREDENTIALS);
    }

    @Test
    public void shouldRejectConnectionsWithDuplicateIds() throws IOException
    {
        try (BinaryEntrypointClient client = establishNewConnection())
        {
            connectionExistsHandler.reset();
            connectionAcquiredHandler.reset();

            connectionRejected(NegotiationRejectCode.DUPLICATE_ID);

            clientTerminatesSession(client);
        }

        // Check that we can Reconnect afterwards
        connectWithSessionVerId(2);
    }

    @Test
    public void shouldAcceptConnectionsWithIncrementingSessionVerId() throws IOException
    {
        successfulConnection();

        connectWithSessionVerId(2);
    }

    @Test
    public void shouldRejectConnectionsWithNonIncrementingSessionVerId() throws IOException
    {
        successfulConnection();

        try (BinaryEntrypointClient client = newClient())
        {
            client.writeNegotiate();

            client.readNegotiateReject(NegotiationRejectCode.DUPLICATE_ID);
            client.assertDisconnected();
        }
    }

    @Test
    public void shouldRejectConnectionsWithIncorrectFirstSessionVerId() throws IOException
    {
        try (BinaryEntrypointClient client = newClient())
        {
            client.sessionVerID(2);
            client.writeNegotiate();

            client.readNegotiateReject(NegotiationRejectCode.UNSPECIFIED);
            client.assertDisconnected();
        }
    }

    @Test
    public void shouldRejectUnNegotiatedEstablish() throws IOException
    {
        try (BinaryEntrypointClient client = newClient())
        {
            client.writeEstablish();
            client.readEstablishReject(EstablishRejectCode.UNNEGOTIATED);
            client.assertDisconnected();
        }
    }

    @Test
    public void shouldRejectUnNegotiatedEstablishWithHigherSessionVerId() throws IOException
    {
        successfulConnection();

        try (BinaryEntrypointClient client = newClient())
        {
            client.sessionVerID(2);
            client.writeEstablish();
            client.readEstablishReject(EstablishRejectCode.UNNEGOTIATED);
            client.assertDisconnected();
        }
    }

    @Test
    public void shouldAcceptReEstablishmentOfSession() throws IOException
    {
        successfulConnection();

        try (BinaryEntrypointClient client = newClient())
        {
            client.writeEstablish();

            libraryAcquiresConnection(client);

            client.readEstablishAck();

            assertConnectionMatches(client);
            clientTerminatesSession(client);
        }
    }

    @Test
    public void shouldRejectReEstablishmentOfSessionIfAuthenticationFails() throws IOException
    {
        successfulConnection();

        fixPAuthenticationStrategy.reject();

        try (BinaryEntrypointClient client = newClient())
        {
            client.writeEstablish();

            client.readEstablishReject(EstablishRejectCode.CREDENTIALS);
            client.assertDisconnected();

            assertAuthStrategyReject(client);
        }
    }

    @Test
    public void shouldRejectLaterEstablishMessage() throws IOException
    {
        try (BinaryEntrypointClient client = establishNewConnection())
        {
            client.writeEstablish();

            // This establish reject doesn't disconnect the already established connection like the others, it is
            // just ignored.
            client.readEstablishReject(EstablishRejectCode.ALREADY_ESTABLISHED);

            clientTerminatesSession(client);
        }
    }

    @Test
    public void shouldRejectEstablishMessageWithInvalidKeepAliveInterval() throws IOException
    {
        try (BinaryEntrypointClient client = newClient())
        {
            client.keepAliveIntervalInMs(Long.MAX_VALUE);

            client.writeNegotiate();
            libraryAcquiresConnection(client);
            client.readNegotiateResponse();

            client.writeEstablish();

            client.readEstablishReject(EstablishRejectCode.KEEPALIVE_INTERVAL);
            client.assertDisconnected();
        }
    }

    private void connectWithSessionVerId(final int sessionVerID) throws IOException
    {
        try (BinaryEntrypointClient client = newClient())
        {
            client.sessionVerID(sessionVerID);
            establishNewConnection(client);
            clientTerminatesSession(client);
        }
    }

    private void successfulConnection() throws IOException
    {
        try (BinaryEntrypointClient client = establishNewConnection())
        {
            clientTerminatesSession(client);
        }

        connectionExistsHandler.reset();
        connectionAcquiredHandler.reset();
    }

    private BinaryEntrypointClient newClient() throws IOException
    {
        return new BinaryEntrypointClient(port, testSystem);
    }

    private void connectionRejected(final NegotiationRejectCode negotiationRejectCode) throws IOException
    {
        try (BinaryEntrypointClient client = newClient())
        {
            client.writeNegotiate();

            client.readNegotiateReject(negotiationRejectCode);
            client.assertDisconnected();

            assertAuthStrategyReject(client);
        }
    }

    private void assertAuthStrategyReject(final BinaryEntrypointClient client)
    {
        final BinaryEntryPointContext id =
            (BinaryEntryPointContext)fixPAuthenticationStrategy.lastSessionId();
        assertNotNull(id);
        assertEquals(BinaryEntrypointClient.SESSION_ID, id.sessionID());
        assertEquals(client.sessionVerID(), id.sessionVerID());

        assertFalse(connectionExistsHandler.invoked());
        assertFalse(connectionAcquiredHandler.invoked());
    }

    private void assertReceivesOrder()
    {
        testSystem.await("does not receive new order single",
            () -> connectionHandler.templateIds().containsInt(NewOrderSingleDecoder.TEMPLATE_ID));
    }

    private void clientTerminatesSession(final BinaryEntrypointClient client)
    {
        client.writeTerminate();
        client.readTerminate();

        client.close();

        assertConnectionDisconnected();
    }

    private void assertConnectionDisconnected()
    {
        testSystem.await("onDisconnect not called", () -> connectionHandler.disconnectReason() != null);
        assertEquals(FixPConnection.State.UNBOUND, connection.state());
    }

    private BinaryEntrypointClient establishNewConnection() throws IOException
    {
        final BinaryEntrypointClient client = newClient();
        establishNewConnection(client);
        return client;
    }

    private void establishNewConnection(final BinaryEntrypointClient client)
    {
        client.writeNegotiate();

        libraryAcquiresConnection(client);

        client.readNegotiateResponse();

        client.writeEstablish();
        client.readEstablishAck();

        assertConnectionMatches(client);
    }

    private void assertConnectionMatches(final BinaryEntrypointClient client)
    {
        connection = (BinaryEntrypointConnection)connectionAcquiredHandler.connection();
        assertEquals(BinaryEntrypointClient.SESSION_ID, connection.sessionId());
        assertEquals(client.sessionVerID(), connection.sessionVerId());
        assertEquals(FixPConnection.State.ESTABLISHED, connection.state());
    }

    private void libraryAcquiresConnection(final BinaryEntrypointClient client)
    {
        testSystem.await("connection doesn't exist", connectionExistsHandler::invoked);
        assertNotNull(fixPAuthenticationStrategy.lastSessionId());
        assertEquals(BinaryEntrypointClient.SESSION_ID, connectionExistsHandler.lastSurrogateSessionId());
        final BinaryEntryPointContext id =
            (BinaryEntryPointContext)connectionExistsHandler.lastIdentification();
        assertEquals(BinaryEntrypointClient.SESSION_ID, id.sessionID());
        assertEquals(client.sessionVerID(), id.sessionVerID());
        final Reply<SessionReplyStatus> reply = connectionExistsHandler.lastReply();

        testSystem.awaitCompletedReply(reply);
        assertEquals(SessionReplyStatus.OK, reply.resultIfPresent());

        testSystem.await("connection not acquired", connectionAcquiredHandler::invoked);
    }

    // Credentials: failed because identity is not recognized, or the user is not
    // authorized to use this service.

    // should support FinishedSending/FinishedReceiving process
    // Unnegotiated: Establish request after session was finalized, requiring renegotiation.

    // shouldCorrectlyAbortBusinessMessage()
    // shouldResendNegotiateAndEstablishOnTimeout() - check protocol spec
    // shouldDisconnectIfNoNegotiate()
    // shouldDisconnectIfNegotiateResponseNotRespondedTo()
    // shouldAllowReconnectAfterNegotiateDisconnect()
    // shouldNegotiationRejectForAuthenticationFailure()
    // shouldEstablishRejectForInvalidSessionId()
    // shouldSupportReestablishingConnections() - continue sequence number
    // shouldSupportReestablishingConnectionsAfterNegotiateReject()
    // shouldSupportReestablishingConnectionsAfterNegotiateTimeout()
    // shouldSupportReestablishingConnectionsAfterRestart()
    // shouldSupportResetState()
    // shouldSupportSequenceMessageHeartbeating()

    // timeout disconnect
    // heartbeat / timeout

    @After
    public void close()
    {
        closeArtio();
        cleanupMediaDriver(mediaDriver);

        verifyNoInteractions(errorHandler);

        Mockito.framework().clearInlineMocks();
    }

    private void closeArtio()
    {
        testSystem.awaitBlocking(() -> CloseHelper.close(engine));
        CloseHelper.close(library);
    }
}
