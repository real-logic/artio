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
import uk.co.real_logic.artio.binary_entrypoint.BinaryEntryPointIdentification;
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
    public void shouldRejectConnectionsAccordingToAuthenticationStrategy() throws IOException
    {
        fixPAuthenticationStrategy.reject();

        try (BinaryEntrypointClient client = new BinaryEntrypointClient(port, testSystem))
        {
            client.writeNegotiate();

            assertEquals(NegotiationRejectCode.CREDENTIALS, client.readNegotiateReject().negotiationRejectCode());

            client.assertDisconnected();

            final BinaryEntryPointIdentification id =
                (BinaryEntryPointIdentification)fixPAuthenticationStrategy.lastSessionId();
            assertNotNull(id);
            assertEquals(BinaryEntrypointClient.SESSION_ID, id.sessionID());
            assertEquals(1, id.sessionVerID());

            assertFalse(connectionExistsHandler.invoked());
            assertFalse(connectionAcquiredHandler.invoked());
        }
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
        final BinaryEntrypointClient client = new BinaryEntrypointClient(port, testSystem);
        client.writeNegotiate();

        testSystem.await("connection doesn't exist", connectionExistsHandler::invoked);
        assertEquals(BinaryEntrypointClient.SESSION_ID, connectionExistsHandler.lastSurrogateSessionId());
        final BinaryEntryPointIdentification id =
            (BinaryEntryPointIdentification)connectionExistsHandler.lastIdentification();
        assertEquals(BinaryEntrypointClient.SESSION_ID, id.sessionID());
        assertEquals(1, id.sessionVerID());
        final Reply<SessionReplyStatus> reply = connectionExistsHandler.lastReply();

        testSystem.awaitCompletedReply(reply);
        assertEquals(SessionReplyStatus.OK, reply.resultIfPresent());

        testSystem.await("connection not acquired", connectionAcquiredHandler::invoked);

        final NegotiateResponseDecoder response = client.readNegotiateResponse();
        assertEquals(BinaryEntrypointClient.SESSION_ID, response.sessionID());
        assertEquals(1, response.sessionVerID());
        assertEquals(BinaryEntrypointClient.FIRM_ID, response.enteringFirm());

        client.writeEstablish();
        final EstablishAckDecoder establishAck = client.readEstablishAck();
        assertEquals(BinaryEntrypointClient.SESSION_ID, establishAck.sessionID());
        assertEquals(1, establishAck.sessionVerID());
        assertEquals(1, establishAck.nextSeqNo());
        assertEquals(0, establishAck.lastIncomingSeqNo());

        connection = (BinaryEntrypointConnection)connectionAcquiredHandler.connection();
        assertEquals(BinaryEntrypointClient.SESSION_ID, connection.sessionId());
        assertEquals(1, connection.sessionVerId());
        assertEquals(FixPConnection.State.ESTABLISHED, connection.state());

        return client;
    }

    // DUPLICATE_ID on negotiate
    // SessionVeID must be incremented each time Negotiate message is sent to gateway

    // Unnegotiated: Establish request was not preceded by a Negotiation or
    // session was finalized, requiring renegotiation.

    // AlreadyEstablished: EstablishmentAck was already sent; Establish was
    // redundant.

    // KeepaliveInterval: value is out of accepted range.

    // SessionBlocked: user is not authorized
    // Credentials: failed because identity is not recognized, or the user is not
    // authorized to use this service.

    // should support FinishedSending/FinishedReceiving process

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
