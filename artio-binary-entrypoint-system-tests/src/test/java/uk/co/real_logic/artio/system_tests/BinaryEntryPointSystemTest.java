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
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import uk.co.real_logic.artio.MonitoringAgentFactory;
import uk.co.real_logic.artio.Reply;
import uk.co.real_logic.artio.binary_entrypoint.BinaryEntryPointContext;
import uk.co.real_logic.artio.binary_entrypoint.BinaryEntrypointConnection;
import uk.co.real_logic.artio.engine.*;
import uk.co.real_logic.artio.engine.framer.LibraryInfo;
import uk.co.real_logic.artio.fixp.FixPConnection;
import uk.co.real_logic.artio.ilink.ILink3Connection;
import uk.co.real_logic.artio.ilink.ILink3ConnectionConfiguration;
import uk.co.real_logic.artio.library.FixLibrary;
import uk.co.real_logic.artio.library.LibraryConfiguration;
import uk.co.real_logic.artio.messages.SessionReplyStatus;
import uk.co.real_logic.artio.session.Session;

import java.io.IOException;
import java.util.List;
import java.util.function.Consumer;

import static io.aeron.CommonContext.IPC_CHANNEL;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static uk.co.real_logic.artio.TestFixtures.*;
import static uk.co.real_logic.artio.engine.EngineConfiguration.DEFAULT_NO_LOGON_DISCONNECT_TIMEOUT_IN_MS;
import static uk.co.real_logic.artio.engine.FixEngine.ENGINE_LIBRARY_ID;
import static uk.co.real_logic.artio.library.LibraryConfiguration.NO_FIXP_MAX_RETRANSMISSION_RANGE;
import static uk.co.real_logic.artio.system_tests.BinaryEntrypointClient.*;
import static uk.co.real_logic.artio.system_tests.FakeBinaryEntrypointConnectionHandler.sendExecutionReportNew;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.*;

public class BinaryEntryPointSystemTest
{
    private static final int AWAIT_TIMEOUT_IN_MS = 10_000;
    private static final int TIMEOUT_EPSILON_IN_MS = 10;
    private static final int TEST_NO_LOGON_DISCONNECT_TIMEOUT_IN_MS = 200;

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

    private boolean printErrors = true;

    @Before
    public void setUp()
    {
        mediaDriver = launchMediaDriver();

        testSystem = new TestSystem().awaitTimeoutInMs(AWAIT_TIMEOUT_IN_MS);
    }

    private void setupArtio(final boolean deleteLogFileDirOnStart)
    {
        setupArtio(
            deleteLogFileDirOnStart,
            DEFAULT_NO_LOGON_DISCONNECT_TIMEOUT_IN_MS,
            NO_FIXP_MAX_RETRANSMISSION_RANGE);
    }

    private void setupArtio(
        final boolean deleteLogFileDirOnStart,
        final int shortLogonTimeoutInMs,
        final int fixPAcceptedSessionMaxRetransmissionRange)
    {
        final EngineConfiguration engineConfig = new EngineConfiguration()
            .logFileDir(ACCEPTOR_LOGS)
            .scheduler(new LowResourceEngineScheduler())
            .libraryAeronChannel(IPC_CHANNEL)
            .replyTimeoutInMs(TEST_REPLY_TIMEOUT_IN_MS)
            .noLogonDisconnectTimeoutInMs(shortLogonTimeoutInMs)
            .fixPAuthenticationStrategy(fixPAuthenticationStrategy)
            .fixPRetransmitHandler(retransmitHandler)
            .acceptBinaryEntryPoint()
            .bindTo("localhost", port)
            .deleteLogFileDirOnStart(deleteLogFileDirOnStart);

        if (!printErrors)
        {
            engineConfig
                .errorHandlerFactory(errorBuffer -> errorHandler)
                .monitoringAgentFactory(MonitoringAgentFactory.none());
        }

        engine = FixEngine.launch(engineConfig);

        final LibraryConfiguration libraryConfig = new LibraryConfiguration()
            .libraryAeronChannels(singletonList(IPC_CHANNEL))
            .replyTimeoutInMs(TEST_REPLY_TIMEOUT_IN_MS)
            .fixPConnectionExistsHandler(connectionExistsHandler)
            .fixPConnectionAcquiredHandler(connectionAcquiredHandler);
        libraryConfig
            .noEstablishFixPTimeoutInMs(shortLogonTimeoutInMs)
            .fixPAcceptedSessionMaxRetransmissionRange(fixPAcceptedSessionMaxRetransmissionRange);

        if (!printErrors)
        {
            libraryConfig
                .errorHandlerFactory(errorBuffer -> errorHandler)
                .monitoringAgentFactory(MonitoringAgentFactory.none());
        }

        library = testSystem.connect(libraryConfig);
    }

    @Test
    public void shouldEstablishConnectionAtBeginningOfWeek() throws IOException
    {
        setupArtio(true);

        try (BinaryEntrypointClient client = establishNewConnection())
        {
            clientTerminatesSession(client);
        }
    }

    @Test
    public void shouldSupportAcceptorTerminateConnection() throws IOException
    {
        setupArtio(true);

        try (BinaryEntrypointClient client = establishNewConnection())
        {
            connection.terminate(TerminationCode.FINISHED);
            assertEquals(FixPConnection.State.UNBINDING, connection.state());

            acceptorTerminatesSession(client);
        }
    }

    @Test
    public void shouldExchangeBusinessMessage() throws IOException
    {
        setupArtio(true);

        connectAndExchangeBusinessMessage();
    }

    private void connectAndExchangeBusinessMessage() throws IOException
    {
        try (BinaryEntrypointClient client = establishNewConnection())
        {
            assertNextSequenceNumbers(1, 1);

            exchangeOrderAndReportNew(client);

            assertNextSequenceNumbers(2, 2);
        }
    }

    @Test
    public void shouldCorrectlyAbortBusinessMessage() throws IOException
    {
        setupArtio(true);

        try (BinaryEntrypointClient client = establishNewConnection())
        {
            connectionHandler.abortReport(true);
            client.writeNewOrderSingle(CL_ORD_ID);
            assertReceivesOrder();

            assertNextSequenceNumbers(2, 1);

            connectionHandler.reset();

            connectionHandler.abortReport(false);
            final int okClOrdId = CL_ORD_ID + 1;
            client.writeNewOrderSingle(okClOrdId);
            assertReceivesOrder();

            client.readExecutionReportNew(okClOrdId);

            assertNextSequenceNumbers(3, 2);
        }
    }

    @Test
    public void shouldRejectConnectionsIfAuthenticationFails() throws IOException
    {
        setupArtio(true);

        fixPAuthenticationStrategy.reject();

        connectionRejected(NegotiationRejectCode.CREDENTIALS);
    }

    @Test
    public void shouldRejectConnectionsWithDuplicateIds() throws IOException
    {
        setupArtio(true);

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
    public void shouldAcceptReNegotiationsWithIncrementingSessionVerId() throws IOException
    {
        successfulConnection();

        connectWithSessionVerId(2);

        // Also accept renegotiations with a gap
        connectWithSessionVerId(4);

        restartArtio();

        connectWithSessionVerId(5);
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
    public void shouldAcceptConnectionsWithArbitraryFirstSessionVerId() throws IOException
    {
        setupArtio(true);

        try (BinaryEntrypointClient client = newClient())
        {
            client.sessionVerID(2);
            client.writeNegotiate();

            client.readNegotiateResponse();
            client.assertDisconnected();
        }
    }

    @Test
    public void shouldRejectUnNegotiatedEstablish() throws IOException
    {
        setupArtio(true);

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
    public void shouldDisconnectIfNegotiateTimeout() throws IOException
    {
        setupArtio(true, TEST_NO_LOGON_DISCONNECT_TIMEOUT_IN_MS, 1);

        final long timeInMs = System.currentTimeMillis();
        try (BinaryEntrypointClient client = newClient())
        {
            client.assertDisconnected();
            final long durationInMs = System.currentTimeMillis() - timeInMs;
            final long acceptableLowerBoundInMs = TEST_NO_LOGON_DISCONNECT_TIMEOUT_IN_MS - TIMEOUT_EPSILON_IN_MS;
            assertThat(durationInMs, Matchers.greaterThanOrEqualTo(acceptableLowerBoundInMs));
        }

        // Test that we can still establish the connection after this
        resetHandlers();

        establishSuccessNewConnection();
    }

    @Test
    public void shouldDisconnectIfEstablishNotSent() throws IOException
    {
        setupArtio(
            true,
            TEST_NO_LOGON_DISCONNECT_TIMEOUT_IN_MS,
            NO_FIXP_MAX_RETRANSMISSION_RANGE);

        try (BinaryEntrypointClient client = newClient())
        {
            final long timeInMs = System.currentTimeMillis();
            client.writeNegotiate();
            client.readNegotiateResponse();

            client.assertDisconnected();
            final long durationInMs = System.currentTimeMillis() - timeInMs;
            assertThat(durationInMs, Matchers.greaterThanOrEqualTo((long)TEST_NO_LOGON_DISCONNECT_TIMEOUT_IN_MS));
        }
    }

    @Test
    public void shouldAcceptReEstablishmentOfSession() throws IOException
    {
        successfulConnection();

        reEstablishConnection(1, 1);

        reEstablishConnection(2, 2);

        restartArtio();
        reEstablishConnection(3, 3);
    }

    @Test
    public void shouldRejectReEstablishmentOfSessionIfAuthenticationFails() throws IOException
    {
        successfulConnection();

        fixPAuthenticationStrategy.reject();

        final long sessionVerID = rejectedReestablish(EstablishRejectCode.CREDENTIALS);
        assertAuthStrategyReject(sessionVerID);
    }

    @Test
    public void shouldRejectLaterEstablishMessage() throws IOException
    {
        setupArtio(true);

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
        setupArtio(true);

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

    // -------------------------------
    // BEGIN SEQUENCE NUMBER GAP TESTS
    // -------------------------------

    @Test
    public void shouldAcceptRetransmitAfterASequenceMessageBasedGap() throws IOException
    {
        setupArtio(true);

        try (BinaryEntrypointClient client = establishNewConnection())
        {
            exchangeOrderAndReportNew(client);

            connectionHandler.replyToOrder(false);

            assertNextSequenceNumbers(2, 2);

            client.writeSequence(4);

            client.readNotApplied(2, 2);

            retransmitAfterGap(client);
        }

        assertSequenceUpdatePersistedInIndex();
    }

    @Test
    public void shouldAcceptRetransmitAfterAnEstablishMessageBasedGap() throws IOException
    {
        successfulConnection();

        connectionHandler.replyToOrder(false);

        try (BinaryEntrypointClient client = newClient())
        {
            client.writeEstablish(4);
            libraryAcquiresConnection(client);
            assertConnectionMatches(client);
            client.readEstablishAck(4, 1);

            retransmitAfterGap(client);
        }

        assertSequenceUpdatePersistedInIndex();
    }

    @Test
    public void shouldAcceptValidRetransmitRequest() throws IOException
    {
        setupArtio(true);

        try (BinaryEntrypointClient client = establishNewConnection())
        {
            exchange4OrdersAndReports(client);

            assertMessagesRetransmitted(client);

            clientTerminatesSession(client);

            assertNextSequenceNumbers(5, 5);
        }

        assertMessagesFromBeforeReEstablishRetransmitted();

        // test repeatability
        assertMessagesFromBeforeReEstablishRetransmitted();

        restartArtio();

        assertMessagesFromBeforeReEstablishRetransmitted();
    }

    private void assertMessagesFromBeforeReEstablishRetransmitted() throws IOException
    {
        withReEstablishedConnection(4, client ->
        {
            assertNextSequenceNumbers(5, 5);

            assertMessagesRetransmitted(client);

            clientTerminatesSession(client);
        });
    }

    private void assertMessagesRetransmitted(final BinaryEntrypointClient client)
    {
        client.writeRetransmitRequest(2, 2);
        client.readRetransmission(2, 2);
        client.readExecutionReportNew(2);
        client.readExecutionReportNew(3);
    }

    private void exchange4OrdersAndReports(final BinaryEntrypointClient client)
    {
        exchangeOrderAndReportNew(client, 1);
        exchangeOrderAndReportNew(client, 2);
        exchangeOrderAndReportNew(client, 3);
        exchangeOrderAndReportNew(client, 4);
        assertNextSequenceNumbers(5, 5);
    }

    @Test
    public void shouldRejectRetransmitRequestWithHighEndNo() throws IOException
    {
        setupArtio(true);

        try (BinaryEntrypointClient client = establishNewConnection())
        {
            exchangeOrderAndReportNew(client, 1);
            exchangeOrderAndReportNew(client, 2);
            assertNextSequenceNumbers(3, 3);

            client.writeRetransmitRequest(2, 2);
            client.readRetransmitReject(RetransmitRejectCode.OUT_OF_RANGE);

            clientTerminatesSession(client);
        }
    }

    @Test
    public void shouldRejectRetransmitRequestWithHighStartNo() throws IOException
    {
        setupArtio(true);

        try (BinaryEntrypointClient client = establishNewConnection())
        {
            assertNextSequenceNumbers(1, 1);

            client.writeRetransmitRequest(2, 2);
            client.readRetransmitReject(RetransmitRejectCode.OUT_OF_RANGE);

            clientTerminatesSession(client);
        }
    }

    @Test
    public void shouldRejectRetransmitRequestWithWrongSessionId() throws IOException
    {
        setupArtio(true);

        try (BinaryEntrypointClient client = establishNewConnection())
        {
            exchangeOrderAndReportNew(client, 1);
            client.writeRetransmitRequest(1000, 1, 1);
            client.readRetransmitReject(RetransmitRejectCode.INVALID_SESSION);

            clientTerminatesSession(client);
        }
    }

    @Test
    public void shouldRejectRetransmitRequestLimitExceeded() throws IOException
    {
        setupArtio(
            true,
            DEFAULT_NO_LOGON_DISCONNECT_TIMEOUT_IN_MS,
            1);

        try (BinaryEntrypointClient client = establishNewConnection())
        {
            exchangeOrderAndReportNew(client, 1);
            exchangeOrderAndReportNew(client, 2);
            client.writeRetransmitRequest(1, 2);
            client.readRetransmitReject(RetransmitRejectCode.REQUEST_LIMIT_EXCEEDED);

            clientTerminatesSession(client);
        }
    }

    // -------------------------------
    // END SEQUENCE NUMBER GAP TESTS
    // -------------------------------

    // ----------------------------------
    // BEGIN FINALIZATION TESTS
    // ----------------------------------

    // FIXP Spec 7.4.1
    @Test
    public void shouldRespondToFinishedSendingWithFinishedReceiving() throws IOException
    {
        setupArtio(true);

        try (BinaryEntrypointClient client = establishNewConnection())
        {
            clientInitiatedFinishSending(client);

            assertTrue("onFinishedSending() not invoked", connectionHandler.hasFinishedSending());

            // can still send a message before we send finished sending.
            final int clOrderID = 2;
            sendExecutionReportNew(connection, clOrderID, SECURITY_ID, false);
            client.readExecutionReportNew(clOrderID);

            assertNextSequenceNumbers(2, 3);

            connection.finishSending();
            assertCannotSendMessage();

            client.readFinishedSending(2);
            client.writeFinishedReceiving();

            clientTerminatesSession(client);
        }

        // Cannot re-establish finished sequence
        rejectedReestablish(EstablishRejectCode.UNNEGOTIATED);
        restartArtio();
        rejectedReestablish(EstablishRejectCode.UNNEGOTIATED);

        // Can reconnect with higher session ver id
        connectWithSessionVerId(2);
    }

    private void clientInitiatedFinishSending(final BinaryEntrypointClient client)
    {
        exchangeOrderAndReportNew(client);

        assertNextSequenceNumbers(2, 2);

        client.writeFinishedSending(1);
        client.readFinishedReceiving();
    }

    @Test
    public void shouldCompleteFinishedSendingProcess() throws IOException
    {
        setupArtio(true);

        try (BinaryEntrypointClient client = establishNewConnection())
        {
            exchangeOrderAndReportNew(client);

            assertNextSequenceNumbers(2, 2);

            acceptorInitiatedFinishSending(client);

            // reply
            client.writeFinishedSending(1);
            client.readFinishedReceiving();

            assertCannotSendMessage();

            acceptorTerminatesSession(client);
        }
    }

    private void acceptorInitiatedFinishSending(final BinaryEntrypointClient client)
    {
        connection.finishSending();

        assertCannotSendMessage();

        client.readFinishedSending(1);
        client.writeFinishedReceiving();
    }

    // FIXP Spec 7.4.2
    @Test
    public void shouldUseFinishedSendingAsAHeartbeatKeepAliveInTheAbsenceOfResponse() throws IOException
    {
        setupArtio(true);

        withLowKeepAliveClient(client ->
        {
            clientInitiatedFinishSending(client);

            connection.finishSending();

            client.readFinishedSending(1);
            client.readFinishedSending(1);

            client.writeFinishedReceiving();

            acceptorTerminatesSession(client);
        });
    }

    // FIXP Spec 7.4.3
    @Test
    public void shouldProcessRetransmitRequestsInResponseToFinishSending() throws IOException
    {
        setupArtio(true);

        try (BinaryEntrypointClient client = establishNewConnection())
        {
            clientInitiatedFinishSending(client);

            processRetransmitRequestsDuringFinishSending(client);

            acceptorTerminatesSession(client);
        }
    }

    @Test
    public void shouldProcessRetransmitRequestsInResponseToAcceptorFinishSending() throws IOException
    {
        setupArtio(true);

        try (BinaryEntrypointClient client = establishNewConnection())
        {
            exchangeOrderAndReportNew(client);

            processRetransmitRequestsDuringFinishSending(client);

            client.writeFinishedSending(1);
            client.readFinishedReceiving();

            acceptorTerminatesSession(client);
        }
    }

    // FIXP Spec 7.4.4
    @Test
    public void shouldTerminateInResponseToReceivingTerminate() throws IOException
    {
        setupArtio(true);

        try (BinaryEntrypointClient client = establishNewConnection())
        {
            exchangeOrderAndReportNew(client);

            acceptorInitiatedFinishSending(client);

            client.writeTerminate();
            client.readTerminate();
        }
    }

    // FIXP Spec 7.4.5
    @Test
    public void shouldTerminateInResponseToReceivingMessageAfterFinishedSending() throws IOException
    {
        setupArtio(true);

        try (BinaryEntrypointClient client = establishNewConnection())
        {
            terminatedBySendingMessageAfterFinishedSending(client, () ->
                client.writeNewOrderSingle(2));
        }

        reNegotiateWithVerId(3, client ->
            terminatedBySendingMessageAfterFinishedSending(client, () ->
            client.writeSequence(1)));

        reNegotiateWithVerId(4, client ->
            terminatedBySendingMessageAfterFinishedSending(client, client::writeNegotiate));
    }

    private void terminatedBySendingMessageAfterFinishedSending(
        final BinaryEntrypointClient client,
        final Runnable sendMessage)
    {
        exchangeOrderAndReportNew(client);

        client.writeFinishedSending(1);
        client.readFinishedReceiving();

        sendMessage.run();

        client.readTerminate(TerminationCode.UNSPECIFIED);
    }

    private void processRetransmitRequestsDuringFinishSending(final BinaryEntrypointClient client)
    {
        connection.finishSending();

        client.readFinishedSending(1);

        client.writeRetransmitRequest(1, 1);
        client.readRetransmission(1, 1);
        client.readExecutionReportNew();
        client.writeFinishedReceiving();
    }

    // ----------------------------------
    // END FINALIZATION TESTS
    // ----------------------------------

    @Test
    public void shouldTerminateSessionWhenSequenceNumberTooLowCanReestablish() throws IOException
    {
        setupArtio(true);

        shouldTerminateSessionWhenSequenceNumberTooLow();

        resetHandlers();

        reEstablishConnection(1, 1);
    }

    @Test
    public void shouldTerminateSessionWhenSequenceNumberTooLowCanRenegotiate() throws IOException
    {
        setupArtio(true);

        shouldTerminateSessionWhenSequenceNumberTooLow();

        connectWithSessionVerId(2);
    }

    @Test
    public void shouldTerminateSessionWhenSequenceNumberTooLowCanReestablishAfterRestart() throws IOException
    {
        setupArtio(true);

        shouldTerminateSessionWhenSequenceNumberTooLow();

        restartArtio();
        resetHandlers();

        reEstablishConnection(1, 1);
    }

    @Test
    public void shouldTerminateSessionWhenSequenceNumberTooLowCanRenegotiateAfterRestart() throws IOException
    {
        setupArtio(true);

        shouldTerminateSessionWhenSequenceNumberTooLow();

        restartArtio();

        connectWithSessionVerId(2);
    }

    @Test
    public void shouldSendSequenceMessageAfterTimeElapsed() throws IOException
    {
        setupArtio(true);

        withLowKeepAliveClient(client ->
        {
            client.readSequence(1);
            client.skipSequence();
            exchangeOrderAndReportNew(client);
            client.dontSkip();

            sleep(100);
            client.writeSequence(2);

            client.readSequence(2);
        });
    }

    @Test
    public void shouldUseSequenceMessagesAsLivenessIndicator() throws IOException
    {
        setupArtio(true);

        withLowKeepAliveClient(client ->
        {
            sleep(200);

            client.skipSequence();
            exchangeOrderAndReportNew(client);
            client.dontSkip();

            // Use a sequence message to test that they keep the connection alive.
            sleep(400);
            client.writeSequence(2);

            sleep(200);
            client.readSequence(2);
            client.skipSequence();

            // Eventually get disconnected when there's no sequence message for longer than the timeout
            client.readTerminate(TerminationCode.UNSPECIFIED);
            client.assertDisconnected();
        });
    }

    @Test
    public void sessionsListedInAdminApi() throws IOException
    {
        setupArtio(true);

        try (BinaryEntrypointClient client = establishNewConnection())
        {
            final List<LibraryInfo> libraries = libraries(engine);
            assertThat(libraries, hasSize(2));

            final LibraryInfo library = libraries.get(0);
            assertEquals(this.library.libraryId(), library.libraryId());

            final List<FixPConnectedSessionInfo> connections = library.fixPConnections();
            assertThat(connections, hasSize(1));

            final FixPConnectedSessionInfo connectedSessionInfo = connections.get(0);
            assertThat(connectedSessionInfo.address(), Matchers.anyOf(
                containsString("localhost"), containsString("127.0.0.1")));

            final int port = client.remoteAddress().getPort();
            assertThat(connectedSessionInfo.address(), containsString(String.valueOf(port)));
            assertEquals(connection.connectionId(), connectedSessionInfo.connectionId());

            assertEquals(connectedSessionInfo.key(), connection.key());

            final LibraryInfo gatewayLibraryInfo = libraries.get(1);
            assertEquals(ENGINE_LIBRARY_ID, gatewayLibraryInfo.libraryId());
            assertThat(gatewayLibraryInfo.sessions(), hasSize(0));

            assertAllSessionsOnlyContains(engine, connection);

            clientTerminatesSession(client);
        }
    }

    @Test
    public void shouldBlockInitiationOfOtherFixPProtocols()
    {
        printErrors = false;

        setupArtio(true);

        final Reply<ILink3Connection> reply = library.initiate(ILink3ConnectionConfiguration.builder()
            .host("127.0.0.1")
            .port(123)
            .sessionId("ABC")
            .firmId("DEF")
            .userKey("blahblah")
            .accessKeyId("access")
            .handler(new FakeILink3ConnectionHandler())
            .build());

        testSystem.awaitErroredReply(reply, allOf(
            containsString("INVALID_CONFIGURATION"), containsString("BINARY_ENTRYPOINT")));
    }

    @Test
    public void shouldBlockInitiationOfOtherFixProtocols()
    {
        printErrors = false;

        setupArtio(true);

        final Reply<Session> reply = initiate(library, port, "ABC", "DEF");

        testSystem.awaitErroredReply(reply, allOf(
            containsString("INVALID_CONFIGURATION"), containsString("FIXP")));
    }

    @Test
    public void shouldSupportResetState() throws IOException
    {
        final Backup backup = new Backup();

        try
        {
            shouldExchangeBusinessMessage();

            closeArtio();

            backup.resetState(engine);
            backup.assertStateReset(mediaDriver, is(0));
            backup.assertRecordingsTruncated();
            // Idempotence
            backup.resetState(engine);

            // all old sessions are removed and we can renegotiate
            setupArtio(false);
            final List<FixPSessionInfo> sessionInfos = engine.allFixPSessions();
            assertThat(sessionInfos, hasSize(0));
            connectAndExchangeBusinessMessage();
        }
        finally
        {
            backup.cleanup();
        }
    }

    @Test
    public void shouldResetSequenceNumbersOfDisconnectedSessions() throws IOException
    {
        shouldExchangeBusinessMessage();

        testSystem.awaitCompletedReply(engine.resetSequenceNumber(connection.sessionId()));

        rejectedReestablish(EstablishRejectCode.UNNEGOTIATED);
    }

    private void assertAllSessionsOnlyContains(final FixEngine engine, final BinaryEntrypointConnection connection)
    {
        final List<FixPSessionInfo> allSessions = engine.allFixPSessions();
        assertThat(allSessions, hasSize(1));

        final FixPSessionInfo sessionInfo = allSessions.get(0);
        assertEquals(sessionInfo.key(), connection.key());
    }

    private void withLowKeepAliveClient(final Consumer<BinaryEntrypointClient> handler) throws IOException
    {
        try (BinaryEntrypointClient client = newClient())
        {
            client.keepAliveIntervalInMs(500);
            establishNewConnection(client);
            handler.accept(client);
        }
    }

    private void sleep(final int timeInMs)
    {
        testSystem.awaitBlocking(() ->
        {
            try
            {
                Thread.sleep(timeInMs);
            }
            catch (final InterruptedException e)
            {
                e.printStackTrace();
            }
        });
    }

    private void shouldTerminateSessionWhenSequenceNumberTooLow() throws IOException
    {
        withLowKeepAliveClient(client ->
        {
            client.skipSequence();
            exchangeOrderAndReportNew(client);
            connectionHandler.replyToOrder(false);
            assertNextSequenceNumbers(2, 2);

            client.writeSequence(1);

            client.readTerminate();
            client.assertDisconnected();
        });

        connectionHandler.replyToOrder(true);
    }

    private void assertSequenceUpdatePersistedInIndex() throws IOException
    {
        resetHandlers();
        reEstablishConnection(4, 1);

        restartArtio();

        reEstablishConnection(5, 2);
    }

    private void retransmitAfterGap(final BinaryEntrypointClient client)
    {
        assertNextSequenceNumbers(4, 2);

        connectionHandler.reset();
        client.writeNewOrderSingle();
        assertReceivesOrder();

        assertNextSequenceNumbers(5, 2);
    }

    private void assertCannotSendMessage()
    {
        assertThrows(IllegalStateException.class, () -> connection.tryClaim(new ExecutionReport_NewEncoder()));
    }

    private long rejectedReestablish(final EstablishRejectCode rejectCode) throws IOException
    {
        try (BinaryEntrypointClient client = newClient())
        {
            client.writeEstablish();

            client.readEstablishReject(rejectCode);
            client.assertDisconnected();

            return client.sessionVerID();
        }
    }

    private void restartArtio()
    {
        closeArtio();
        setupArtio(false);
    }

    private void reEstablishConnection(final int alreadyRecvMsgCount, final int alreadySentMsgCount) throws IOException
    {
        withReEstablishedConnection(alreadyRecvMsgCount, client ->
        {
            assertNextSequenceNumbers(alreadyRecvMsgCount + 1, alreadySentMsgCount + 1);

            exchangeOrderAndReportNew(client);

            assertNextSequenceNumbers(alreadyRecvMsgCount + 2, alreadySentMsgCount + 2);

            clientTerminatesSession(client);
        });
    }

    private void withReEstablishedConnection(
        final int alreadyRecvMsgCount,
        final Consumer<BinaryEntrypointClient> handler) throws IOException
    {
        try (BinaryEntrypointClient client = newClient())
        {
            final int nextSeqNo = alreadyRecvMsgCount + 1;
            client.writeEstablish(nextSeqNo);

            libraryAcquiresConnection(client);

            client.readEstablishAck(nextSeqNo, alreadyRecvMsgCount);

            assertConnectionMatches(client);

            handler.accept(client);
        }
    }

    private void exchangeOrderAndReportNew(final BinaryEntrypointClient client)
    {
        exchangeOrderAndReportNew(client, CL_ORD_ID);
    }

    private void exchangeOrderAndReportNew(final BinaryEntrypointClient client, final int clOrdId)
    {
        client.writeNewOrderSingle(clOrdId);
        assertReceivesOrder();
        client.readExecutionReportNew(clOrdId);
    }

    private void assertNextSequenceNumbers(final int nextRecvSeqNo, final int nextSentSeqNo)
    {
        assertEquals("wrong nextSentSeqNo", nextSentSeqNo, connection.nextSentSeqNo());
        assertEquals("wrong nextRecvSeqNo", nextRecvSeqNo, connection.nextRecvSeqNo());
    }

    private void connectWithSessionVerId(final int sessionVerID) throws IOException
    {
        reNegotiateWithVerId(sessionVerID, client ->
        {
            exchangeOrderAndReportNew(client);

            assertNextSequenceNumbers(2, 2);

            clientTerminatesSession(client);
        });

        resetHandlers();
    }

    private void reNegotiateWithVerId(
        final int sessionVerID, final Consumer<BinaryEntrypointClient> handler)
        throws IOException
    {
        resetHandlers();

        try (BinaryEntrypointClient client = newClient())
        {
            client.sessionVerID(sessionVerID);
            establishNewConnection(client);

            handler.accept(client);
        }
    }

    private void successfulConnection() throws IOException
    {
        setupArtio(true);

        establishSuccessNewConnection();

        resetHandlers();
    }

    private void establishSuccessNewConnection() throws IOException
    {
        try (BinaryEntrypointClient client = establishNewConnection())
        {
            exchangeOrderAndReportNew(client);

            assertNextSequenceNumbers(2, 2);

            clientTerminatesSession(client);
        }
    }

    private void resetHandlers()
    {
        connectionHandler.replyToOrder(true);
        connectionExistsHandler.reset();
        connectionAcquiredHandler.reset();
        connection = null;
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

            assertAuthStrategyReject(client.sessionVerID());
        }
    }

    private void assertAuthStrategyReject(final long sessionVerID)
    {
        final BinaryEntryPointContext id =
            (BinaryEntryPointContext)fixPAuthenticationStrategy.lastSessionId();
        assertNotNull(id);
        assertEquals(SESSION_ID, id.sessionID());
        assertEquals(sessionVerID, id.sessionVerID());

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

    private void acceptorTerminatesSession(final BinaryEntrypointClient client)
    {
        client.readTerminate();
        client.writeTerminate();

        client.assertDisconnected();
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
        client.readFirstEstablishAck();

        assertConnectionMatches(client);
    }

    private void assertConnectionMatches(final BinaryEntrypointClient client)
    {
        connection = (BinaryEntrypointConnection)connectionAcquiredHandler.connection();
        assertEquals(SESSION_ID, connection.sessionId());
        assertEquals(client.sessionVerID(), connection.sessionVerId());
        assertEquals(FixPConnection.State.ESTABLISHED, connection.state());
    }

    private void libraryAcquiresConnection(final BinaryEntrypointClient client)
    {
        testSystem.await("connection doesn't exist", connectionExistsHandler::invoked);
        assertNotNull(fixPAuthenticationStrategy.lastSessionId());
        assertEquals(SESSION_ID, connectionExistsHandler.lastSurrogateSessionId());
        final BinaryEntryPointContext id =
            (BinaryEntryPointContext)connectionExistsHandler.lastIdentification();
        assertEquals(SESSION_ID, id.sessionID());
        assertEquals("sessionVerID", client.sessionVerID(), id.sessionVerID());
        final Reply<SessionReplyStatus> reply = connectionExistsHandler.lastReply();

        testSystem.awaitCompletedReply(reply);
        assertEquals(SessionReplyStatus.OK, reply.resultIfPresent());

        testSystem.await("connection not acquired", connectionAcquiredHandler::invoked);
    }

    @After
    public void close()
    {
        closeArtio();
        cleanupMediaDriver(mediaDriver);

        if (printErrors)
        {
            verifyNoInteractions(errorHandler);
        }

        Mockito.framework().clearInlineMocks();
    }

    private void closeArtio()
    {
        testSystem.awaitBlocking(() -> CloseHelper.close(engine));
        testSystem.close(library);
    }
}
