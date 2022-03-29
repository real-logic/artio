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
import io.aeron.archive.client.AeronArchive;
import org.agrona.collections.Long2LongHashMap;
import org.agrona.collections.LongArrayList;
import org.agrona.concurrent.status.ReadablePosition;
import org.hamcrest.Matchers;
import org.junit.Test;
import uk.co.real_logic.artio.DebugLogger;
import uk.co.real_logic.artio.LogTag;
import uk.co.real_logic.artio.Reply;
import uk.co.real_logic.artio.binary_entrypoint.BinaryEntryPointConnection;
import uk.co.real_logic.artio.binary_entrypoint.BinaryEntryPointContext;
import uk.co.real_logic.artio.binary_entrypoint.BinaryEntryPointKey;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.engine.FixPConnectedSessionInfo;
import uk.co.real_logic.artio.engine.FixPSessionInfo;
import uk.co.real_logic.artio.engine.framer.LibraryInfo;
import uk.co.real_logic.artio.fixp.FixPConnection;
import uk.co.real_logic.artio.ilink.ILink3Connection;
import uk.co.real_logic.artio.ilink.ILink3ConnectionConfiguration;
import uk.co.real_logic.artio.library.FixLibrary;
import uk.co.real_logic.artio.messages.SessionReplyStatus;
import uk.co.real_logic.artio.messages.ThrottleConfigurationStatus;
import uk.co.real_logic.artio.session.Session;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static io.aeron.logbuffer.LogBufferDescriptor.TERM_MIN_LENGTH;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;
import static uk.co.real_logic.artio.TestFixtures.launchMediaDriver;
import static uk.co.real_logic.artio.engine.EngineConfiguration.DEFAULT_NO_LOGON_DISCONNECT_TIMEOUT_IN_MS;
import static uk.co.real_logic.artio.engine.FixEngine.ENGINE_LIBRARY_ID;
import static uk.co.real_logic.artio.fixp.FixPConnection.State.*;
import static uk.co.real_logic.artio.library.LibraryConfiguration.NO_FIXP_MAX_RETRANSMISSION_RANGE;
import static uk.co.real_logic.artio.messages.ThrottleConfigurationStatus.OK;
import static uk.co.real_logic.artio.system_tests.AbstractMessageBasedAcceptorSystemTest.*;
import static uk.co.real_logic.artio.system_tests.ArchivePruneSystemTest.*;
import static uk.co.real_logic.artio.system_tests.BinaryEntryPointClient.*;
import static uk.co.real_logic.artio.system_tests.FakeBinaryEntrypointConnectionHandler.sendExecutionReportNew;
import static uk.co.real_logic.artio.system_tests.FakeFixPConnectionExistsHandler.requestSession;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.initiate;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.libraries;

public class BinaryEntryPointSystemTest extends AbstractBinaryEntryPointSystemTest
{

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldEstablishConnectionAtBeginningOfWeek() throws IOException
    {
        setupArtio();

        try (BinaryEntryPointClient client = establishNewConnection())
        {
            clientTerminatesSession(client);
        }
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldSupportAcceptorTerminateConnection() throws IOException
    {
        setupArtio();

        try (BinaryEntryPointClient client = establishNewConnection())
        {
            connection.terminate(TerminationCode.FINISHED);
            assertEquals(UNBINDING, connection.state());

            acceptorTerminatesSession(client);
        }
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldExchangeBusinessMessage() throws IOException
    {
        setupArtio();

        connectAndExchangeBusinessMessage();
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldCorrectlyAbortBusinessMessage() throws IOException
    {
        setupArtio();

        try (BinaryEntryPointClient client = establishNewConnection())
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

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldRejectConnectionsIfAuthenticationFails() throws IOException
    {
        setupArtio();

        fixPAuthenticationStrategy.reject();

        connectionRejected(NegotiationRejectCode.CREDENTIALS);
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldRejectConnectionsWithDuplicateIds() throws IOException
    {
        setupArtio();

        try (BinaryEntryPointClient client = establishNewConnection())
        {
            connectionExistsHandler.reset();
            connectionAcquiredHandler.reset();

            connectionRejected(NegotiationRejectCode.DUPLICATE_ID);

            clientTerminatesSession(client);
        }

        // Check that we can Reconnect afterwards
        connectWithSessionVerId(2);
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldAcceptReNegotiationsWithIncrementingSessionVerId() throws IOException
    {
        successfulConnection();

        connectWithSessionVerId(2);

        // Also accept renegotiations with a gap
        connectWithSessionVerId(4);

        restartArtio();

        connectWithSessionVerId(5);
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldRejectConnectionsWithNonIncrementingSessionVerId() throws IOException
    {
        successfulConnection();

        try (BinaryEntryPointClient client = newClient())
        {
            client.writeNegotiate();

            client.readNegotiateReject(NegotiationRejectCode.DUPLICATE_ID);
            client.assertDisconnected();
        }
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldAcceptConnectionsWithArbitraryFirstSessionVerId() throws IOException
    {
        setupArtio();

        try (BinaryEntryPointClient client = newClient())
        {
            client.sessionVerID(2);
            client.writeNegotiate();

            client.readNegotiateResponse();
            client.assertDisconnected();
        }
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldRejectUnNegotiatedEstablish() throws IOException
    {
        setupArtio();

        try (BinaryEntryPointClient client = newClient())
        {
            client.writeEstablish();
            client.readEstablishReject(EstablishRejectCode.UNNEGOTIATED);
            client.assertDisconnected();
        }
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldRejectUnNegotiatedEstablishWithHigherSessionVerId() throws IOException
    {
        successfulConnection();

        try (BinaryEntryPointClient client = newClient())
        {
            client.sessionVerID(2);
            client.writeEstablish();
            client.readEstablishReject(EstablishRejectCode.UNNEGOTIATED);
            client.assertDisconnected();
        }
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldDisconnectIfNegotiateTimeout() throws IOException
    {
        setupArtio(TEST_NO_LOGON_DISCONNECT_TIMEOUT_IN_MS, 1);

        final long timeInMs = System.currentTimeMillis();
        try (BinaryEntryPointClient client = newClient())
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

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldDisconnectIfEstablishNotSent() throws IOException
    {
        setupArtio(
            TEST_NO_LOGON_DISCONNECT_TIMEOUT_IN_MS,
            NO_FIXP_MAX_RETRANSMISSION_RANGE);

        try (BinaryEntryPointClient client = newClient())
        {
            final long timeInMs = System.currentTimeMillis();
            client.writeNegotiate();
            client.readNegotiateResponse();

            client.assertDisconnected();
            final long durationInMs = System.currentTimeMillis() - timeInMs;
            assertThat(durationInMs, Matchers.greaterThanOrEqualTo((long)TEST_NO_LOGON_DISCONNECT_TIMEOUT_IN_MS));
        }
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldAcceptReEstablishmentOfSession() throws IOException
    {
        successfulConnection();

        reEstablishConnection(1, 1);

        reEstablishConnection(2, 2);

        restartArtio();
        reEstablishConnection(3, 3);
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldRejectReEstablishmentOfSessionIfAuthenticationFails() throws IOException
    {
        successfulConnection();

        fixPAuthenticationStrategy.reject();

        final long sessionVerID = rejectedReestablish(EstablishRejectCode.CREDENTIALS);
        assertAuthStrategyReject(sessionVerID);
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldRejectLaterEstablishMessage() throws IOException
    {
        setupArtio();

        try (BinaryEntryPointClient client = establishNewConnection())
        {
            client.writeEstablish();

            // This establish reject doesn't disconnect the already established connection like the others, it is
            // just ignored.
            client.readEstablishReject(EstablishRejectCode.ALREADY_ESTABLISHED);

            clientTerminatesSession(client);
        }
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldRejectEstablishMessageWithInvalidKeepAliveInterval() throws IOException
    {
        setupArtio();

        try (BinaryEntryPointClient client = newClient())
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

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldAcceptRetransmitAfterASequenceMessageBasedGap() throws IOException
    {
        setupArtio();

        try (BinaryEntryPointClient client = establishNewConnection())
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

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldAcceptRetransmitAfterAnEstablishMessageBasedGap() throws IOException
    {
        successfulConnection();

        connectionHandler.replyToOrder(false);

        try (BinaryEntryPointClient client = newClient())
        {
            client.writeEstablish(4);
            libraryAcquiresConnection(client);
            assertConnectionMatches(client);
            client.readEstablishAck(4, 1);

            retransmitAfterGap(client);
        }

        assertSequenceUpdatePersistedInIndex();
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldAcceptValidRetransmitRequest() throws IOException
    {
        setupArtio();

        try (BinaryEntryPointClient client = establishNewConnection())
        {
            exchange4OrdersAndReports(client);

            assertMessagesRetransmitted(client, 5);

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

            assertMessagesRetransmitted(client, 5);

            clientTerminatesSession(client);
        });
    }

    private void assertMessagesRetransmitted(final BinaryEntryPointClient client, final long nextSeqNo)
    {
        client.writeRetransmitRequest(2, 2);
        client.readRetransmission(2, 2);
        client.readExecutionReportNew(2);
        client.readExecutionReportNew(3);
        client.readSequence(nextSeqNo);
    }

    private void exchange4OrdersAndReports(final BinaryEntryPointClient client)
    {
        exchangeNOrdersAndReports(client, 4);
        assertNextSequenceNumbers(5, 5);
    }

    private void exchangeNOrdersAndReports(final BinaryEntryPointClient client, final int n)
    {
        for (int i = 0; i < n; i++)
        {
            exchangeOrderAndReportNew(client, i + 1);
        }
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldRejectRetransmitRequestWithHighEndNo() throws IOException
    {
        setupArtio();

        try (BinaryEntryPointClient client = establishNewConnection())
        {
            exchangeOrderAndReportNew(client, 1);
            exchangeOrderAndReportNew(client, 2);
            assertNextSequenceNumbers(3, 3);

            client.writeRetransmitRequest(2, 2);
            client.readRetransmitReject(RetransmitRejectCode.OUT_OF_RANGE);

            clientTerminatesSession(client);
        }
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldRejectRetransmitRequestWithHighStartNo() throws IOException
    {
        setupArtio();

        try (BinaryEntryPointClient client = establishNewConnection())
        {
            assertNextSequenceNumbers(1, 1);

            client.writeRetransmitRequest(2, 2);
            client.readRetransmitReject(RetransmitRejectCode.OUT_OF_RANGE);

            clientTerminatesSession(client);
        }
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldRejectRetransmitRequestWithWrongSessionId() throws IOException
    {
        setupArtio();

        try (BinaryEntryPointClient client = establishNewConnection())
        {
            exchangeOrderAndReportNew(client, 1);
            client.writeRetransmitRequest(1000, 1, 1);
            client.readRetransmitReject(RetransmitRejectCode.INVALID_SESSION);

            clientTerminatesSession(client);
        }
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldRejectRetransmitRequestLimitExceeded() throws IOException
    {
        setupArtio(
            DEFAULT_NO_LOGON_DISCONNECT_TIMEOUT_IN_MS,
            1);

        try (BinaryEntryPointClient client = establishNewConnection())
        {
            exchangeOrderAndReportNew(client, 1);
            exchangeOrderAndReportNew(client, 2);
            client.writeRetransmitRequest(1, 2);
            client.readRetransmitReject(RetransmitRejectCode.REQUEST_LIMIT_EXCEEDED);

            clientTerminatesSession(client);
        }
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldNotInterleaveRetransmitRequestAndMessageSending() throws IOException
    {
        setup();
        final int senderMaxBytesInBuffer = 128;
        setupJustArtio(
            true,
            DEFAULT_NO_LOGON_DISCONNECT_TIMEOUT_IN_MS,
            NO_FIXP_MAX_RETRANSMISSION_RANGE,
            null,
            false,
            senderMaxBytesInBuffer);

        try (BinaryEntryPointClient client = establishNewConnection())
        {
            final int retransmitCount = 1_000;
            exchangeNOrdersAndReports(client, retransmitCount);

            client.writeRetransmitRequest(1, retransmitCount);
            client.readRetransmission(1, retransmitCount);
            client.readExecutionReportNew(1);

            final int newClOrdId = retransmitCount + 1;
            sendExecutionReportNew(connection, newClOrdId, SECURITY_ID, false);

            for (int i = 2; i < retransmitCount + 1; i++)
            {
                client.readExecutionReportNew(i);
            }

            // receive new execution report after
            client.readSequence(newClOrdId);
            client.readExecutionReportNew(newClOrdId);

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
    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldRespondToFinishedSendingWithFinishedReceiving() throws IOException
    {
        setupArtio();

        try (BinaryEntryPointClient client = establishNewConnection())
        {
            clientInitiatedFinishSending(client);

            assertTrue("onFinishedSending() not invoked", connectionHandler.hasFinishedSending());

            // can still send a message before we send finished sending.
            final int clOrderID = 2;
            sendExecutionReportNew(connection, clOrderID, SECURITY_ID, false);
            client.readExecutionReportNew(clOrderID);

            assertNextSequenceNumbers(2, 3);

            acceptorInitiatedFinishSending(client, 1);

            clientTerminatesSession(client);
        }

        // Cannot re-establish finished sequence
        rejectedReestablish(EstablishRejectCode.UNNEGOTIATED);
        restartArtio();
        rejectedReestablish(EstablishRejectCode.UNNEGOTIATED);

        // Can reconnect with higher session ver id
        connectWithSessionVerId(2);
    }

    private void clientInitiatedFinishSending(final BinaryEntryPointClient client)
    {
        exchangeOrderAndReportNew(client);

        assertNextSequenceNumbers(2, 2);

        client.writeFinishedSending(1);
        client.readFinishedReceiving();
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldCompleteFinishedSendingProcess() throws IOException
    {
        setupArtio();

        try (BinaryEntryPointClient client = establishNewConnection())
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

    private void acceptorInitiatedFinishSending(final BinaryEntryPointClient client)
    {
        acceptorInitiatedFinishSending(client, 1);
    }

    private void acceptorInitiatedFinishSending(final BinaryEntryPointClient client, final int lastSeqNo)
    {
        connection.finishSending();

        assertCannotSendMessage();

        client.readFinishedSending(lastSeqNo);
        client.writeFinishedReceiving();
    }

    // FIXP Spec 7.4.2
    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldUseFinishedSendingAsAHeartbeatKeepAliveInTheAbsenceOfResponse() throws IOException
    {
        setupArtio();

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
    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldProcessRetransmitRequestsInResponseToFinishSending() throws IOException
    {
        setupArtio();

        try (BinaryEntryPointClient client = establishNewConnection())
        {
            clientInitiatedFinishSending(client);

            processRetransmitRequestsDuringFinishSending(client);

            acceptorTerminatesSession(client);
        }
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldProcessRetransmitRequestsInResponseToAcceptorFinishSending() throws IOException
    {
        setupArtio();

        try (BinaryEntryPointClient client = establishNewConnection())
        {
            exchangeOrderAndReportNew(client);

            processRetransmitRequestsDuringFinishSending(client);

            client.writeFinishedSending(1);
            client.readFinishedReceiving();

            acceptorTerminatesSession(client);
        }
    }

    // FIXP Spec 7.4.4
    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldTerminateInResponseToReceivingTerminate() throws IOException
    {
        setupArtio();

        try (BinaryEntryPointClient client = establishNewConnection())
        {
            exchangeOrderAndReportNew(client);

            acceptorInitiatedFinishSending(client);

            client.writeTerminate();
            client.readTerminate();
        }
    }

    // FIXP Spec 7.4.5
    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldTerminateInResponseToReceivingMessageAfterFinishedSending() throws IOException
    {
        setupArtio();

        try (BinaryEntryPointClient client = establishNewConnection())
        {
            terminatedBySendingMessageAfterFinishedSending(client, () ->
                client.writeNewOrderSingle(2));
        }

        reNegotiateWithVerId(3, false, client ->
            terminatedBySendingMessageAfterFinishedSending(client, () ->
            client.writeSequence(1)));

        reNegotiateWithVerId(4, false, client ->
            terminatedBySendingMessageAfterFinishedSending(client, client::writeNegotiate));
    }

    private void terminatedBySendingMessageAfterFinishedSending(
        final BinaryEntryPointClient client,
        final Runnable sendMessage)
    {
        exchangeOrderAndReportNew(client);

        client.writeFinishedSending(1);
        client.readFinishedReceiving();

        sendMessage.run();

        client.readTerminate(TerminationCode.UNSPECIFIED);
    }

    private void processRetransmitRequestsDuringFinishSending(final BinaryEntryPointClient client)
    {
        connection.finishSending();

        client.readFinishedSending(1);

        client.writeRetransmitRequest(1, 1);
        client.readRetransmission(1, 1);
        client.readExecutionReportNew();
        client.writeFinishedReceiving();
        client.readSequence(2);
    }

    // ----------------------------------
    // END FINALIZATION TESTS
    // ----------------------------------

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldTerminateSessionWhenSequenceNumberTooLowCanReestablish() throws IOException
    {
        setupArtio();

        shouldTerminateSessionWhenSequenceNumberTooLow();

        resetHandlers();

        reEstablishConnection(1, 1);
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldTerminateSessionWhenSequenceNumberTooLowCanRenegotiate() throws IOException
    {
        setupArtio();

        shouldTerminateSessionWhenSequenceNumberTooLow();

        connectWithSessionVerId(2);
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldTerminateSessionWhenSequenceNumberTooLowCanReestablishAfterRestart() throws IOException
    {
        setupArtio();

        shouldTerminateSessionWhenSequenceNumberTooLow();

        restartArtio();
        resetHandlers();

        reEstablishConnection(1, 1);
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldTerminateSessionWhenSequenceNumberTooLowCanRenegotiateAfterRestart() throws IOException
    {
        setupArtio();

        shouldTerminateSessionWhenSequenceNumberTooLow();

        restartArtio();

        connectWithSessionVerId(2);
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldSendSequenceMessageAfterTimeElapsed() throws IOException
    {
        setupArtio();

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

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldUseSequenceMessagesAsLivenessIndicator() throws IOException
    {
        setupArtio();

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

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void sessionsListedInAdminApi() throws IOException
    {
        setupArtio();

        try (BinaryEntryPointClient client = establishNewConnection())
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

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldBlockInitiationOfOtherFixPProtocols()
    {
        printErrors = false;

        setupArtio();

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

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldBlockInitiationOfOtherFixProtocols()
    {
        printErrors = false;

        setupArtio();

        final Reply<Session> reply = initiate(library, port, "ABC", "DEF");

        testSystem.awaitErroredReply(reply, allOf(
            containsString("INVALID_CONFIGURATION"), containsString("FIXP")));
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
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
            setupJustArtio(false);
            final List<FixPSessionInfo> sessionInfos = engine.allFixPSessions();
            assertThat(sessionInfos, hasSize(0));
            connectAndExchangeBusinessMessage();
        }
        finally
        {
            backup.cleanup();
        }
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldResetSequenceNumbersOfDisconnectedSessions() throws IOException
    {
        setupArtio();

        final ReadablePosition positionCounter = testSystem.libraryPosition(engine, library);

        connectAndExchangeBusinessMessage();

        testSystem.awaitPosition(positionCounter, connectionHandler.lastPosition());

        resetSequenceNumber();

        rejectedReestablish(EstablishRejectCode.UNNEGOTIATED);
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldOnlyRequestSessionsThatCanBeAcquired() throws IOException
    {
        setupArtio();

        try (BinaryEntryPointClient client = establishNewConnection())
        {
            final Reply<SessionReplyStatus> reply = requestSession(library, connection.sessionId());
            testSystem.awaitCompletedReply(reply);
            assertEquals(SessionReplyStatus.OTHER_SESSION_OWNER, reply.resultIfPresent());
        }
    }

    // ----------------------------------
    // BEGIN PRUNE TESTS
    // ----------------------------------

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldPruneAwayOldArchivePositionsAfterRenegotiate() throws IOException
    {
        shouldPruneAwayOldArchivePositions(false, () -> connectWithSessionVerId(2));
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldPruneAwayOldArchivePositionsAfterResetSequenceNumbers() throws IOException
    {
        shouldPruneAwayOldArchivePositions(false, this::resetSequenceNumber);
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldPruneAwayOldArchivePositionsAfterFinishSendingAndRenegotiate() throws IOException
    {
        // Don't support purging the sequence after a finish sending on it's own as a resend
        // request could come in after that point.
        shouldPruneAwayOldArchivePositions(true, () -> connectWithSessionVerId(2));
    }

    private interface ResetOperation
    {
        void reset() throws IOException;
    }

    private void shouldPruneAwayOldArchivePositions(
        final boolean finishSending, final ResetOperation resetOp) throws IOException
    {
        mediaDriver = launchMediaDriver(TERM_MIN_LENGTH);
        newTestSystem();
        setupJustArtio(true);

        exchangeOverASegmentOfMessages(finishSending);

        resetOp.reset();

        assertPruneWorks();
    }

    private void assertPruneWorks() throws IOException
    {
        try (AeronArchive archive = newArchive(engine))
        {
            final Long2LongHashMap prePruneRecordingIdToStartPos = getRecordingStartPos(archive);

            final Long2LongHashMap recordingIdToStartPos = testSystem.pruneArchive(null, engine);
            final Long2LongHashMap prunedRecordingIdToStartPos = getRecordingStartPos(archive);

            DebugLogger.log(LogTag.STATE_CLEANUP,
                "prePruneRecordingIdToStartPos = " + prePruneRecordingIdToStartPos +
                ", prunedRecordingIdToStartPos = " + prunedRecordingIdToStartPos +
                ", recordingIdToStartPos = " + recordingIdToStartPos);

            assertHasBeenPruned(recordingIdToStartPos, 0L);
            assertHasBeenPruned(recordingIdToStartPos, 2L);

            assertRecordingsPruned(
                prePruneRecordingIdToStartPos, recordingIdToStartPos, prunedRecordingIdToStartPos);

            restartArtio();

            connectWithSessionVerId(3);

            // Ensure that the recordings have been extended
            final Long2LongHashMap endRecordingIdToStartPos = getRecordingStartPos(archive);
            assertEquals(prunedRecordingIdToStartPos, endRecordingIdToStartPos);
        }
    }

    private void assertHasBeenPruned(final Long2LongHashMap recordingIdToStartPos, final long recordingId)
    {
        assertThat(recordingIdToStartPos.toString(), recordingIdToStartPos,
            hasEntry(is(recordingId), greaterThanOrEqualTo((long)TERM_MIN_LENGTH)));
    }

    private void exchangeOverASegmentOfMessages(final boolean finishSending) throws IOException
    {
        try (BinaryEntryPointClient client = establishNewConnection())
        {
            final int overASegmentOfMessages = TERM_MIN_LENGTH / NewOrderSingleEncoder.BLOCK_LENGTH;
            for (int i = 0; i < overASegmentOfMessages; i++)
            {
                exchangeOrderAndReportNew(client, i);
            }

            if (finishSending)
            {
                client.writeFinishedSending(overASegmentOfMessages);
                client.readFinishedReceiving();
                acceptorInitiatedFinishSending(client, overASegmentOfMessages);
            }
        }
    }

    // ----------------------------------
    // END PRUNE TESTS
    // ----------------------------------

    // ----------------------------------
    // BEGIN CARDINALITY TESTS
    // ----------------------------------

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldSupportMultipleSessions() throws IOException
    {
        setupArtio();

        try (BinaryEntryPointClient client = establishNewConnection())
        {
            final BinaryEntryPointConnection firstConnection = this.connection;
            resetHandlers();

            try (BinaryEntryPointClient client2 = newClient())
            {
                client2.sessionId(SESSION_ID_2);
                establishNewConnection(client2);

                exchangeOrderAndReportNew(client2);

                clientTerminatesSession(client2);
            }

            this.connection = firstConnection;

            exchangeOrderAndReportNew(client);

            connection.terminate(TerminationCode.FINISHED);

            testSystem.await("Failed to send termiante", () ->
            {
                final FixPConnection.State state = connection.state();
                return state == SENT_TERMINATE || state == UNBINDING || state == UNBOUND;
            });

            acceptorTerminatesSession(client);
        }

        assertEquals(connectionHandler.sessionIds(),
            new LongArrayList(new long[]{SESSION_ID_2, SESSION_ID}, 2, LongArrayList.DEFAULT_NULL_VALUE));
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldSupportMultipleLibraries() throws IOException
    {
        setupArtio();

        final FakeFixPConnectionExistsHandler connectionExistsHandler2 = new FakeFixPConnectionExistsHandler();
        final FakeBinaryEntrypointConnectionHandler connectionHandler2 = new FakeBinaryEntrypointConnectionHandler();
        final FakeFixPConnectionAcquiredHandler connectionAcquiredHandler2 = new FakeFixPConnectionAcquiredHandler(
            connectionHandler2);

        final FixLibrary library2 = launchLibrary(
            DEFAULT_NO_LOGON_DISCONNECT_TIMEOUT_IN_MS,
            NO_FIXP_MAX_RETRANSMISSION_RANGE,
            connectionExistsHandler2,
            connectionAcquiredHandler2);

        try
        {
            connectionExistsHandler2.request(false);

            // connect to library 1
            try (BinaryEntryPointClient client = establishNewConnection())
            {
                assertEquals(connectionExistsHandler.lastIdentification(),
                    connectionExistsHandler2.lastIdentification());
                assertFalse(connectionAcquiredHandler2.invoked());

                exchangeOrderAndReportNew(client);

                assertThat(connectionHandler2.templateIds(), hasSize(0));
            }

            resetHandlers();
            connectionExistsHandler2.reset();
            connectionExistsHandler.request(false);
            connectionExistsHandler2.request(true);

            // connect to library 2
            try (BinaryEntryPointClient client = newClient())
            {
                client.sessionVerID(2);
                establishNewConnection(client, connectionExistsHandler2, connectionAcquiredHandler2, false);

                assertFalse(connectionAcquiredHandler.invoked());

                exchangeOrderAndReportNew(client, CL_ORD_ID, connectionHandler2);

                assertThat(connectionHandler.templateIds(), hasSize(0));
            }
        }
        finally
        {
            testSystem.close(library2);
        }
    }

    // ----------------------------------
    // END CARDINALITY TESTS
    // ----------------------------------

    // ----------------------------------
    // BEGIN OFFLINE TESTS
    // ----------------------------------

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldSupportOfflineSessions() throws IOException
    {
        setupArtio();

        try (BinaryEntryPointClient client = establishNewConnection())
        {
            exchangeOrderAndReportNew(client);

            clientTerminatesSession(client);
        }

        final long sessionId = connection.sessionId();
        resetHandlers();

        final Reply<SessionReplyStatus> reply = requestSession(library, sessionId);
        testSystem.awaitCompletedReply(reply);
        assertEquals(SessionReplyStatus.OK, reply.resultIfPresent());
        acquireConnection(connectionAcquiredHandler);
        assertEquals(FixPConnection.State.UNBOUND, connection.state());
        assertNextSequenceNumbers(2, 2);
        assertEquals(sessionId, connection.sessionId());
        assertEquals(1, connection.sessionVerId());
        assertEquals(1, connectionAcquiredHandler.sessionVerIdAtAcquire());

        connectionAcquiredHandler.reset();

        // Reconnect should automatically send the reconnected session to the library that owns the offline session
        try (BinaryEntryPointClient client = newClient())
        {
            client.writeEstablish(2);

            testSystem.await("connection not acquired", connectionAcquiredHandler::invoked);
            final BinaryEntryPointConnection offlineConnection = this.connection;
            acquireConnection(connectionAcquiredHandler);
            assertFalse(connectionExistsHandler.invoked());
            assertSame(offlineConnection, connection);

            assertConnectionMatches(client);
            client.readEstablishAck(2, 1);

            exchangeOrderAndReportNew(client);

            clientTerminatesSession(client);
        }
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldSupportCreatedOfflineSession() throws IOException
    {
        final int sessionVerID = offlineSessionWithRetransmittableMessage();

        assertOnlyOneFixPSession();

        // re-establish and check sequence number, retransmit messages.
        try (BinaryEntryPointClient client = newClient())
        {
            client.sessionVerID(sessionVerID);
            client.writeEstablish(1);

            testSystem.await("connection not acquired", connectionAcquiredHandler::invoked);

            assertConnectionMatches(client);
            assertNextSequenceNumbers(1, 2);
            client.readEstablishAck(1, 0);
            client.writeRetransmitRequest(1, 1);
            client.readRetransmission(1, 1);
            client.readExecutionReportNew();
            client.readSequence(2);

            clientTerminatesSession(client);
        }
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldSupportRenegotiateOfCreatedOfflineSession() throws IOException
    {
        int sessionVerID = offlineSessionWithRetransmittableMessage();

        // re-negotiate and check sequence number, retransmit messages.
        sessionVerID++;
        reNegotiateWithVerId(sessionVerID, true, client ->
        {
            assertNextSequenceNumbers(1, 1);

            exchangeOrderAndReportNew(client);
            assertNextSequenceNumbers(2, 2);
            clientTerminatesSession(client);
        });
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldCreateFollowerSessionsWhenSessionAlreadyExistsWhenLoggedIn() throws IOException
    {
        setupArtio();

        // when logged in
        final BinaryEntryPointContext contextHigherVersion;
        final BinaryEntryPointContext context;
        try (BinaryEntryPointClient client = establishNewConnection())
        {
            context = newContext();

            createFollowerSession(context);

            // cannot use a different session ver id when logged in
            contextHigherVersion = newContext(INITIAL_SESSION_VER_ID + 1, true);
            final Reply<Long> reply = library.followerFixPSession(contextHigherVersion, TEST_TIMEOUT_IN_MS);
            testSystem.awaitErroredReply(reply, containsString(
                "currently connected with a different session version"));

            clientTerminatesSession(client);
        }
        assertOnlyOneFixPSession();
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldCreateFollowerSessionsWhenSessionAlreadyExistsWhenNotLoggedIn() throws IOException
    {
        setupArtio();

        checkFollowerSessionIsConnectable(newContext(INITIAL_SESSION_VER_ID - 1, true));
        fixPAuthenticationStrategy.resetLastSessionId();
        connectionExistsHandler.reset();

        createFollowerSession(newContext(INITIAL_SESSION_VER_ID, true));
        try (BinaryEntryPointClient client = newClient())
        {
            client.sessionVerID(INITIAL_SESSION_VER_ID + 1);
            establishNewConnection(client);
            clientTerminatesSession(client);
        }

        assertOnlyOneFixPSession();
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldCreateFollowerSessionsWhenSessionAlreadyExistsWhenNotLoggedInNotNegotiate() throws IOException
    {
        setupArtio();

        checkFollowerSessionIsConnectable(newContext(INITIAL_SESSION_VER_ID - 1, false));

        checkFollowerSessionIsConnectable(newContext(INITIAL_SESSION_VER_ID - 1, false));
    }

    private void checkFollowerSessionIsConnectable(final BinaryEntryPointContext context) throws IOException
    {
        createFollowerSession(context);

        try (BinaryEntryPointClient client = establishNewConnection())
        {
            clientTerminatesSession(client);
        }

        assertOnlyOneFixPSession();
    }

    private void assertOnlyOneFixPSession()
    {
        final List<FixPSessionInfo> fixPSessions = engine.allFixPSessions();
        assertThat(fixPSessions, hasSize(1));
        final FixPSessionInfo sessionInfo = fixPSessions.get(0);
        assertEquals(new BinaryEntryPointKey(SESSION_ID), sessionInfo.key());
    }

    private BinaryEntryPointContext newContext()
    {
        return newContext(INITIAL_SESSION_VER_ID, true);
    }

    private BinaryEntryPointContext newContext(final int sessionVerId, final boolean fromNegotiate)
    {
        return new BinaryEntryPointContext(
            SESSION_ID,
            sessionVerId,
            nanoClock.nanoTime(),
            FIRM_ID,
            fromNegotiate);
    }

    private int offlineSessionWithRetransmittableMessage()
    {
        setupArtio();

        // create session and acquire it
        final int sessionVerID = 2;
        final BinaryEntryPointContext context = new BinaryEntryPointContext(
            SESSION_ID,
            sessionVerID,
            System.nanoTime(),
            FIRM_ID,
            true);

        createFollowerSession(context);

        final Reply<SessionReplyStatus> sessionReply = requestSession(library, SESSION_ID);
        testSystem.awaitCompletedReply(sessionReply);
        assertEquals(SessionReplyStatus.OK, sessionReply.resultIfPresent());
        acquireConnection(connectionAcquiredHandler);
        assertEquals(FixPConnection.State.UNBOUND, connection.state());
        assertNextSequenceNumbers(1, 1);
        assertEquals(SESSION_ID, connection.sessionId());
        assertEquals(sessionVerID, connection.sessionVerId());
        assertEquals(sessionVerID, connectionAcquiredHandler.sessionVerIdAtAcquire());

        final long msgPos = sendExecutionReportNew(connection, CL_ORD_ID, SECURITY_ID, false);
        final ReadablePosition pos = testSystem.awaitCompletedReply(
            engine.libraryIndexedPosition(library.libraryId())).resultIfPresent();
        testSystem.awaitPosition(pos, msgPos);
        assertNextSequenceNumbers(1, 2);

        resetHandlers();

        return sessionVerID;
    }

    private void createFollowerSession(final BinaryEntryPointContext context)
    {
        final Reply<Long> reply = library.followerFixPSession(context, TEST_TIMEOUT_IN_MS);
        testSystem.awaitCompletedReply(reply);
        assertEquals(context.sessionID(), reply.resultIfPresent().longValue());
    }

    // ----------------------------------
    // END OFFLINE TESTS
    // ----------------------------------

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldRejectMessagesOverThrottle() throws IOException
    {
        setup();
        setupJustArtio(
            true,
            DEFAULT_NO_LOGON_DISCONNECT_TIMEOUT_IN_MS,
            NO_FIXP_MAX_RETRANSMISSION_RANGE,
            null,
            true, EngineConfiguration.DEFAULT_SENDER_MAX_BYTES_IN_BUFFER);

        connectionHandler.replyToOrder(false);

        try (BinaryEntryPointClient client = establishNewConnection())
        {
            awaitedSleepThrottleWindow();

            assertMessagesRejectedAboveThrottleRate(client, THROTTLE_MSG_LIMIT, 1);

            // After throttle timeout we can exchange messages
            awaitedSleepThrottleWindow();
            connectionHandler.replyToOrder(true);
            exchangeOrderAndReportNew(client);
            connectionHandler.replyToOrder(false);

            // Test that resend requests work with throttle rejection
            client.writeRetransmitRequest(1, 2);
            client.readRetransmission(1, 2);
            client.readBusinessReject(4, 4);
            client.readBusinessReject(5, 5);
            client.readSequence(9);

            // Reset the throttle rate
            final Reply<ThrottleConfigurationStatus> reply = testSystem.awaitCompletedReply(
                connection.throttleMessagesAt(TEST_THROTTLE_WINDOW_IN_MS, RESET_THROTTLE_MSG_LIMIT));
            assertEquals(reply.toString(), OK, reply.resultIfPresent());

            awaitedSleepThrottleWindow();

            assertMessagesRejectedAboveThrottleRate(client, RESET_THROTTLE_MSG_LIMIT, 12);

            awaitedSleepThrottleWindow();

            clientTerminatesSession(client);
        }
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldLogoutSessionsOnEngineClose() throws Exception
    {
        setupArtio();

        try (BinaryEntryPointClient client = establishNewConnection())
        {
            closeEngine(() -> acceptorTerminatesSession(client));
        }
    }

    @Test(timeout = TEST_TIMEOUT_IN_MS)
    public void shouldDisconnectNotYetEstablishedSessionsOnEngineClose() throws Exception
    {
        setupArtio();

        connectionExistsHandler.request(false);

        try (BinaryEntryPointClient client = newClient())
        {
            client.writeNegotiate();
            closeEngine(client::assertDisconnected);
        }
    }

    private void closeEngine(final Runnable duringClose) throws InterruptedException
    {
        final AtomicBoolean closeException = new AtomicBoolean(false);
        final Thread closeThread = new Thread(() ->
        {
            try
            {
                engine.close();
            }
            catch (final Throwable t)
            {
                t.printStackTrace();
                closeException.set(true);
            }
        });
        closeThread.setName("closeThread");
        closeThread.start();

        duringClose.run();

        closeThread.join(AWAIT_TIMEOUT_IN_MS);
        assertFalse(closeException.get());
    }

    private void awaitedSleepThrottleWindow()
    {
        testSystem.awaitBlocking(MessageBasedAcceptorSystemTest::sleepThrottleWindow);
    }

    private void assertMessagesRejectedAboveThrottleRate(
        final BinaryEntryPointClient connection,
        final int limitOfMessages,
        final int seqNumOffset)
    {
        // use the same number for clOrdId as sequence numbers
        final int reportCount = 10;
        for (int i = 0; i < reportCount; i++)
        {
            connection.writeNewOrderSingle(i + seqNumOffset);

            sleep(1);
        }

        // messagesWithinThrottleWindow - could be login message at the start
        for (int i = 0; i < (reportCount - limitOfMessages); i++)
        {
            final int refSeqNum = i + seqNumOffset + limitOfMessages;
            connection.readBusinessReject(refSeqNum, refSeqNum);
        }
    }

    private void resetSequenceNumber()
    {
        final Reply<?> reply = testSystem.awaitNotNull(
            "Timed out whilst attempting resetSequenceNumber",
            () -> engine.resetSequenceNumber(connection.sessionId()));
        testSystem.awaitCompletedReply(reply);
    }

    private void assertAllSessionsOnlyContains(final FixEngine engine, final BinaryEntryPointConnection connection)
    {
        final List<FixPSessionInfo> allSessions = engine.allFixPSessions();
        assertThat(allSessions, hasSize(1));

        final FixPSessionInfo sessionInfo = allSessions.get(0);
        assertEquals(sessionInfo.key(), connection.key());
    }

    private void withLowKeepAliveClient(final Consumer<BinaryEntryPointClient> handler) throws IOException
    {
        try (BinaryEntryPointClient client = newClient())
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

    private void retransmitAfterGap(final BinaryEntryPointClient client)
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
        try (BinaryEntryPointClient client = newClient())
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
        setupJustArtio(false);
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
        final Consumer<BinaryEntryPointClient> handler) throws IOException
    {
        try (BinaryEntryPointClient client = newClient())
        {
            final int nextSeqNo = alreadyRecvMsgCount + 1;
            client.writeEstablish(nextSeqNo);

            libraryAcquiresConnection(client);

            client.readEstablishAck(nextSeqNo, alreadyRecvMsgCount);

            assertConnectionMatches(client);

            handler.accept(client);
        }
    }

    private void connectWithSessionVerId(final int sessionVerID) throws IOException
    {
        reNegotiateWithVerId(sessionVerID, false, client ->
        {
            exchangeOrderAndReportNew(client);

            assertNextSequenceNumbers(2, 2);

            clientTerminatesSession(client);
        });

        resetHandlers();
    }

    private void reNegotiateWithVerId(
        final int sessionVerID, final boolean offlineOwned, final Consumer<BinaryEntryPointClient> handler)
        throws IOException
    {
        resetHandlers();

        try (BinaryEntryPointClient client = newClient())
        {
            client.sessionVerID(sessionVerID);
            establishNewConnection(client, connectionExistsHandler, connectionAcquiredHandler, offlineOwned);

            handler.accept(client);
        }
    }

    private void successfulConnection() throws IOException
    {
        setupArtio();

        establishSuccessNewConnection();

        resetHandlers();
    }

    private void establishSuccessNewConnection() throws IOException
    {
        try (BinaryEntryPointClient client = establishNewConnection())
        {
            exchangeOrderAndReportNew(client);

            assertNextSequenceNumbers(2, 2);

            clientTerminatesSession(client);
        }
    }

    private void connectionRejected(final NegotiationRejectCode negotiationRejectCode) throws IOException
    {
        try (BinaryEntryPointClient client = newClient())
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
}
