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

import b3.entrypoint.fixp.sbe.NewOrderSingleDecoder;
import io.aeron.archive.ArchivingMediaDriver;
import org.agrona.CloseHelper;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.EpochNanoClock;
import org.agrona.concurrent.OffsetEpochNanoClock;
import org.junit.After;
import org.mockito.Mockito;
import uk.co.real_logic.artio.CommonConfiguration;
import uk.co.real_logic.artio.MonitoringAgentFactory;
import uk.co.real_logic.artio.Reply;
import uk.co.real_logic.artio.binary_entrypoint.BinaryEntryPointContext;
import uk.co.real_logic.artio.binary_entrypoint.BinaryEntryPointConnection;
import uk.co.real_logic.artio.engine.EngineConfiguration;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.engine.ILink3RetransmitHandler;
import uk.co.real_logic.artio.engine.LowResourceEngineScheduler;
import uk.co.real_logic.artio.fixp.FixPCancelOnDisconnectTimeoutHandler;
import uk.co.real_logic.artio.fixp.FixPConnection;
import uk.co.real_logic.artio.library.FixLibrary;
import uk.co.real_logic.artio.library.LibraryConfiguration;
import uk.co.real_logic.artio.messages.FixPProtocolType;
import uk.co.real_logic.artio.messages.SessionReplyStatus;

import java.io.IOException;

import static io.aeron.CommonContext.IPC_CHANNEL;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoInteractions;
import static uk.co.real_logic.artio.TestFixtures.*;
import static uk.co.real_logic.artio.engine.EngineConfiguration.DEFAULT_NO_LOGON_DISCONNECT_TIMEOUT_IN_MS;
import static uk.co.real_logic.artio.engine.EngineConfiguration.DEFAULT_SENDER_MAX_BYTES_IN_BUFFER;
import static uk.co.real_logic.artio.library.LibraryConfiguration.NO_FIXP_MAX_RETRANSMISSION_RANGE;
import static uk.co.real_logic.artio.system_tests.AbstractMessageBasedAcceptorSystemTest.TEST_THROTTLE_WINDOW_IN_MS;
import static uk.co.real_logic.artio.system_tests.AbstractMessageBasedAcceptorSystemTest.THROTTLE_MSG_LIMIT;
import static uk.co.real_logic.artio.system_tests.BinaryEntryPointClient.CL_ORD_ID;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.ACCEPTOR_LOGS;
import static uk.co.real_logic.artio.system_tests.SystemTestUtil.TEST_REPLY_TIMEOUT_IN_MS;

public class AbstractBinaryEntryPointSystemTest
{
    public static final long TEST_TIMEOUT_IN_MS = 20_000L;

    static final int AWAIT_TIMEOUT_IN_MS = 10_000;
    static final int TIMEOUT_EPSILON_IN_MS = 50;
    static final int TEST_NO_LOGON_DISCONNECT_TIMEOUT_IN_MS = 2000;

    final EpochNanoClock nanoClock = new OffsetEpochNanoClock();
    final int port = unusedPort();

    ArchivingMediaDriver mediaDriver;
    TestSystem testSystem;
    FixEngine engine;
    FixLibrary library;

    final ErrorHandler errorHandler = mock(ErrorHandler.class);
    final ILink3RetransmitHandler retransmitHandler = mock(ILink3RetransmitHandler.class);
    final FakeFixPConnectionExistsHandler connectionExistsHandler = new FakeFixPConnectionExistsHandler();
    final FakeBinaryEntrypointConnectionHandler connectionHandler = new FakeBinaryEntrypointConnectionHandler();
    final FakeFixPConnectionAcquiredHandler connectionAcquiredHandler = new FakeFixPConnectionAcquiredHandler(
        connectionHandler);
    final FakeFixPAuthenticationStrategy fixPAuthenticationStrategy = new FakeFixPAuthenticationStrategy();

    BinaryEntryPointConnection connection;

    boolean printErrors = true;
    boolean acceptorWillTerminate = false;
    long artioKeepAliveIntervalInMs = CommonConfiguration.DEFAULT_ACCEPTOR_FIXP_KEEPALIVE_TIMEOUT_IN_MS;

    void setupArtio()
    {
        setup();
        setupJustArtio(true);
    }

    void setup()
    {
        mediaDriver = launchMediaDriver();
        newTestSystem();
    }

    void newTestSystem()
    {
        testSystem = new TestSystem().awaitTimeoutInMs(AWAIT_TIMEOUT_IN_MS);
    }

    void setupArtio(
        final int logonTimeoutInMs,
        final int fixPAcceptedSessionMaxRetransmissionRange)
    {
        setup();
        setupJustArtio(
            true,
            logonTimeoutInMs,
            fixPAcceptedSessionMaxRetransmissionRange,
            null,
            false,
            DEFAULT_SENDER_MAX_BYTES_IN_BUFFER);
    }

    void setupJustArtio(final boolean deleteLogFileDirOnStart)
    {
        setupJustArtio(
            deleteLogFileDirOnStart,
            DEFAULT_NO_LOGON_DISCONNECT_TIMEOUT_IN_MS,
            NO_FIXP_MAX_RETRANSMISSION_RANGE,
            null,
            false,
            DEFAULT_SENDER_MAX_BYTES_IN_BUFFER);
    }

    void setupJustArtio(
        final boolean deleteLogFileDirOnStart,
        final int shortLogonTimeoutInMs,
        final int fixPAcceptedSessionMaxRetransmissionRange,
        final FixPCancelOnDisconnectTimeoutHandler cancelOnDisconnectTimeoutHandler,
        final boolean enableThrottle, final int senderMaxBytesInBuffer)
    {
        final EngineConfiguration engineConfig = new EngineConfiguration()
            .logFileDir(ACCEPTOR_LOGS)
            .scheduler(new LowResourceEngineScheduler())
            .libraryAeronChannel(IPC_CHANNEL)
            .slowConsumerTimeoutInMs(TEST_REPLY_TIMEOUT_IN_MS)
            .replyTimeoutInMs(TEST_REPLY_TIMEOUT_IN_MS)
            .noLogonDisconnectTimeoutInMs(shortLogonTimeoutInMs)
            .fixPAuthenticationStrategy(fixPAuthenticationStrategy)
            .fixPRetransmitHandler(retransmitHandler)
            .fixPCancelOnDisconnectTimeoutHandler(cancelOnDisconnectTimeoutHandler)
            .acceptFixPProtocol(FixPProtocolType.BINARY_ENTRYPOINT)
            .bindTo("localhost", port)
            .deleteLogFileDirOnStart(deleteLogFileDirOnStart)
            .epochNanoClock(nanoClock)
            .senderMaxBytesInBuffer(senderMaxBytesInBuffer);

        engineConfig.errorHandlerFactory(ffs -> Throwable::printStackTrace).printAeronStreamIdentifiers(true);
        engineConfig.acceptorFixPKeepaliveTimeoutInMs(artioKeepAliveIntervalInMs);
        configureAeronArchive(engineConfig.aeronArchiveContext());

        if (!printErrors)
        {
            engineConfig
                .errorHandlerFactory(errorBuffer -> errorHandler)
                .monitoringAgentFactory(MonitoringAgentFactory.none());
        }

        if (enableThrottle)
        {
            engineConfig.enableMessageThrottle(TEST_THROTTLE_WINDOW_IN_MS, THROTTLE_MSG_LIMIT);
        }

        engine = FixEngine.launch(engineConfig);

        library = launchLibrary(
            shortLogonTimeoutInMs,
            fixPAcceptedSessionMaxRetransmissionRange,
            connectionExistsHandler,
            connectionAcquiredHandler);
    }

    FixLibrary launchLibrary(
        final int shortLogonTimeoutInMs,
        final int fixPAcceptedSessionMaxRetransmissionRange,
        final FakeFixPConnectionExistsHandler connectionExistsHandler,
        final FakeFixPConnectionAcquiredHandler connectionAcquiredHandler)
    {
        final LibraryConfiguration libraryConfig = new LibraryConfiguration()
            .libraryAeronChannels(singletonList(IPC_CHANNEL))
            .replyTimeoutInMs(TEST_REPLY_TIMEOUT_IN_MS)
            .fixPConnectionExistsHandler(connectionExistsHandler)
            .fixPConnectionAcquiredHandler(connectionAcquiredHandler)
            .noEstablishFixPTimeoutInMs(shortLogonTimeoutInMs)
            .fixPAcceptedSessionMaxRetransmissionRange(fixPAcceptedSessionMaxRetransmissionRange)
            .epochNanoClock(nanoClock);

        libraryConfig.acceptorFixPKeepaliveTimeoutInMs(artioKeepAliveIntervalInMs).printAeronStreamIdentifiers(true);

        if (!printErrors)
        {
            libraryConfig
                .errorHandlerFactory(errorBuffer -> errorHandler)
                .monitoringAgentFactory(MonitoringAgentFactory.none());
        }

        return testSystem.connect(libraryConfig);
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

    void closeArtio()
    {
        testSystem.awaitBlocking(() -> CloseHelper.close(engine));
        testSystem.close(library);
    }

    BinaryEntryPointClient establishNewConnection() throws IOException
    {
        final BinaryEntryPointClient client = newClient();
        establishNewConnection(client);
        return client;
    }

    void establishNewConnection(final BinaryEntryPointClient client)
    {
        establishNewConnection(client, connectionExistsHandler, connectionAcquiredHandler, false);
    }

    void establishNewConnection(
        final BinaryEntryPointClient client,
        final FakeFixPConnectionExistsHandler connectionExistsHandler,
        final FakeFixPConnectionAcquiredHandler connectionAcquiredHandler, final boolean offlineOwned)
    {
        client.writeNegotiate();

        libraryAcquiresConnection(client, connectionExistsHandler, connectionAcquiredHandler, offlineOwned);

        client.readNegotiateResponse();

        client.writeEstablish();
        client.readFirstEstablishAck();

        assertConnectionMatches(client, connectionAcquiredHandler);
    }

    void resetHandlers()
    {
        connectionHandler.replyToOrder(true);
        connectionHandler.reset();
        connectionExistsHandler.reset();
        connectionAcquiredHandler.reset();
        connection = null;
    }

    BinaryEntryPointClient newClient() throws IOException
    {
        return new BinaryEntryPointClient(port, testSystem, artioKeepAliveIntervalInMs);
    }

    void assertConnectionMatches(final BinaryEntryPointClient client)
    {
        assertConnectionMatches(client, connectionAcquiredHandler);
    }

    void assertConnectionMatches(
        final BinaryEntryPointClient client, final FakeFixPConnectionAcquiredHandler connectionAcquiredHandler)
    {
        acquireConnection(connectionAcquiredHandler);
        assertEquals(client.sessionId(), connection.sessionId());
        assertEquals(client.sessionVerID(), connection.sessionVerId());
        assertEquals(client.sessionVerID(), connectionAcquiredHandler.sessionVerIdAtAcquire());
        assertEquals(FixPConnection.State.ESTABLISHED, connection.state());
    }

    void acquireConnection(final FakeFixPConnectionAcquiredHandler connectionAcquiredHandler)
    {
        connection = (BinaryEntryPointConnection)connectionAcquiredHandler.connection();
    }

    void libraryAcquiresConnection(
        final BinaryEntryPointClient client,
        final FakeFixPConnectionExistsHandler connectionExistsHandler,
        final FakeFixPConnectionAcquiredHandler connectionAcquiredHandler,
        final boolean offlineOwned)
    {
        if (offlineOwned)
        {
            testSystem.await("connection not acquired", connectionAcquiredHandler::invoked);
            testSystem.await("not authenticated", () -> fixPAuthenticationStrategy.lastSessionId() != null);

            lastAuthStrategySessionIs(client);
        }
        else
        {
            testSystem.await("connection doesn't exist", connectionExistsHandler::invoked);
            testSystem.await("not authenticated", () -> fixPAuthenticationStrategy.lastSessionId() != null);

            lastAuthStrategySessionIs(client);

            assertEquals(client.sessionId(), connectionExistsHandler.lastSurrogateSessionId());
            final BinaryEntryPointContext id =
                (BinaryEntryPointContext)connectionExistsHandler.lastIdentification();
            assertEquals("wrong sessionId", client.sessionId(), id.sessionID());
            assertEquals("wrong sessionVerID", client.sessionVerID(), id.sessionVerID());
            final Reply<SessionReplyStatus> reply = connectionExistsHandler.lastReply();

            testSystem.awaitCompletedReply(reply);
            assertEquals(SessionReplyStatus.OK, reply.resultIfPresent());

            testSystem.await("connection not acquired", connectionAcquiredHandler::invoked);
        }
    }

    private void lastAuthStrategySessionIs(final BinaryEntryPointClient client)
    {
        final BinaryEntryPointContext context = (BinaryEntryPointContext)fixPAuthenticationStrategy.lastSessionId();
        assertNotNull(context);
        assertEquals("wrong sessionId", client.sessionId(), context.sessionID());
        assertEquals("wrong sessionVerID", client.sessionVerID(), context.sessionVerID());
        assertEquals("wrong sessionId", BinaryEntryPointClient.CREDENTIALS, context.credentials());
        if (context.fromNegotiate())
        {
            assertEquals(BinaryEntryPointClient.CLIENT_IP, context.clientIP());
            assertEquals(BinaryEntryPointClient.CLIENT_APP_NAME, context.clientAppName());
            assertEquals(BinaryEntryPointClient.CLIENT_APP_VERSION, context.clientAppVersion());
        }
    }

    void clientTerminatesConnection(final BinaryEntryPointClient client)
    {
        client.writeTerminate();
        client.readTerminate();

        client.close();

        assertConnectionDisconnected();
    }

    void acceptorTerminatesConnection(final BinaryEntryPointClient client)
    {
        client.readTerminate();
        client.writeTerminate();

        client.assertDisconnected();
        assertConnectionDisconnected();
    }

    void assertConnectionDisconnected()
    {
        testSystem.await("onDisconnect not called", () -> connectionHandler.disconnectReason() != null);
        assertEquals(FixPConnection.State.UNBOUND, connection.state());
    }

    void libraryAcquiresConnection(final BinaryEntryPointClient client)
    {
        libraryAcquiresConnection(client, connectionExistsHandler, connectionAcquiredHandler, false);
    }

    void connectAndExchangeBusinessMessage() throws IOException
    {
        try (BinaryEntryPointClient client = establishNewConnection())
        {
            assertNextSequenceNumbers(1, 1);

            exchangeOrderAndReportNew(client);

            assertNextSequenceNumbers(2, 2);

            clientTerminatesConnection(client);
        }
    }

    void exchangeOrderAndReportNew(final BinaryEntryPointClient client)
    {
        exchangeOrderAndReportNew(client, CL_ORD_ID);
    }

    void exchangeOrderAndReportNew(final BinaryEntryPointClient client, final int clOrdId)
    {
        exchangeOrderAndReportNew(client, clOrdId, connectionHandler);
    }

    void exchangeOrderAndReportNew(
        final BinaryEntryPointClient client,
        final int clOrdId,
        final FakeBinaryEntrypointConnectionHandler connectionHandler)
    {
        client.writeNewOrderSingle(clOrdId);
        assertReceivesOrder(connectionHandler);
        client.readExecutionReportNew(clOrdId);
    }

    void assertReceivesOrder()
    {
        assertReceivesOrder(connectionHandler);
    }

    void assertReceivesOrder(final FakeBinaryEntrypointConnectionHandler connectionHandler)
    {
        testSystem.await("does not receive new order single",
            () -> connectionHandler.templateIds().containsInt(NewOrderSingleDecoder.TEMPLATE_ID));
    }

    void assertNextSequenceNumbers(final int nextRecvSeqNo, final int nextSentSeqNo)
    {
        assertEquals("wrong nextSentSeqNo", nextSentSeqNo, connection.nextSentSeqNo());
        assertEquals("wrong nextRecvSeqNo", nextRecvSeqNo, connection.nextRecvSeqNo());
    }
}
