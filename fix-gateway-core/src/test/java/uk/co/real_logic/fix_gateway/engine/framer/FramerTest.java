/*
 * Copyright 2015-2016 Real Logic Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.co.real_logic.fix_gateway.engine.framer;

import io.aeron.Subscription;
import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import io.aeron.logbuffer.Header;
import org.agrona.ErrorHandler;
import org.agrona.LangUtil;
import org.agrona.collections.LongHashSet;
import org.agrona.concurrent.QueuedPipe;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.verification.VerificationMode;
import uk.co.real_logic.fix_gateway.engine.CompletionPosition;
import uk.co.real_logic.fix_gateway.engine.EngineConfiguration;
import uk.co.real_logic.fix_gateway.engine.EngineDescriptorStore;
import uk.co.real_logic.fix_gateway.engine.SessionInfo;
import uk.co.real_logic.fix_gateway.engine.logger.ReplayQuery;
import uk.co.real_logic.fix_gateway.engine.logger.SequenceNumberIndexReader;
import uk.co.real_logic.fix_gateway.messages.*;
import uk.co.real_logic.fix_gateway.protocol.GatewayPublication;
import uk.co.real_logic.fix_gateway.replication.ClusterableStreams;
import uk.co.real_logic.fix_gateway.replication.ClusterableSubscription;
import uk.co.real_logic.fix_gateway.replication.SoloSubscription;
import uk.co.real_logic.fix_gateway.session.CompositeKey;
import uk.co.real_logic.fix_gateway.session.Session;
import uk.co.real_logic.fix_gateway.session.SessionIdStrategy;
import uk.co.real_logic.fix_gateway.timing.Timer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static io.aeron.Publication.BACK_PRESSURED;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.ABORT;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.fix_gateway.CommonConfiguration.DEFAULT_NAME_PREFIX;
import static uk.co.real_logic.fix_gateway.Timing.assertEventuallyTrue;
import static uk.co.real_logic.fix_gateway.engine.FixEngine.ENGINE_LIBRARY_ID;
import static uk.co.real_logic.fix_gateway.library.FixLibrary.NO_MESSAGE_REPLAY;
import static uk.co.real_logic.fix_gateway.library.SessionConfiguration.AUTOMATIC_INITIAL_SEQUENCE_NUMBER;
import static uk.co.real_logic.fix_gateway.messages.ConnectionType.INITIATOR;
import static uk.co.real_logic.fix_gateway.messages.DisconnectReason.APPLICATION_DISCONNECT;
import static uk.co.real_logic.fix_gateway.messages.GatewayError.*;
import static uk.co.real_logic.fix_gateway.messages.SequenceNumberType.TRANSIENT;
import static uk.co.real_logic.fix_gateway.messages.SessionReplyStatus.OK;
import static uk.co.real_logic.fix_gateway.messages.SessionState.ACTIVE;
import static uk.co.real_logic.fix_gateway.messages.SessionState.CONNECTED;

public class FramerTest
{
    private static final InetSocketAddress TEST_ADDRESS = new InetSocketAddress("localhost", 9998);
    private static final InetSocketAddress FRAMER_ADDRESS = new InetSocketAddress("localhost", 9999);
    private static final int LIBRARY_ID = 3;
    private static final int REPLY_TIMEOUT_IN_MS = 10;
    private static final int HEARTBEAT_INTERVAL_IN_S = 10;
    private static final long HEARTBEAT_INTERVAL_IN_MS = TimeUnit.SECONDS.toMillis(HEARTBEAT_INTERVAL_IN_S);
    private static final int CORR_ID = 1;
    private static final long POSITION = 1024;
    private static final int AERON_SESSION_ID = 234;
    private static final long SESSION_ID = 123;

    private ServerSocketChannel server;

    private SocketChannel client;
    private final ByteBuffer clientBuffer = ByteBuffer.allocate(1024);

    private final SenderEndPoint mockSenderEndPoint = mock(SenderEndPoint.class);
    private final ReceiverEndPoint mockReceiverEndPoint = mock(ReceiverEndPoint.class);
    private final EndPointFactory mockEndPointFactory = mock(EndPointFactory.class);
    private final GatewayPublication inboundPublication = mock(GatewayPublication.class);
    private final SessionIdStrategy mockSessionIdStrategy = mock(SessionIdStrategy.class);
    private final Header header = mock(Header.class);
    private final FakeEpochClock mockClock = new FakeEpochClock();
    private final SequenceNumberIndexReader sentSequenceNumberIndex = mock(SequenceNumberIndexReader.class);
    private final SequenceNumberIndexReader receivedSequenceNumberIndex = mock(SequenceNumberIndexReader.class);
    private final ReplayQuery replayQuery = mock(ReplayQuery.class);
    private final SessionContexts sessionContexts = mock(SessionContexts.class);
    private final GatewaySessions gatewaySessions = mock(GatewaySessions.class);
    private final GatewaySession gatewaySession = mock(GatewaySession.class);
    private final Session session = mock(Session.class);
    private final SoloSubscription outboundSubscription = mock(SoloSubscription.class);
    private final ClusterableStreams node = mock(ClusterableStreams.class);

    @SuppressWarnings("unchecked")
    private final ArgumentCaptor<List<SessionInfo>> sessionCaptor = ArgumentCaptor.forClass(List.class);

    private final EngineConfiguration engineConfiguration = new EngineConfiguration()
        .bindTo(FRAMER_ADDRESS.getHostName(), FRAMER_ADDRESS.getPort())
        .replyTimeoutInMs(REPLY_TIMEOUT_IN_MS);

    private Framer framer;

    private final ArgumentCaptor<Long> connectionId = ArgumentCaptor.forClass(Long.class);
    private final ErrorHandler errorHandler = mock(ErrorHandler.class);

    @Before
    @SuppressWarnings("unchecked")
    public void setUp() throws IOException
    {
        server = ServerSocketChannel.open().bind(TEST_ADDRESS);
        server.configureBlocking(false);

        clientBuffer.putInt(10, 5);

        when(mockEndPointFactory.receiverEndPoint(
            any(), connectionId.capture(), anyLong(), anyInt(), any(),
            eq(sentSequenceNumberIndex), eq(receivedSequenceNumberIndex), any(), any()))
            .thenReturn(mockReceiverEndPoint);

        when(mockEndPointFactory.senderEndPoint(any(), anyLong(), anyInt(), any()))
            .thenReturn(mockSenderEndPoint);

        when(mockReceiverEndPoint.connectionId()).then((inv) -> connectionId.getValue());

        when(mockSenderEndPoint.connectionId()).then((inv) -> connectionId.getValue());

        when(mockReceiverEndPoint.libraryId()).thenReturn(LIBRARY_ID);

        isLeader(true);

        framer = new Framer(
            mockClock,
            mock(Timer.class),
            mock(Timer.class),
            engineConfiguration,
            mockEndPointFactory,
            mock(ClusterableSubscription.class),
            outboundSubscription,
            mock(ClusterableSubscription.class),
            mock(Subscription.class),
            mock(QueuedPipe.class),
            mockSessionIdStrategy,
            sessionContexts,
            sentSequenceNumberIndex,
            receivedSequenceNumberIndex,
            gatewaySessions,
            replayQuery,
            errorHandler,
            mock(GatewayPublication.class),
            mock(GatewayPublication.class),
            node,
            mock(EngineDescriptorStore.class),
            new LongHashSet(SessionContexts.MISSING_SESSION_ID),
            inboundPublication,
            DEFAULT_NAME_PREFIX,
            mock(CompletionPosition.class),
            mock(CompletionPosition.class),
            mock(CompletionPosition.class));

        when(sessionContexts.onLogon(any())).thenReturn(
            new SessionContext(SESSION_ID, SessionContext.UNKNOWN_SEQUENCE_INDEX, sessionContexts, 0));
    }

    private void isLeader(final boolean value)
    {
        when(node.isLeader()).thenReturn(value);
    }

    @After
    public void tearDown() throws IOException
    {
        framer.onClose();
        server.close();
        if (client != null)
        {
            client.close();
        }
    }

    @Test
    public void shouldListenOnSpecifiedPort() throws IOException
    {
        aClientConnects();

        assertTrue("Client has failed to connect", client.finishConnect());
    }

    @Test
    public void shouldCreateEndPointWhenClientConnects() throws Exception
    {
        aClientConnects();

        framer.doWork();

        verifyEndpointsCreated();
    }

    @Test
    public void shouldPassDataToEndPointWhenSent() throws Exception
    {
        aClientConnects();
        framer.doWork();

        aClientSendsData();

        assertEventuallyTrue("Receiver end point never polled",
            () ->
            {
                doWork();
                verify(mockReceiverEndPoint).pollForData();
            });
    }

    @Test
    public void shouldCloseSocketUponDisconnect() throws Exception
    {
        aClientConnects();
        framer.doWork();

        framer.onDisconnect(LIBRARY_ID, connectionId.getValue(), APPLICATION_DISCONNECT);
        framer.doWork();

        verifyEndPointsDisconnected(APPLICATION_DISCONNECT);
    }

    @Test
    public void shouldConnectToAddress() throws Exception
    {
        initiateConnection();
    }

    @Test
    public void shouldNotConnectIfLibraryUnknown() throws Exception
    {
        onInitiateConnection();

        framer.doWork();

        assertNull("Sender has connected to server", server.accept());
        verifyErrorPublished(UNKNOWN_LIBRARY);
    }

    @Test
    public void shouldNotifyLibraryOfInitiatedConnection() throws Exception
    {
        initiateConnection();

        framer.doWork();

        notifyLibraryOfConnection();
    }

    @Test
    public void shouldReplyWithSocketConnectionError() throws Exception
    {
        server.close();

        libraryConnects();

        assertEquals(CONTINUE, onInitiateConnection());

        assertEventuallyTrue("Never sends UNABLE_TO_CONNECT message",
            () ->
            {
                doWork();
                verifyErrorPublished(UNABLE_TO_CONNECT);
            });
    }

    private void doWork()
    {
        try
        {
            framer.doWork();
        }
        catch (Exception e)
        {
            LangUtil.rethrowUnchecked(e);
        }
    }

    @Test
    public void shouldIdentifyDuplicateInitiatedSessions() throws Exception
    {
        initiateConnection();

        framer.doWork();

        notifyLibraryOfConnection();

        when(sessionContexts.onLogon(any())).thenReturn(SessionContexts.DUPLICATE_SESSION);

        initiateConnection();

        verifyErrorPublished(DUPLICATE_SESSION);
    }

    @Test
    public void shouldAcquireInitiatedClientsWhenLibraryDisconnects() throws Exception
    {
        initiateConnection();

        timeoutLibrary();

        framer.doWork();

        verifySessionsAcquired(ACTIVE);
        verifyLibraryTimeout();
    }

    @Test
    public void shouldAcquireAcceptedClientsWhenLibraryDisconnects() throws Exception
    {
        libraryHasAcceptedClient();

        timeoutLibrary();

        framer.doWork();

        verifySessionsAcquired(ACTIVE);
        verifyLibraryTimeout();
    }

    @Test
    public void shouldAcquireAcceptedClientsWhenLibraryDisconnectsAndIndexerCaughtUp() throws Exception
    {
        sentIndexedToPosition(-100L);

        libraryHasAcceptedClient();

        timeoutLibrary();

        framer.doWork();

        verifySessionsAcquired(ACTIVE, never());

        sentIndexedToPosition(100L);

        framer.doWork();

        verifySessionsAcquired(ACTIVE);
    }

    @Test
    public void shouldRetryNotifyingLibraryOfInitiateWhenBackPressured() throws Exception
    {
        backPressureFirstSaveAttempts();

        libraryConnects();

        assertEquals(CONTINUE, onInitiateConnection());

        // Requires 4 steps to complete
        framer.doWork();
        framer.doWork();
        framer.doWork();
        framer.doWork();

        notifyLibraryOfConnection(times(2));
    }

    @Test
    public void shouldWaitForSequenceNumberIndexingProcessToUpdate() throws Exception
    {
        setupHeader();

        sentIndexedToPosition(0, POSITION + 1);

        libraryConnects();

        initiateConnection();

        framer.doWork();
        framer.doWork();

        notifyLibraryOfConnection(times(1));
    }

    private void setupHeader()
    {
        when(header.sessionId()).thenReturn(AERON_SESSION_ID);
        when(header.position()).thenReturn(POSITION);
    }

    @Test
    public void shouldManageGatewaySessions() throws Exception
    {
        openSocket();

        framer.doWork();

        verifyEndpointsCreated();

        verifySessionsAcquired(CONNECTED);
    }

    @Test
    public void shouldNotifyLibraryOfAuthenticatedGatewaySessions() throws Exception
    {
        shouldManageGatewaySessions();

        givenAGatewayToManage();

        libraryConnects();

        verifyLogonSaved(times(1), LogonStatus.LIBRARY_NOTIFICATION);
    }

    @Test
    public void shouldRetryNotifyingLibraryOfAuthenticatedGatewaySessionsWhenBackPressured() throws Exception
    {
        shouldManageGatewaySessions();

        givenAGatewayToManage();

        backPressureSaveLogon();

        assertEquals(ABORT, onLibraryConnect());

        libraryConnects();

        verifyLogonSaved(times(2), LogonStatus.LIBRARY_NOTIFICATION);
    }

    @Test
    public void shouldAcquireInitiatedClientsUponReleased() throws Exception
    {
        initiateConnection();

        releaseConnection(CONTINUE);

        verifySessionsAcquired(ACTIVE);
    }

    @Test
    public void shouldRetryAcquiringInitiatedClientsUponReleasedWhenBackPressured() throws Exception
    {
        initiateConnection();

        when(inboundPublication.saveReleaseSessionReply(LIBRARY_ID, OK, CORR_ID))
            .thenReturn(BACK_PRESSURED, POSITION);

        releaseConnection(ABORT);

        releaseConnection(CONTINUE);

        verifySessionsAcquired(ACTIVE);
    }

    @Test
    public void shouldDisconnectConnectionsToFollowers() throws Exception
    {
        isLeader(false);

        openSocket();

        framer.doWork();

        verifyClientDisconnected();
        verify(errorHandler).onError(any(IllegalStateException.class));
    }

    @Test
    public void shouldHandoverSessionToLibraryUponRequest() throws IOException
    {
        aClientConnects();

        handoverSessionToLibrary();
    }

    @Test
    public void shouldHandoverSessionToLibraryUponRequestWhenBackPressured() throws IOException
    {
        when(inboundPublication.saveManageConnection(
            anyLong(),
            anyLong(),
            any(),
            anyInt(),
            any(),
            anyInt(),
            anyInt(),
            any(),
            anyInt(),
            anyLong(),
            anyInt()))
            .thenReturn(BACK_PRESSURED, POSITION);

        aClientConnects();

        sessionIsActive();

        assertEquals(ABORT, onRequestSession());

        assertEquals(CONTINUE, onRequestSession());

        verify(inboundPublication, times(2)).saveManageConnection(
            anyLong(),
            anyLong(),
            any(),
            eq(LIBRARY_ID),
            any(),
            anyInt(),
            anyInt(),
            any(),
            anyInt(),
            anyLong(),
            anyInt());

        saveRequestSessionReply();

        neverSavesUnknownSession();
    }

    private void neverSavesUnknownSession()
    {
        verify(inboundPublication, never())
            .saveRequestSessionReply(LIBRARY_ID, SessionReplyStatus.UNKNOWN_SESSION, CORR_ID);
    }

    @Test
    public void shouldNotifyLibraryOfControlledSessionsUponDuplicateConnect() throws IOException
    {
        aClientConnects();

        handoverSessionToLibrary();

        duplicateLibraryConnect();

        verifyLibraryControlNotified(Matchers.contains(gatewaySession));
    }

    @Test
    public void shouldNotifyLibraryOnlyOfControlledSessionsUponDuplicateConnect() throws IOException
    {
        aClientConnects();

        duplicateLibraryConnect();

        verifyLibraryControlNotified(hasSize(0));
    }

    @Test
    public void shouldNotNotifyLibraryOfControlledSessionsUponDuplicateConnectAfterTimeout() throws Exception
    {
        aClientConnects();

        handoverSessionToLibrary();

        timeoutLibrary();

        framer.doWork();

        reset(inboundPublication);

        duplicateLibraryConnect();

        saveControlNotification(never());
    }

    @Test
    public void shouldNotNotifyLibraryOfControlledSessionsUponHeartbeatAfterTimeout() throws Exception
    {
        // If a library has timed out we should notify it that it has no sessions
        aClientConnects();

        handoverSessionToLibrary();

        timeoutLibrary();

        framer.doWork();

        reset(inboundPublication);

        framer.onApplicationHeartbeat(LIBRARY_ID, AERON_SESSION_ID);

        verifyLibraryControlNotified(hasSize(0));
    }

    private void duplicateLibraryConnect()
    {
        framer.onLibraryConnect(LIBRARY_ID, CORR_ID + 1, AERON_SESSION_ID);
    }

    private void verifyLibraryControlNotified(final Matcher<? super Collection<?>> sessionMatcher)
    {
        verify(inboundPublication).saveApplicationHeartbeat(LIBRARY_ID);
        saveControlNotification(times(1));

        final List<SessionInfo> sessions = sessionCaptor.getValue();
        assertThat(sessions, sessionMatcher);
    }

    private void saveControlNotification(final VerificationMode times)
    {
        verify(inboundPublication, times).saveControlNotification(eq(LIBRARY_ID), sessionCaptor.capture());
    }

    private void verifyClientDisconnected()
    {
        final int bytesToSend = 1;
        final ByteBuffer buffer = ByteBuffer.allocateDirect(bytesToSend);
        while (buffer.hasRemaining())
        {
            try
            {
                client.write(buffer);
            }
            catch (final IOException ignore)
            {
                return;
            }
        }
    }

    private void handoverSessionToLibrary()
    {
        sessionIsActive();

        assertEquals(CONTINUE, onRequestSession());

        saveRequestSessionReply();
    }

    private long saveRequestSessionReply()
    {
        return verify(inboundPublication).saveRequestSessionReply(LIBRARY_ID, OK, CORR_ID);
    }

    private Action onRequestSession()
    {
        return framer.onRequestSession(LIBRARY_ID, SESSION_ID, CORR_ID, NO_MESSAGE_REPLAY);
    }

    private void sessionIsActive()
    {
        when(gatewaySessions.releaseBySessionId(SESSION_ID)).thenReturn(gatewaySession, (GatewaySession)null);
        when(gatewaySession.session()).thenReturn(session);
        when(gatewaySession.heartbeatIntervalInS()).thenReturn(HEARTBEAT_INTERVAL_IN_S);
        when(session.isActive()).thenReturn(true);
    }

    private void verifyErrorPublished(final GatewayError error)
    {
        verify(inboundPublication).saveError(eq(error), eq(LIBRARY_ID), anyLong(), anyString());
    }

    private void releaseConnection(final Action expectedResult)
    {
        assertEquals(expectedResult, framer.onReleaseSession(
            LIBRARY_ID, connectionId.getValue(), CORR_ID, ACTIVE, HEARTBEAT_INTERVAL_IN_MS, 0, 0, "", "", header));
    }

    private Action onLibraryConnect()
    {
        return framer.onLibraryConnect(LIBRARY_ID, CORR_ID, AERON_SESSION_ID);
    }

    private void givenAGatewayToManage()
    {
        when(gatewaySession.connectionId()).thenReturn(connectionId.getValue());
        when(gatewaySession.sessionKey()).thenReturn(mock(CompositeKey.class));
        when(gatewaySessions.sessions()).thenReturn(singletonList(gatewaySession));
    }

    private void backPressureFirstSaveAttempts()
    {
        when(inboundPublication.saveManageConnection(
            anyLong(),
            anyLong(),
            anyString(),
            eq(LIBRARY_ID),
            eq(INITIATOR),
            anyInt(),
            anyInt(),
            any(),
            anyInt(),
            anyLong(),
            anyInt()))
            .thenReturn(BACK_PRESSURED, POSITION);
        backPressureSaveLogon();
    }

    private void backPressureSaveLogon()
    {
        when(inboundPublication.saveLogon(
            eq(LIBRARY_ID), anyLong(), anyLong(),
            anyInt(), anyInt(),
            any(), any(), any(), any(),
            any(), any(), any()))
            .thenReturn(BACK_PRESSURED, POSITION);
    }

    private void verifySessionsAcquired(final SessionState state)
    {
        verifySessionsAcquired(state, times(1));
    }

    private void verifySessionsAcquired(final SessionState state, final VerificationMode times)
    {
        verify(gatewaySessions, times).acquire(
            any(),
            eq(state),
            eq(HEARTBEAT_INTERVAL_IN_S),
            anyInt(),
            anyInt(),
            any(),
            any()
        );
    }

    private void verifyEndPointsDisconnected(final DisconnectReason reason)
    {
        verify(mockReceiverEndPoint).close(reason);
        verify(mockSenderEndPoint).close();
    }

    private void timeoutLibrary()
    {
        mockClock.advanceMilliSeconds(REPLY_TIMEOUT_IN_MS * 2);
    }

    private void libraryConnects()
    {
        assertEquals(Action.CONTINUE, onLibraryConnect());
    }

    private void initiateConnection() throws Exception
    {
        libraryConnects();

        assertEquals(CONTINUE, onInitiateConnection());

        do
        {
            framer.doWork();
        }
        while (server.accept() == null);

        assertNotNull("Connection not completed yet", connectionId.getValue());
    }

    private Action onInitiateConnection()
    {
        return framer.onInitiateConnection(
            LIBRARY_ID, TEST_ADDRESS.getPort(), TEST_ADDRESS.getHostName(), "LEH_LZJ02", null, null, "CCG",
            TRANSIENT, AUTOMATIC_INITIAL_SEQUENCE_NUMBER, "", "", HEARTBEAT_INTERVAL_IN_S, CORR_ID, header);
    }

    private void aClientConnects() throws IOException
    {
        libraryConnects();

        openSocket();
    }

    private void openSocket() throws IOException
    {
        client = SocketChannel.open(FRAMER_ADDRESS);
    }

    private void notifyLibraryOfConnection()
    {
        notifyLibraryOfConnection(times(1));
    }

    private void notifyLibraryOfConnection(final VerificationMode times)
    {
        verify(inboundPublication, times).saveManageConnection(
            eq(connectionId.getValue()),
            anyLong(),
            anyString(),
            eq(LIBRARY_ID),
            eq(INITIATOR),
            anyInt(),
            anyInt(),
            any(),
            anyInt(),
            anyLong(),
            anyInt());
        verifyLogonSaved(times, LogonStatus.NEW);
    }

    private void verifyLogonSaved(final VerificationMode times, final LogonStatus status)
    {
        verify(inboundPublication, times).saveLogon(
            eq(LIBRARY_ID), eq(connectionId.getValue()), anyLong(),
            anyInt(), anyInt(),
            any(), any(), any(), any(),
            any(), any(), eq(status));
    }

    private void aClientSendsData() throws IOException
    {
        clientBuffer.position(0);
        assertEquals("Has written bytes", clientBuffer.remaining(), client.write(clientBuffer));
    }

    private void verifyEndpointsCreated() throws IOException
    {
        verify(mockEndPointFactory).receiverEndPoint(
            notNull(), anyLong(), anyLong(), eq(ENGINE_LIBRARY_ID), eq(framer),
            eq(sentSequenceNumberIndex), eq(receivedSequenceNumberIndex), any(), any());

        verify(mockEndPointFactory).senderEndPoint(
            notNull(), anyLong(), eq(ENGINE_LIBRARY_ID), eq(framer));
    }

    private void verifyLibraryTimeout()
    {
        verify(inboundPublication).saveLibraryTimeout(LIBRARY_ID, 0);
    }

    private void libraryHasAcceptedClient() throws IOException
    {
        aClientConnects();
        sessionIsActive();
        assertEquals(CONTINUE, onRequestSession());
        when(receivedSequenceNumberIndex.lastKnownSequenceNumber(anyInt())).thenReturn(1);
    }

    private void sentIndexedToPosition(final long position, final Long ... positions)
    {
        when(sentSequenceNumberIndex.indexedPosition(anyInt())).thenReturn(position, positions);
    }
}