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
package uk.co.real_logic.fix_gateway.library;

import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.stubbing.Answer;
import org.mockito.stubbing.OngoingStubbing;
import uk.co.real_logic.fix_gateway.FixCounters;
import uk.co.real_logic.fix_gateway.engine.framer.FakeEpochClock;
import uk.co.real_logic.fix_gateway.messages.ControlNotificationDecoder.SessionsDecoder;
import uk.co.real_logic.fix_gateway.messages.LogonStatus;
import uk.co.real_logic.fix_gateway.protocol.GatewayPublication;
import uk.co.real_logic.fix_gateway.replication.ClusterableSubscription;
import uk.co.real_logic.fix_gateway.session.Session;
import uk.co.real_logic.fix_gateway.timing.LibraryTimers;

import java.util.List;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;

import static io.aeron.CommonContext.IPC_CHANNEL;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.fix_gateway.CommonConfiguration.DEFAULT_REPLY_TIMEOUT_IN_MS;
import static uk.co.real_logic.fix_gateway.engine.FixEngine.ENGINE_LIBRARY_ID;
import static uk.co.real_logic.fix_gateway.messages.ConnectionType.ACCEPTOR;
import static uk.co.real_logic.fix_gateway.messages.SessionState.ACTIVE;

public class LibraryPollerTest
{
    private static final long CONNECTION_ID = 2;
    private static final long SESSION_ID = 3;
    private static final long OTHER_CONNECTION_ID = 4;
    private static final long OTHER_SESSION_ID = 5;

    private static final int LAST_SENT_SEQUENCE_NUMBER = 1;
    private static final int LAST_RECEIVED_SEQUENCE_NUMBER = 1;
    private static final int HEARTBEAT_INTERVAL_IN_S = 1;
    private static final int REPLY_TO_ID = 0;

    private static final String FIRST_CHANNEL = "1";
    private static final String LEADER_CHANNEL = "2";
    private static final List<String> CLUSTER_CHANNELS = asList(FIRST_CHANNEL, LEADER_CHANNEL, "3");

    private ArgumentCaptor<Session> session = ArgumentCaptor.forClass(Session.class);
    private LibraryConnectHandler connectHandler = mock(LibraryConnectHandler.class);
    private SessionHandler sessionHandler = mock(SessionHandler.class);
    private SessionAcquireHandler sessionAcquireHandler = mock(SessionAcquireHandler.class);
    private GatewayPublication outboundPublication = mock(GatewayPublication.class);
    private ClusterableSubscription inboundSubscription = mock(ClusterableSubscription.class);
    private LibraryTransport transport = mock(LibraryTransport.class);
    private FixCounters counters = mock(FixCounters.class);
    private FixLibrary fixLibrary = mock(FixLibrary.class);
    private UnsafeBuffer address = new UnsafeBuffer(new byte[1024]);
    private int addressLength = address.putStringUtf8(0, "localhost:1234");
    private FakeEpochClock clock = new FakeEpochClock();

    private LibraryPoller library;

    @Before
    public void setUp()
    {
        when(transport.outboundPublication()).thenReturn(outboundPublication);
        when(transport.inboundSubscription()).thenReturn(inboundSubscription);

        when(counters.receivedMsgSeqNo(anyLong())).thenReturn(mock(AtomicCounter.class));
        when(counters.sentMsgSeqNo(anyLong())).thenReturn(mock(AtomicCounter.class));

        when(sessionAcquireHandler.onSessionAcquired(session.capture())).thenReturn(sessionHandler);
    }

    @Test
    public void shouldNotifyClientOfSessionTimeouts()
    {
        connect();

        manageConnection(CONNECTION_ID, SESSION_ID);

        library.onControlNotification(libraryId(), noSessionIds());

        verify(sessionHandler).onTimeout(libraryId(), SESSION_ID);
    }

    @Test
    public void shouldNotifyClientsOfRelevantSessionTimeouts()
    {
        connect();

        manageConnection(CONNECTION_ID, SESSION_ID);
        manageConnection(OTHER_CONNECTION_ID, OTHER_SESSION_ID);

        library.onControlNotification(libraryId(), hasOtherSessionId());

        verify(sessionHandler).onTimeout(libraryId(), SESSION_ID);
    }

    @Test
    public void shouldDisconnectAfterTimeout()
    {
        connect();

        disconnectDueToTimeout();
    }

    @Test
    public void shouldReconnectAfterTimeoutOnHeartbeat()
    {
        shouldDisconnectAfterTimeout();

        reconnectAfterTimeout();
    }

    @Test
    public void shouldRepeatedlyReconnectAfterTimeoutOnHeartbeat()
    {
        shouldReconnectAfterTimeoutOnHeartbeat();

        disconnectDueToTimeout();

        reconnectAfterTimeout();

        disconnectDueToTimeout();

        reconnectAfterTimeout();

        disconnectDueToTimeout();

        reconnectAfterTimeout();
    }

    @Test
    public void shouldAttemptAnotherEngineWhenNotLeader()
    {
        shouldReplyToOnNotLeaderWith(this::libraryId, this::connectCorrelationId, FIRST_CHANNEL, LEADER_CHANNEL);
    }

    @Test
    public void shouldNotAttemptAnotherEngineWithDifferentLibraryId()
    {
        shouldReplyToOnNotLeaderWith(() -> ENGINE_LIBRARY_ID, this::connectCorrelationId, FIRST_CHANNEL);
    }

    @Test
    public void shouldNotAttemptAnotherEngineWithDifferentConnectCorrelation()
    {
        shouldReplyToOnNotLeaderWith(this::libraryId, () -> 0, FIRST_CHANNEL);
    }

    private void disconnectDueToTimeout()
    {
        advanceBeyondReplyTimeout();

        library.poll(1);

        assertFalse("Library failed to timeout", library.isConnected());
    }

    private void reconnectAfterTimeout()
    {
        receiveOneApplicationHeartbeat();

        library.poll(1);

        assertTrue("Library still timed out", library.isConnected());
    }

    private void advanceBeyondReplyTimeout()
    {
        clock.advanceMilliSeconds(DEFAULT_REPLY_TIMEOUT_IN_MS + 1);
    }

    private int libraryId()
    {
        return library.libraryId();
    }

    private long connectCorrelationId()
    {
        return library.connectCorrelationId();
    }

    private void shouldReplyToOnNotLeaderWith(
        final IntSupplier libraryId,
        final LongSupplier connectCorrelationId,
        final String... channels)
    {
        whenPolled()
            .then(
                (inv) ->
                {
                    library.onNotLeader(libraryId.getAsInt(), connectCorrelationId.getAsLong(), LEADER_CHANNEL);
                    return 1;
                })
            .then(replyWithApplicationHeartbeat())
            .then(noReply());

        newLibraryPoller(CLUSTER_CHANNELS);

        library.connect();

        attemptToConnectTo(channels);
        verify(connectHandler).onConnect(fixLibrary);
    }

    private void attemptToConnectTo(final String... channels)
    {
        final InOrder inOrder = inOrder(transport, outboundPublication);
        for (final String channel : channels)
        {
            inOrder.verify(transport).initStreams(channel);
            inOrder.verify(transport).inboundSubscription();
            inOrder.verify(transport).outboundPublication();
            inOrder.verify(outboundPublication)
                .saveLibraryConnect(eq(libraryId()), anyLong());
        }
        verifyNoMoreInteractions(transport);
    }

    private void connect()
    {
        receiveOneApplicationHeartbeat();

        newLibraryPoller(singletonList(IPC_CHANNEL));

        library.connect();
    }

    private void receiveOneApplicationHeartbeat()
    {
        whenPolled()
            .then(replyWithApplicationHeartbeat())
            .then(noReply());
    }

    private Answer<Integer> replyWithApplicationHeartbeat()
    {
        return (inv) ->
        {
            library.onApplicationHeartbeat(libraryId());
            return 1;
        };
    }

    private Answer<Integer> noReply()
    {
        return inv -> 0;
    }

    private void newLibraryPoller(final List<String> libraryAeronChannels)
    {
        library = new LibraryPoller(
            new LibraryConfiguration()
                .libraryAeronChannels(libraryAeronChannels)
                .sessionAcquireHandler(sessionAcquireHandler)
                .libraryConnectHandler(connectHandler),
            new LibraryTimers(),
            counters,
            transport,
            fixLibrary,
            clock);
    }

    private OngoingStubbing<Integer> whenPolled()
    {
        return when(inboundSubscription.controlledPoll(any(), anyInt()));
    }

    private void manageConnection(final long connectionId, final long sessionId)
    {
        library.onManageConnection(
            libraryId(),
            connectionId,
            sessionId,
            ACCEPTOR,
            LAST_SENT_SEQUENCE_NUMBER,
            LAST_RECEIVED_SEQUENCE_NUMBER,
            address,
            0,
            addressLength,
            ACTIVE,
            HEARTBEAT_INTERVAL_IN_S,
            REPLY_TO_ID);

        library.onLogon(
            libraryId(),
            connectionId,
            sessionId,
            LAST_SENT_SEQUENCE_NUMBER,
            LAST_RECEIVED_SEQUENCE_NUMBER,
            LogonStatus.NEW,
            "",
            "",
            "",
            "",
            null,
            null
        );
    }

    private SessionsDecoder hasOtherSessionId()
    {
        final SessionsDecoder sessionsDecoder = mock(SessionsDecoder.class);
        when(sessionsDecoder.hasNext()).thenReturn(true, false);
        when(sessionsDecoder.sessionId()).thenReturn(OTHER_SESSION_ID);
        return sessionsDecoder;
    }

    private SessionsDecoder noSessionIds()
    {
        final SessionsDecoder sessionsDecoder = mock(SessionsDecoder.class);
        when(sessionsDecoder.hasNext()).thenReturn(false);
        return sessionsDecoder;
    }
}
