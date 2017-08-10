/*
 * Copyright 2015-2017 Real Logic Ltd.
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

import io.aeron.Subscription;
import org.agrona.concurrent.status.AtomicCounter;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.InOrder;
import org.mockito.stubbing.Answer;
import org.mockito.stubbing.OngoingStubbing;
import org.mockito.verification.VerificationMode;
import uk.co.real_logic.fix_gateway.FixCounters;
import uk.co.real_logic.fix_gateway.engine.framer.FakeEpochClock;
import uk.co.real_logic.fix_gateway.messages.ControlNotificationDecoder.SessionsDecoder;
import uk.co.real_logic.fix_gateway.messages.LogonStatus;
import uk.co.real_logic.fix_gateway.messages.SlowStatus;
import uk.co.real_logic.fix_gateway.protocol.GatewayPublication;
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
import static uk.co.real_logic.fix_gateway.LivenessDetector.SEND_INTERVAL_FRACTION;
import static uk.co.real_logic.fix_gateway.engine.FixEngine.ENGINE_LIBRARY_ID;
import static uk.co.real_logic.fix_gateway.messages.ConnectionType.ACCEPTOR;
import static uk.co.real_logic.fix_gateway.messages.SessionState.ACTIVE;

public class LibraryPollerTest
{
    private static final long CONNECTION_ID = 2;
    private static final long SESSION_ID = 3;
    private static final long OTHER_CONNECTION_ID = 4;
    private static final long OTHER_SESSION_ID = 5;
    private static final long CONNECT_ATTEMPT_TIMEOUT = DEFAULT_REPLY_TIMEOUT_IN_MS / SEND_INTERVAL_FRACTION;

    private static final int LAST_SENT_SEQUENCE_NUMBER = 1;
    private static final int LAST_RECEIVED_SEQUENCE_NUMBER = 1;
    private static final int HEARTBEAT_INTERVAL_IN_S = 1;
    private static final int REPLY_TO_ID = 0;

    private static final String FIRST_CHANNEL = "1";
    private static final String LEADER_CHANNEL = "2";
    private static final List<String> CLUSTER_CHANNELS = asList(FIRST_CHANNEL, LEADER_CHANNEL, "3");
    private static final int SEQUENCE_INDEX = 0;

    private ArgumentCaptor<Session> session = ArgumentCaptor.forClass(Session.class);
    private LibraryConnectHandler connectHandler = mock(LibraryConnectHandler.class);
    private SessionHandler sessionHandler = mock(SessionHandler.class);
    private SessionAcquireHandler sessionAcquireHandler = mock(SessionAcquireHandler.class);
    private GatewayPublication outboundPublication = mock(GatewayPublication.class);
    private Subscription inboundSubscription = mock(Subscription.class);
    private LibraryTransport transport = mock(LibraryTransport.class);
    private FixCounters counters = mock(FixCounters.class);
    private FixLibrary fixLibrary = mock(FixLibrary.class);
    private String address = "localhost:1234";
    private FakeEpochClock clock = new FakeEpochClock();

    private LibraryPoller library;

    @Before
    public void setUp()
    {
        when(transport.outboundPublication()).thenReturn(outboundPublication);
        when(transport.inboundSubscription()).thenReturn(inboundSubscription);

        when(counters.receivedMsgSeqNo(anyLong())).thenReturn(mock(AtomicCounter.class));
        when(counters.sentMsgSeqNo(anyLong())).thenReturn(mock(AtomicCounter.class));

        when(sessionAcquireHandler.onSessionAcquired(session.capture(), anyBoolean())).thenReturn(sessionHandler);
    }

    @Test
    public void shouldNotifyClientOfSessionTimeouts()
    {
        connectToSingleEngine();

        manageConnection(CONNECTION_ID, SESSION_ID);

        library.onControlNotification(libraryId(), noSessionIds());

        verify(sessionHandler).onTimeout(libraryId(), session.getValue());
    }

    @Test
    public void shouldNotifyClientsOfRelevantSessionTimeouts()
    {
        connectToSingleEngine();

        manageConnection(CONNECTION_ID, SESSION_ID);
        manageConnection(OTHER_CONNECTION_ID, OTHER_SESSION_ID);

        library.onControlNotification(libraryId(), hasOtherSessionId());

        final Session firstSession = session.getAllValues().get(0);
        verify(sessionHandler).onTimeout(libraryId(), firstSession);
    }

    @Test
    public void shouldDisconnectSingleEngineAfterTimeout()
    {
        connectToSingleEngine();

        disconnectDueToTimeout();
    }

    @Test
    public void shouldReconnectToSingleEngineAfterTimeoutOnHeartbeat()
    {
        shouldDisconnectSingleEngineAfterTimeout();

        reconnectAfterTimeout();
    }

    @Test
    public void shouldRepeatedlyReconnectToSingleEngineAfterTimeoutOnHeartbeat()
    {
        shouldReconnectToSingleEngineAfterTimeoutOnHeartbeat();

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
    public void shouldResendConnectToSameEngineWhenConnectionTimesOut()
    {
        setupAndConnectToFirstChannel();

        pollTwice();

        clock.advanceMilliSeconds(CONNECT_ATTEMPT_TIMEOUT + 1);

        poll();

        sendsLibraryConnect(times(1));

        doesNotAttemptConnectTo(LEADER_CHANNEL);
    }

    @Test
    public void shouldStopResendingConnectToSameEngineAfterHeartbeat()
    {
        shouldResendConnectToSameEngineWhenConnectionTimesOut();

        reset(outboundPublication);

        reconnectAfterTimeout();

        clock.advanceMilliSeconds(CONNECT_ATTEMPT_TIMEOUT + 1);

        pollTwice();

        sendsLibraryConnect(never());
    }

    @Test
    public void shouldAttemptNextEngineWhenEngineTimesOut()
    {
        setupAndConnectToFirstChannel();

        pollTwice();

        clock.advanceMilliSeconds(DEFAULT_REPLY_TIMEOUT_IN_MS + 1);

        poll();

        attemptToConnectTo(LEADER_CHANNEL);
    }

    @Test
    public void shouldNotAttemptNextEngineUntilEngineTimesOut()
    {
        setupAndConnectToFirstChannel();

        pollTwice();

        clock.advanceMilliSeconds(DEFAULT_REPLY_TIMEOUT_IN_MS - 1);

        pollTwice();

        doesNotAttemptConnectTo(LEADER_CHANNEL);
    }

    private void sendsLibraryConnect(final VerificationMode times)
    {
        verify(outboundPublication, times)
            .saveLibraryConnect(eq(libraryId()), anyString(), anyLong());
    }

    private void pollTwice()
    {
        poll();

        poll();
    }

    private void poll()
    {
        library.poll(1);
    }

    private void doesNotAttemptConnectTo(final String channel)
    {
        verify(transport, never()).initStreams(channel);
    }

    private void setupAndConnectToFirstChannel()
    {
        newLibraryPoller(CLUSTER_CHANNELS);

        library.startConnecting();

        poll();

        attemptToConnectTo(FIRST_CHANNEL);
    }

    private void disconnectDueToTimeout()
    {
        advanceBeyondReplyTimeout();

        pollTwice();

        assertFalse("Library failed to timeout", library.isConnected());
    }

    private void reconnectAfterTimeout()
    {
        receiveOneApplicationHeartbeat();

        pollTwice();

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

        library.startConnecting();

        pollTwice();

        poll();

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
                   .saveLibraryConnect(eq(libraryId()), anyString(), anyLong());
        }
        verifyNoMoreInteractions(transport);
        reset(outboundPublication);
    }

    private void connectToSingleEngine()
    {
        receiveOneApplicationHeartbeat();

        newLibraryPoller(singletonList(IPC_CHANNEL));

        library.startConnecting();

        pollTwice();

        assertTrue("Failed to connect", library.isConnected());
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
            new LibraryTimers(clock::time),
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
        library.onManageSession(libraryId(),
            connectionId,
            sessionId,
            LAST_SENT_SEQUENCE_NUMBER,
            LAST_RECEIVED_SEQUENCE_NUMBER,
            -1,
            LogonStatus.NEW,
            SlowStatus.NOT_SLOW,
            ACCEPTOR,
            ACTIVE,
            HEARTBEAT_INTERVAL_IN_S,
            REPLY_TO_ID,
            SEQUENCE_INDEX,
            "",
            "",
            "",
            "",
            "",
            "",
            address);
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
