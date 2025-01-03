/*
 * Copyright 2015-2025 Real Logic Limited, Adaptive Financial Consulting Ltd.
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
package uk.co.real_logic.artio.engine.framer;

import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.LangUtil;
import org.agrona.concurrent.EpochNanoClock;
import org.agrona.concurrent.status.AtomicCounter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.InOrder;
import org.mockito.Mockito;
import org.mockito.verification.VerificationMode;
import uk.co.real_logic.artio.TestFixtures;
import uk.co.real_logic.artio.decoder.LogonDecoder;
import uk.co.real_logic.artio.dictionary.FixDictionary;
import uk.co.real_logic.artio.engine.FixEngine;
import uk.co.real_logic.artio.messages.DisconnectReason;
import uk.co.real_logic.artio.messages.MessageStatus;
import uk.co.real_logic.artio.messages.SessionState;
import uk.co.real_logic.artio.protocol.GatewayPublication;
import uk.co.real_logic.artio.session.CompositeKey;
import uk.co.real_logic.artio.session.InternalSession;
import uk.co.real_logic.artio.session.SessionIdStrategy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.HashMap;
import java.util.function.ToIntFunction;
import java.util.stream.IntStream;

import static io.aeron.Publication.BACK_PRESSURED;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.artio.dictionary.ExampleDictionary.TAG_SPECIFIED_OUT_OF_REQUIRED_ORDER_MESSAGE_BYTES;
import static uk.co.real_logic.artio.engine.EngineConfiguration.NO_THROTTLE_WINDOW;
import static uk.co.real_logic.artio.messages.DisconnectReason.*;
import static uk.co.real_logic.artio.messages.MessageStatus.*;
import static uk.co.real_logic.artio.session.Session.UNKNOWN;
import static uk.co.real_logic.artio.util.TestMessages.*;

class ReceiverEndPointTest
{
    private static final long MESSAGE_TYPE = 'D';
    private static final long CONNECTION_ID = 20L;
    private static final long SESSION_ID = 4L;
    private static final int LIBRARY_ID = FixEngine.ENGINE_LIBRARY_ID;
    private static final long POSITION = 1024L;
    private static final int BUFFER_SIZE = 16 * 1024;
    private static final int SEQUENCE_INDEX = 0;
    private static final int LOGON_LEN = LOGON_MESSAGE.length;
    private static final int OUT_OF_REQUIRED_ORDER_MSG_LEN = TAG_SPECIFIED_OUT_OF_REQUIRED_ORDER_MESSAGE_BYTES.length;
    private static final long TIMESTAMP = 1000L;
    // private static final long BACKPRESSURED_TIMESTAMP = 2000L;

    private final AcceptorLogonResult pendingAuth = createSuccessfulPendingAuth();
    private final AcceptorLogonResult backpressuredPendingAuth = createBackpressuredPendingAuth();
    private final TcpChannel mockChannel = mock(TcpChannel.class);
    private final GatewayPublication publication = mock(GatewayPublication.class);
    private final FixContexts mockFixContexts = mock(FixContexts.class);
    private final AtomicCounter messagesRead = mock(AtomicCounter.class);
    private final ErrorHandler errorHandler = mock(ErrorHandler.class);
    private final Framer framer = mock(Framer.class);
    private final FixGatewaySession gatewaySession = mock(FixGatewaySession.class);
    private final InternalSession session = mock(InternalSession.class);
    private final FixGatewaySessions mockGatewaySessions = mock(FixGatewaySessions.class);
    private final CompositeKey sessionKey = SessionIdStrategy
        .senderAndTarget()
        .onInitiateLogon("ACCEPTOR", "", "", "INIATOR", "", "");
    private FixReceiverEndPoint endPoint;
    private final EpochNanoClock mockClock = mock(EpochNanoClock.class);

    private AcceptorLogonResult createSuccessfulPendingAuth()
    {
        final AcceptorLogonResult pendingAcceptorLogon = mock(AcceptorLogonResult.class);
        when(pendingAcceptorLogon.poll()).thenReturn(false, true);
        when(pendingAcceptorLogon.isAccepted()).thenReturn(true);
        return pendingAcceptorLogon;
    }

    private AcceptorLogonResult createBackpressuredPendingAuth()
    {
        final AcceptorLogonResult pendingAcceptorLogon = mock(AcceptorLogonResult.class);
        when(pendingAcceptorLogon.poll()).thenReturn(true);
        when(pendingAcceptorLogon.isAccepted()).thenReturn(true);
        return pendingAcceptorLogon;
    }

    @BeforeEach
    void setUp()
    {
        givenReceiverEndPoint(SESSION_ID);
        when(gatewaySession.session()).thenReturn(session);
        when(gatewaySession.sessionKey()).thenReturn(sessionKey);
        when(gatewaySession.sessionId()).thenReturn(SESSION_ID);
        when(session.state()).thenReturn(SessionState.CONNECTED);
        givenAnAuthenticatedReceiverEndPoint();

        doAnswer(
            (inv) ->
            {
                ((Continuation)inv.getArguments()[0]).attemptToAction();
                return null;
            }).when(framer).schedule(any(Continuation.class));

        when(mockClock.nanoTime()).thenReturn(TIMESTAMP);
    }

    private void givenLogonResult(final AcceptorLogonResult logonResult)
    {
        when(mockGatewaySessions.authenticate(
            any(),
            anyLong(),
            eq(gatewaySession),
            any(),
            any(),
            eq(framer),
            any(),
            any()))
            .thenReturn(logonResult);
    }

    private void givenAnUnauthenticatedReceiverEndPoint()
    {
        givenReceiverEndPoint(UNKNOWN);
    }

    private void givenAnAuthenticatedReceiverEndPoint()
    {
        givenReceiverEndPoint(SESSION_ID);
    }

    private void givenReceiverEndPoint(final long sessionId)
    {
        endPoint = new FixReceiverEndPoint(
            mockChannel, BUFFER_SIZE, publication,
            CONNECTION_ID, sessionId, SEQUENCE_INDEX + 1, mockFixContexts,
            messagesRead, framer, errorHandler, LIBRARY_ID,
            mockGatewaySessions,
            mockClock,
            new AcceptorFixDictionaryLookup(FixDictionary.of(FixDictionary.findDefault()), new HashMap<>()),
            new FixReceiverEndPoint.FixReceiverEndPointFormatters(),
            NO_THROTTLE_WINDOW,
            NO_THROTTLE_WINDOW,
            false);
        endPoint.gatewaySession(gatewaySession);
    }

    private void theEndpointReceivesALogon()
    {
        theEndpointReceives(LOGON_MESSAGE, 0, LOGON_MESSAGE.length);
    }

    @Test
    void shouldNotifyDuplicateSession()
    {
        givenAnUnauthenticatedReceiverEndPoint();
        givenADuplicateSession();

        theEndpointReceivesALogon();

        polls(LOGON_MESSAGE.length);
        pollWithNoData(1);

        verifyDuplicateSession(times(1));
    }

    @Test
    void shouldDisconnectWhenFirstMessageIsNotALogon()
    {
        givenAnUnauthenticatedReceiverEndPoint();
        givenLogonResult(pendingAuth);

        theEndpointReceivesACompleteMessage();

        polls(MSG_LEN);
        pollWithNoData(0);

        verify(publication).saveDisconnect(anyInt(), anyLong(), eq(DisconnectReason.FIRST_MESSAGE_NOT_LOGON));
    }

    @Test
    void shouldFrameValidFixMessage()
    {
        theEndpointReceivesACompleteMessage();

        polls(MSG_LEN);

        savesAFramedMessage();

        sessionReceivesOneMessage();
    }

    static IntStream overflowRange()
    {
        return IntStream.range(1, 10);
    }

    @ParameterizedTest
    @MethodSource("overflowRange")
    void shouldDetectOversizedFixMessage(final int overflow)
    {
        theEndpointReceivesTheStartOfAnOversizedMessage(overflow);

        polls(BUFFER_SIZE);

        savesInvalidMessage(BUFFER_SIZE, times(1), INVALID, TIMESTAMP);
        verifyError(times(1));
        verifyDisconnected(EXCEPTION);

        sessionReceivesNoMessages();
    }

    @ParameterizedTest
    @MethodSource("overflowRange")
    void shouldHandleBackPressureWhenSavingOversizedMessage(final int overflow)
    {
        theEndpointReceivesTheStartOfAnOversizedMessage(overflow);

        firstSaveAttemptIsBackPressured();
        polls(-BUFFER_SIZE);
        assertTrue(endPoint.retryFrameMessages());

        savesInvalidMessage(BUFFER_SIZE, times(2), INVALID, TIMESTAMP);
        verifyError(times(1));
        verifyDisconnected(EXCEPTION);

        sessionReceivesNoMessages();
    }

    @Test
    void shouldFrameValidFixMessageWhenBackpressuredSelectionKeyCase()
    {
        firstSaveAttemptIsBackPressured();

        theEndpointReceivesACompleteMessage();
        polls(-MSG_LEN);

        assertTrue(endPoint.retryFrameMessages());

        savesFramedMessages(2, OK, MSG_LEN);

        sessionReceivesOneMessage();
    }

    @Test
    void shouldFrameValidFixMessageWhenBackpressuredPollingCase()
    {
        firstSaveAttemptIsBackPressured();

        theEndpointReceivesACompleteMessage();
        polls(-MSG_LEN);

        theEndpointReceivesNothing();
        polls(0);

        savesFramedMessages(2, OK, MSG_LEN);

        sessionReceivesOneMessage();
    }

    @Test
    void shouldIgnoreMessageWithBodyLengthTooShort()
    {
        final int length = INVALID_LENGTH_MESSAGE.length;
        theEndpointReceives(INVALID_LENGTH_MESSAGE, 0, length);

        endPoint.poll();

        savesInvalidMessage(length, times(1), INVALID_BODYLENGTH, TIMESTAMP);
        verifyNoError();
        verifyNotDisconnected();
        sessionReceivesNoMessages();

        shouldFrameValidFixMessage();
    }

    @Test
    void shouldOnlyFrameCompleteFixMessage()
    {
        theEndpointReceivesAnIncompleteMessage();

        endPoint.poll();

        nothingMoreSaved();
    }

    @Test
    void shouldFrameSplitFixMessage()
    {
        theEndpointReceivesAnIncompleteMessage();
        endPoint.poll();

        theEndpointReceivesTheRestOfTheMessage();
        endPoint.poll();

        savesAFramedMessage();

        sessionReceivesOneMessage();
    }

    @Test
    void shouldFrameTwoCompleteFixMessagesInOnePacket()
    {
        theEndpointReceivesTwoCompleteMessages();

        endPoint.poll();

        savesTwoFramedMessages(1);

        sessionReceivesTwoMessages();
    }

    @Test
    void shouldFrameOneCompleteMessageWhenTheSecondMessageIsIncomplete()
    {
        theEndpointReceivesACompleteAndAnIncompleteMessage();

        endPoint.poll();

        savesAFramedMessage();

        sessionReceivesOneMessage();
    }

    @Test
    void shouldFrameSecondSplitMessage()
    {
        theEndpointReceivesACompleteAndAnIncompleteMessage();
        endPoint.poll();

        theEndpointReceivesTheRestOfTheMessage();
        endPoint.poll();

        savesFramedMessages(2, OK, MSG_LEN);

        sessionReceivesTwoMessageAtBufferStart();
    }

    @Test
    void aClosedSocketSavesItsDisconnect() throws IOException
    {
        theChannelIsClosedByException();

        endPoint.poll();

        verify(mockFixContexts).onDisconnect(anyLong());
        verifyDisconnected(REMOTE_DISCONNECT);
    }

    @Test
    void anUnreadableSocketDisconnectsItsSession() throws IOException
    {
        theChannelIsClosed();

        endPoint.poll();

        verifyDisconnected(REMOTE_DISCONNECT);
    }

    @Test
    void invalidChecksumMessageRecorded()
    {
        theEndpointReceivesAMessageWithInvalidChecksum();

        endPoint.poll();

        // Test for bug where invalid message re-saved.
        pollWithNoData(0);

        savesInvalidChecksumMessage(times(1));
        nothingMoreSaved();
    }

    @Test
    void invalidChecksumMessageRecordedWhenBackpressuredPolling()
    {
        firstSaveAttemptIsBackPressured();

        theEndpointReceivesAMessageWithInvalidChecksum();

        assertEquals(-INVALID_CHECKSUM_LEN, endPoint.poll());

        pollWithNoData(0);

        savesInvalidChecksumMessage(times(2));
    }

    @Test
    void invalidChecksumMessageRecordedWhenBackpressuredSelectionKey()
    {
        firstSaveAttemptIsBackPressured();

        theEndpointReceivesAMessageWithInvalidChecksum();

        assertEquals(-INVALID_CHECKSUM_LEN, endPoint.poll());

        assertTrue(endPoint.retryFrameMessages());

        savesInvalidChecksumMessage(times(2));
    }

    @Test
    void fieldOutOfOrderMessageRecordedOnce()
    {
        theEndpointReceivesAnOutOfOrderMessage(OUT_OF_REQUIRED_ORDER_MSG_LEN);

        // Test for bug where invalid message re-saved.
        pollWithNoData(0);

        savesInvalidOutOfRequiredMessage(times(1), TIMESTAMP);
        nothingMoreSaved();
        verifyNoError();
    }

    @Test
    void fieldOutOfOrderMessageRecordedWhenBackpressured()
    {
        firstSaveAttemptIsBackPressured();
        theEndpointReceivesAnOutOfOrderMessage(-OUT_OF_REQUIRED_ORDER_MSG_LEN);

        pollWithNoData(0);

        savesInvalidOutOfRequiredMessage(times(2), TIMESTAMP);
        verifyNoError();
    }

    @Test
    void shouldFrameSplitFixMessageWhenBackpressured()
    {
        firstSaveAttemptIsBackPressured();

        theEndpointReceivesAnIncompleteMessage();
        assertEquals(MSG_LEN - 8, endPoint.poll());

        theEndpointReceivesTheRestOfTheMessage();
        assertEquals(-8, endPoint.poll());

        pollWithNoData(0);

        savesFramedMessages(2, OK, MSG_LEN);

        sessionReceivesOneMessage();
    }

    @Test
    void shouldFrameTwoCompleteFixMessagesInOnePacketWhenBackpressured()
    {
        firstSaveAttemptIsBackPressured();

        theEndpointReceivesTwoCompleteMessages();
        final int expected = -2 * MSG_LEN;
        assertEquals(expected, endPoint.poll());

        pollWithNoData(0);

        savesTwoFramedMessages(2);

        sessionReceivesTwoMessages();
    }

    @Test
    void shouldFrameOneCompleteMessageWhenTheSecondMessageIsIncompleteWhenBackpressured()
    {
        firstSaveAttemptIsBackPressured();

        theEndpointReceivesACompleteAndAnIncompleteMessage();
        endPoint.poll();

        pollWithNoData(0);

        savesFramedMessages(2, OK, MSG_LEN);
    }

    @Test
    void shouldFrameSecondSplitMessageWhenBackpressured()
    {
        firstSaveAttemptIsBackPressured();

        theEndpointReceivesACompleteAndAnIncompleteMessage();
        endPoint.poll();

        theEndpointReceivesTheRestOfTheMessage();
        endPoint.poll();

        savesFramedMessages(2, OK, MSG_LEN);

        sessionReceivesTwoMessages();
    }

    @Test
    void shouldFrameLogonMessageWhenLoggerBehind()
    {
        givenAnUnauthenticatedReceiverEndPoint();
        givenLogonResult(backpressuredPendingAuth);

        theEndpointReceivesALogon();

        // Backpressured attempt
        polls(LOGON_LEN);

        nothingMoreSaved();

        // Successful attempt
        pollWithNoData(LOGON_LEN);

        savesFramedMessages(1, OK, LOGON_LEN, LogonDecoder.MESSAGE_TYPE);
    }

    private void firstSaveAttemptIsBackPressured()
    {
        when(publication
            .saveMessage(
                anyBuffer(),
                anyInt(),
                anyInt(),
                anyInt(),
                anyLong(),
                anyLong(),
                anyInt(),
                anyLong(),
                any(),
                anyInt(),
                anyLong()))
            .thenReturn(BACK_PRESSURED, POSITION);
    }

    private DirectBuffer anyBuffer()
    {
        return any(DirectBuffer.class);
    }

    private void polls(final int bytesReadAndSaved)
    {
        assertEquals(bytesReadAndSaved, endPoint.poll());
    }

    private void verifyNoError()
    {
        verify(errorHandler, never()).onError(any());
        assertFalse(endPoint.hasDisconnected(), "Endpoint Disconnected");
    }

    private void verifyError(final VerificationMode mode)
    {
        verify(errorHandler, mode).onError(any());
        assertTrue(endPoint.hasDisconnected(), "Endpoint Connected");
    }

    private void savesInvalidOutOfRequiredMessage(final VerificationMode mode, final long timestamp)
    {
        savesInvalidMessage(TAG_SPECIFIED_OUT_OF_REQUIRED_ORDER_MESSAGE_BYTES.length, mode, INVALID, timestamp);
    }

    private void savesInvalidMessage(
        final int length, final VerificationMode mode, final MessageStatus status, final long timestamp)
    {
        verify(publication, mode).saveMessage(
            anyBuffer(), eq(0), eq(length), eq(LIBRARY_ID),
            anyLong(), anyLong(), anyInt(), eq(CONNECTION_ID),
            eq(status), eq(0), eq(timestamp));
    }

    private void verifyDisconnected(final DisconnectReason reason)
    {
        verify(publication).saveDisconnect(LIBRARY_ID, CONNECTION_ID, reason);
    }

    private void verifyNotDisconnected()
    {
        verify(publication, never()).saveDisconnect(anyInt(), anyLong(), any());
    }

    private void theChannelIsClosed() throws IOException
    {
        when(mockChannel.read(any(ByteBuffer.class))).thenReturn(-1);
    }

    private void theChannelIsClosedByException() throws IOException
    {
        doThrow(new ClosedChannelException()).when(mockChannel).read(any(ByteBuffer.class));
    }

    private void savesAFramedMessage()
    {
        savesFramedMessages(1, OK, MSG_LEN);
    }

    private void savesFramedMessages(
        final int numberOfMessages,
        final MessageStatus status,
        final int msgLen)
    {
        savesFramedMessages(numberOfMessages, status, msgLen, MESSAGE_TYPE);
    }

    private long savesFramedMessages(
        final int numberOfMessages, final MessageStatus status, final int msgLen, final long messageType)
    {
        return verify(publication, times(numberOfMessages)).saveMessage(
            anyBuffer(), eq(0), eq(msgLen), eq(LIBRARY_ID),
            eq(messageType), eq(SESSION_ID), anyInt(), eq(CONNECTION_ID),
            eq(status), eq(0), eq(TIMESTAMP));
    }

    private void savesTwoFramedMessages(final int firstMessageSaveAttempts)
    {
        final InOrder inOrder = Mockito.inOrder(publication);
        inOrder.verify(publication, times(firstMessageSaveAttempts)).saveMessage(
            anyBuffer(),
            eq(0),
            eq(MSG_LEN),
            eq(LIBRARY_ID),
            eq(MESSAGE_TYPE),
            eq(SESSION_ID),
            anyInt(),
            eq(CONNECTION_ID),
            eq(OK),
            eq(0),
            eq(TIMESTAMP));

        inOrder.verify(publication, times(1)).saveMessage(
            anyBuffer(),
            eq(MSG_LEN),
            eq(MSG_LEN),
            eq(LIBRARY_ID),
            eq(MESSAGE_TYPE),
            eq(SESSION_ID),
            anyInt(),
            eq(CONNECTION_ID),
            eq(OK),
            eq(0),
            eq(TIMESTAMP));

        inOrder.verifyNoMoreInteractions();
    }

    private void nothingMoreSaved()
    {
        verifyNoMoreInteractions(publication);
    }

    private void theEndpointReceivesACompleteMessage()
    {
        theEndpointReceives(EG_MESSAGE, 0, MSG_LEN);
    }

    private void theEndpointReceivesTheStartOfAnOversizedMessage(final int overflow)
    {
        endpointBufferUpdatedWith(
            (buffer) ->
            {
                final int capacity = buffer.capacity();
                final int messageLength = capacity + overflow;
                final byte[] bytes = TestFixtures.largeMessage(messageLength);
                buffer.put(bytes, 0, capacity);
                return capacity;
            });
    }

    private void theEndpointReceivesTwoCompleteMessages()
    {
        theEndpointReceivesTwoMessages(0, MSG_LEN);
    }

    private void theEndpointReceivesAnIncompleteMessage()
    {
        theEndpointReceives(EG_MESSAGE, 0, MSG_LEN - 8);
    }

    private void theEndpointReceivesTheRestOfTheMessage()
    {
        theEndpointReceives(EG_MESSAGE, MSG_LEN - 8, 8);
    }

    private void theEndpointReceivesACompleteAndAnIncompleteMessage()
    {
        theEndpointReceivesTwoMessages(0, MSG_LEN - 8);
    }

    private void theEndpointReceives(final byte[] data, final int offset, final int length)
    {
        endpointBufferUpdatedWith(
            (buffer) ->
            {
                buffer.put(data, offset, length);
                return length;
            });
    }

    private void theEndpointReceivesNothing()
    {
        endpointBufferUpdatedWith(buffer -> 0);
    }

    private void theEndpointReceivesTwoMessages(final int secondOffset, final int secondLength)
    {
        endpointBufferUpdatedWith(
            (buffer) ->
            {
                buffer.put(EG_MESSAGE).put(EG_MESSAGE, secondOffset, secondLength);
                return MSG_LEN + secondLength;
            });
    }

    private void theEndpointReceivesAnOutOfOrderMessage(final int bytesRead)
    {
        theEndpointReceives(TAG_SPECIFIED_OUT_OF_REQUIRED_ORDER_MESSAGE_BYTES, 0, OUT_OF_REQUIRED_ORDER_MSG_LEN);

        assertEquals(bytesRead, endPoint.poll());
    }

    private void endpointBufferUpdatedWith(final ToIntFunction<ByteBuffer> bufferUpdater)
    {
        try
        {
            doAnswer(
                (invocation) ->
                {
                    final ByteBuffer buffer = (ByteBuffer)invocation.getArguments()[0];
                    return bufferUpdater.applyAsInt(buffer);
                }).when(mockChannel).read(any(ByteBuffer.class));
        }
        catch (final IOException ex)
        {
            // Should never happen, test in error
            LangUtil.rethrowUnchecked(ex);
        }
    }

    private void theEndpointReceivesAMessageWithInvalidChecksum()
    {
        theEndpointReceives(INVALID_CHECKSUM_MSG, 0, INVALID_CHECKSUM_LEN);
    }

    private void savesInvalidChecksumMessage(final VerificationMode mode)
    {
        verify(publication, mode).saveMessage(
            anyBuffer(),
            eq(0),
            eq(INVALID_CHECKSUM_LEN),
            eq(LIBRARY_ID),
            eq(MESSAGE_TYPE),
            anyLong(),
            anyInt(),
            eq(CONNECTION_ID),
            eq(INVALID_CHECKSUM),
            eq(0),
            eq(TIMESTAMP));
    }

    private void sessionReceivesOneMessage()
    {
        sessionReceivesMessageAt(0, MSG_LEN, times(1));
        sessionReceivedCountIs(1);
    }

    private void sessionReceivesTwoMessages()
    {
        sessionReceivesMessageAt(0, MSG_LEN, times(1));
        sessionReceivesMessageAt(MSG_LEN, MSG_LEN, times(1));
        sessionReceivedCountIs(2);
    }

    private void sessionReceivesTwoMessageAtBufferStart()
    {
        sessionReceivesMessageAt(0, MSG_LEN, times(2));
        sessionReceivedCountIs(2);
    }

    private void sessionReceivedCountIs(final int numberOfMessages)
    {
        verify(gatewaySession, times(numberOfMessages))
            .onMessage(any(), anyInt(), anyInt(), anyLong(), anyLong());
    }

    private void sessionReceivesMessageAt(final int offset, final int length, final VerificationMode mode)
    {
        verify(gatewaySession, mode)
            .onMessage(any(), eq(offset), eq(length), eq(MESSAGE_TYPE), anyLong());
    }

    private void sessionReceivesNoMessages()
    {
        verify(gatewaySession, never())
            .onMessage(any(), anyInt(), anyInt(), anyLong(), anyLong());
    }

    private void pollWithNoData(final int expected)
    {
        theEndpointReceivesNothing();
        assertEquals(expected, endPoint.poll());
    }

    private void verifyDuplicateSession(final VerificationMode times)
    {
        verify(publication, times).saveDisconnect(anyInt(), anyLong(), eq(DisconnectReason.DUPLICATE_SESSION));
    }

    private void givenADuplicateSession()
    {
        final AcceptorLogonResult pendingAcceptorLogon = mock(AcceptorLogonResult.class);
        when(pendingAcceptorLogon.poll()).thenReturn(true);
        when(pendingAcceptorLogon.isAccepted()).thenReturn(false);
        when(pendingAcceptorLogon.reason()).thenReturn(DUPLICATE_SESSION);

        givenLogonResult(pendingAcceptorLogon);
    }
}
