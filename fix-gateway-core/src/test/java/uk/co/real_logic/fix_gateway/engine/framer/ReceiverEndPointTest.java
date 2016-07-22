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

import org.agrona.ErrorHandler;
import org.agrona.LangUtil;
import org.agrona.concurrent.AtomicBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;
import uk.co.real_logic.fix_gateway.engine.FixEngine;
import uk.co.real_logic.fix_gateway.engine.logger.SequenceNumberIndexReader;
import uk.co.real_logic.fix_gateway.messages.MessageStatus;
import uk.co.real_logic.fix_gateway.protocol.GatewayPublication;
import uk.co.real_logic.fix_gateway.session.CompositeKey;
import uk.co.real_logic.fix_gateway.session.SessionIdStrategy;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.function.ToIntFunction;

import static io.aeron.Publication.BACK_PRESSURED;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.fix_gateway.dictionary.ExampleDictionary.TAG_SPECIFIED_OUT_OF_REQUIRED_ORDER_MESSAGE_BYTES;
import static uk.co.real_logic.fix_gateway.messages.DisconnectReason.REMOTE_DISCONNECT;
import static uk.co.real_logic.fix_gateway.messages.MessageStatus.*;
import static uk.co.real_logic.fix_gateway.session.Session.UNKNOWN;
import static uk.co.real_logic.fix_gateway.util.TestMessages.*;

public class ReceiverEndPointTest
{
    private static final int MESSAGE_TYPE = 'D';
    private static final long CONNECTION_ID = 20L;
    private static final long SESSION_ID = 4L;
    private static final int LIBRARY_ID = FixEngine.GATEWAY_LIBRARY_ID;
    private static final long POSITION = 1024L;

    private CompositeKey compositeKey = mock(CompositeKey.class);
    private TcpChannel mockChannel = mock(TcpChannel.class);
    private GatewayPublication publication = mock(GatewayPublication.class);
    private SessionIdStrategy mockSessionIdStrategy = mock(SessionIdStrategy.class);
    private SessionIds mockSessionIds = mock(SessionIds.class);
    private AtomicCounter messagesRead = mock(AtomicCounter.class);
    private ErrorHandler errorHandler = mock(ErrorHandler.class);
    private SequenceNumberIndexReader sentSequenceNumbers = mock(SequenceNumberIndexReader.class);
    private SequenceNumberIndexReader receivedSequenceNumbers = mock(SequenceNumberIndexReader.class);
    private Framer framer = mock(Framer.class);

    private ReceiverEndPoint endPoint =
        new ReceiverEndPoint(
            mockChannel, 16 * 1024, publication, CONNECTION_ID, UNKNOWN, mockSessionIdStrategy, mockSessionIds,
            sentSequenceNumbers, receivedSequenceNumbers, messagesRead, framer, errorHandler, LIBRARY_ID, false);

    @Before
    public void setUp()
    {
        endPoint.gatewaySession(mock(GatewaySession.class));
        when(mockSessionIds.onLogon(any())).thenReturn(SESSION_ID);
        when(mockSessionIdStrategy.onLogon(any())).thenReturn(compositeKey);
        doAnswer(inv ->
        {
            ((Transaction) inv.getArguments()[0]).attempt();
            return null;
        }).when(framer).schedule(any(Transaction.class));
    }

    @Test
    public void shouldFrameValidFixMessage()
    {
        theEndpointReceivesACompleteMessage();

        pollsData(2 * MSG_LEN);

        savesAFramedMessage();
    }

    @Test
    public void shouldIgnoreGarbledMessages() throws IOException
    {
        final int length = GARBLED_MESSAGE.length;
        theEndpointReceives(GARBLED_MESSAGE, 0, length);

        endPoint.pollForData();

        savesInvalidMessage(length);
        verifyNoError();
    }

    @Test
    public void shouldOnlyFrameCompleteFixMessage()
    {
        theEndpointReceivesAnIncompleteMessage();

        endPoint.pollForData();

        nothingSaved();
    }

    @Test
    public void shouldFrameSplitFixMessage()
    {
        theEndpointReceivesAnIncompleteMessage();
        endPoint.pollForData();

        theEndpointReceivesTheRestOfTheMessage();
        endPoint.pollForData();

        savesAFramedMessage();
    }

    @Test
    public void shouldFrameTwoCompleteFixMessagesInOnePacket()
    {
        theEndpointReceivesTwoCompleteMessages();

        endPoint.pollForData();

        savesTwoFramedMessages(1);
    }

    @Test
    public void shouldFrameOneCompleteMessageWhenTheSecondMessageIsIncomplete()
    {
        theEndpointReceivesACompleteAndAnIncompleteMessage();

        endPoint.pollForData();

        savesAFramedMessage();
    }

    @Test
    public void shouldFrameSecondSplitMessage()
    {
        theEndpointReceivesACompleteAndAnIncompleteMessage();
        endPoint.pollForData();

        theEndpointReceivesTheRestOfTheMessage();
        endPoint.pollForData();

        savesFramedMessages(2, OK, MSG_LEN);
    }

    @Test
    public void aClosedSocketSavesItsDisconnect() throws IOException
    {
        theChannelIsClosedByException();

        endPoint.pollForData();

        verify(mockSessionIds).onDisconnect(anyLong());
        assertSavesDisconnect();
    }

    @Test
    public void anUnreadableSocketDisconnectsItsSession() throws IOException
    {
        theChannelIsClosed();

        endPoint.pollForData();

        assertSavesDisconnect();
    }

    @Test
    public void invalidChecksumMessageRecorded() throws IOException
    {
        theEndpointReceives(INVALID_CHECKSUM_MSG, 0, INVALID_CHECKSUM_LEN);

        endPoint.pollForData();

        verify(publication, times(1))
            .saveMessage(
                anyBuffer(), eq(0), eq(INVALID_CHECKSUM_LEN),
                eq(LIBRARY_ID), eq(MESSAGE_TYPE), anyLong(), eq(CONNECTION_ID),
                eq(INVALID_CHECKSUM));
    }

    @Test
    public void fieldOutOfOrderMessageRecorded() throws IOException
    {
        final int length = TAG_SPECIFIED_OUT_OF_REQUIRED_ORDER_MESSAGE_BYTES.length;

        theEndpointReceives(
            TAG_SPECIFIED_OUT_OF_REQUIRED_ORDER_MESSAGE_BYTES, 0, length);
        endPoint.pollForData();

        savesInvalidMessage(length);
        verifyNoError();
    }

    @Test
    public void shouldFrameValidFixMessageWhenBackpressured()
    {
        firstSaveAttemptIsBackpressured();

        theEndpointReceivesACompleteMessage();
        pollsData(MSG_LEN);

        theEndpointReceivesNothing();
        pollsData(MSG_LEN);

        savesFramedMessages(2, OK, MSG_LEN);
    }

    @Test
    public void shouldFrameSplitFixMessageWhenBackpressured()
    {
        firstSaveAttemptIsBackpressured();

        theEndpointReceivesAnIncompleteMessage();
        endPoint.pollForData();

        theEndpointReceivesTheRestOfTheMessage();
        endPoint.pollForData();

        theEndpointReceivesNothing();
        endPoint.pollForData();

        savesFramedMessages(2, OK, MSG_LEN);
    }

    @Test
    public void shouldFrameTwoCompleteFixMessagesInOnePacketWhenBackpressured()
    {
        firstSaveAttemptIsBackpressured();

        theEndpointReceivesTwoCompleteMessages();
        endPoint.pollForData();

        theEndpointReceivesNothing();
        endPoint.pollForData();

        savesTwoFramedMessages(2);
    }

    @Test
    public void shouldFrameOneCompleteMessageWhenTheSecondMessageIsIncompleteWhenBackpressured()
    {
        firstSaveAttemptIsBackpressured();

        theEndpointReceivesACompleteAndAnIncompleteMessage();
        endPoint.pollForData();

        theEndpointReceivesNothing();
        endPoint.pollForData();

        savesFramedMessages(2, OK, MSG_LEN);
    }

    @Test
    public void shouldFrameSecondSplitMessageWhenBackpressured()
    {
        firstSaveAttemptIsBackpressured();

        theEndpointReceivesACompleteAndAnIncompleteMessage();
        endPoint.pollForData();

        theEndpointReceivesTheRestOfTheMessage();
        endPoint.pollForData();

        savesFramedMessages(2, OK, MSG_LEN);
    }

    private void firstSaveAttemptIsBackpressured()
    {
        when(publication
            .saveMessage(anyBuffer(), anyInt(), anyInt(), anyInt(), anyInt(), anyLong(), anyLong(), eq(OK)))
            .thenReturn(BACK_PRESSURED, POSITION);
    }

    private AtomicBuffer anyBuffer()
    {
        return any(AtomicBuffer.class);
    }

    private void pollsData(final int bytesReadAndSaved)
    {
        assertEquals(bytesReadAndSaved, endPoint.pollForData());
    }

    private void verifyNoError()
    {
        verify(errorHandler, never()).onError(any());
        assertFalse("Endpoint Disconnected", endPoint.hasDisconnected());
    }

    private void savesInvalidMessage(final int length)
    {
        verify(publication, times(1))
            .saveMessage(
                anyBuffer(), eq(0), eq(length), eq(LIBRARY_ID),
                anyInt(), anyLong(), eq(CONNECTION_ID),
                eq(INVALID));
    }

    private void assertSavesDisconnect()
    {
        verify(publication).saveDisconnect(LIBRARY_ID, CONNECTION_ID, REMOTE_DISCONNECT);
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
        int numberOfMessages,
        final MessageStatus status,
        final int msgLen)
    {
        verify(publication, times(numberOfMessages))
            .saveMessage(
                anyBuffer(), eq(0), eq(msgLen), eq(LIBRARY_ID),
                eq(MESSAGE_TYPE), eq(SESSION_ID), eq(CONNECTION_ID),
                eq(status));
    }

    private void savesTwoFramedMessages(final int firstMessageSaveAttempts)
    {
        final InOrder inOrder = Mockito.inOrder(publication);
        inOrder.verify(publication, times(firstMessageSaveAttempts))
            .saveMessage(
                anyBuffer(), eq(0), eq(MSG_LEN), eq(LIBRARY_ID), eq(MESSAGE_TYPE), eq(SESSION_ID),
                eq(CONNECTION_ID), eq(OK));
        inOrder.verify(publication, times(1))
            .saveMessage(
                anyBuffer(), eq(MSG_LEN), eq(MSG_LEN), eq(LIBRARY_ID),
                eq(MESSAGE_TYPE), eq(SESSION_ID), eq(CONNECTION_ID),
                eq(OK));
        inOrder.verifyNoMoreInteractions();
    }

    private void nothingSaved()
    {
        verifyNoMoreInteractions(publication);
    }

    private void theEndpointReceivesACompleteMessage()
    {
        theEndpointReceives(EG_MESSAGE, 0, MSG_LEN);
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
                buffer.put(EG_MESSAGE)
                    .put(EG_MESSAGE, secondOffset, secondLength);
                return MSG_LEN + secondLength;
            });
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
}
