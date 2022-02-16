/*
 * Copyright 2015-2022 Real Logic Limited.
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

import io.aeron.ExclusivePublication;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import io.aeron.logbuffer.FrameDescriptor;
import io.aeron.logbuffer.Header;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.ErrorHandler;
import org.agrona.LangUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.junit.Before;
import org.junit.Test;
import org.mockito.stubbing.Answer;
import org.mockito.verification.VerificationMode;
import uk.co.real_logic.artio.engine.MessageTimingHandler;
import uk.co.real_logic.artio.engine.SenderSequenceNumber;
import uk.co.real_logic.artio.messages.MessageHeaderDecoder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.artio.engine.EngineConfiguration.DEFAULT_SLOW_CONSUMER_TIMEOUT_IN_MS;
import static uk.co.real_logic.artio.engine.framer.FixSenderEndPoint.START_REPLAY_LENGTH;
import static uk.co.real_logic.artio.engine.logger.ArchiveDescriptor.alignTerm;
import static uk.co.real_logic.artio.messages.DisconnectReason.SLOW_CONSUMER;
import static uk.co.real_logic.artio.protocol.GatewayPublication.FRAME_SIZE;

public class FixSenderEndPointTest
{
    private static final long CONNECTION_ID = 1;
    private static final int LIBRARY_ID = 2;
    private static final int HEADER_LENGTH = MessageHeaderDecoder.ENCODED_LENGTH;

    private static final long POSITION = 8 * 1024;
    private static final int BODY_LENGTH = 84;
    private static final int LENGTH = FRAME_SIZE + BODY_LENGTH;
    private static final int FRAGMENT_LENGTH = alignTerm(HEADER_LENGTH + FRAME_SIZE + BODY_LENGTH);
    private static final long BEGIN_POSITION = 8000;
    private static final int MAX_BYTES_IN_BUFFER = 3 * BODY_LENGTH;
    public static final int INBOUND_BUFFER_LEN = 128;
    public static final int REPLAY_CORRELATION_ID = 2;

    private final TcpChannel tcpChannel = mock(TcpChannel.class);
    private final AtomicCounter bytesInBuffer = fakeCounter();
    private final AtomicCounter invalidLibraryAttempts = mock(AtomicCounter.class);
    private final ErrorHandler errorHandler = mock(ErrorHandler.class);
    private final Framer framer = mock(Framer.class);
    private final ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    private final UnsafeBuffer buffer = new UnsafeBuffer(byteBuffer);
    private final BlockablePosition libraryBlockablePosition = mock(BlockablePosition.class);
    private final BlockablePosition replayBlockablePosition = mock(BlockablePosition.class);
    private final SenderSequenceNumber senderSequenceNumber = mock(SenderSequenceNumber.class);
    private final MessageTimingHandler messageTimingHandler = mock(MessageTimingHandler.class);
    private final ExclusivePublication inboundPublication = mock(ExclusivePublication.class);
    private final UnsafeBuffer inboundBuffer = new UnsafeBuffer(new byte[INBOUND_BUFFER_LEN]);
    private final Header header = mock(Header.class);

    private final FixSenderEndPoint endPoint = new FixSenderEndPoint(
        CONNECTION_ID,
        LIBRARY_ID,
        libraryBlockablePosition,
        inboundPublication,
        replayBlockablePosition,
        tcpChannel,
        bytesInBuffer,
        invalidLibraryAttempts,
        errorHandler,
        framer,
        MAX_BYTES_IN_BUFFER,
        DEFAULT_SLOW_CONSUMER_TIMEOUT_IN_MS,
        0,
        senderSequenceNumber,
        messageTimingHandler);

    @Before
    public void setup()
    {
        when(header.flags()).thenReturn(FrameDescriptor.UNFRAGMENTED);

        when(inboundPublication.tryClaim(anyInt(), any())).then(invocation ->
        {
            final BufferClaim claim = invocation.getArgument(1);
            final int length = invocation.getArgument(0);
            claim.wrap(inboundBuffer, 0, length + DataHeaderFlyweight.HEADER_LENGTH);
            return 1L;
        });
    }

    @Test
    public void shouldRetrySlowConsumerMessage()
    {
        becomeSlowConsumer();

        channelWillWrite(BODY_LENGTH);

        onSlowOutboundMessage();
        byteBufferWritten();
        assertBytesInBuffer(0);
        verifyDoesNotBlock();
    }

    @Test
    public void shouldBeAbleToFragmentSlowConsumerRetries()
    {
        becomeSlowConsumer();

        final int firstWrites = 41;
        final int remaining = BODY_LENGTH - firstWrites;

        channelWillWrite(firstWrites);
        onSlowOutboundMessage();
        verifyBlocksLibraryAt(BEGIN_POSITION);
        byteBufferWritten();
        assertBytesInBuffer(remaining);

        channelWillWrite(remaining);
        onSlowOutboundMessage();
        verifyDoesNotBlock();
        byteBufferWritten();
        assertBytesInBuffer(0);
        verifyNoMoreErrors();
    }

    @Test
    public void shouldDisconnectSlowConsumerAfterTimeout()
    {
        long timeInMs = 100;
        long position = POSITION;
        channelWillWrite(BODY_LENGTH);
        onOutboundMessage(timeInMs, position);

        timeInMs += 100;
        position += FRAGMENT_LENGTH;

        channelWillWrite(0);
        onOutboundMessage(timeInMs, position);

        timeInMs += DEFAULT_SLOW_CONSUMER_TIMEOUT_IN_MS + 1;

        endPoint.checkTimeouts(timeInMs);

        verifySlowConsumerDisconnect(times(1));
        errorLogged();
    }

    @Test
    public void shouldNotDisconnectSlowConsumerBeforeTimeout()
    {
        long timeInMs = 100;
        channelWillWrite(BODY_LENGTH);
        onOutboundMessage(timeInMs, POSITION);

        timeInMs += (DEFAULT_SLOW_CONSUMER_TIMEOUT_IN_MS - 1);

        endPoint.checkTimeouts(timeInMs);

        verifySlowConsumerDisconnect(never());
        verifyNoMoreErrors();
    }

    @Test
    public void shouldNotDisconnectSlowConsumerBeforeTimeoutOnSlowChannel()
    {
        final int replayWrites = 41;

        long timeInMs = 100;
        channelWillWrite(0);
        onOutboundMessage(timeInMs, POSITION);

        timeInMs += (DEFAULT_SLOW_CONSUMER_TIMEOUT_IN_MS - 1);

        channelWillWrite(replayWrites);
        onSlowOutboundMessage(timeInMs);
        verifyBlocksLibraryAt(BEGIN_POSITION);

        timeInMs += (DEFAULT_SLOW_CONSUMER_TIMEOUT_IN_MS - 1);

        endPoint.checkTimeouts(timeInMs);

        verifySlowConsumerDisconnect(never());
        verifyNoMoreErrors();
    }

    @Test
    public void shouldNotDisconnectAtStartDueToTimeout()
    {
        final long timeInMs = DEFAULT_SLOW_CONSUMER_TIMEOUT_IN_MS - 1;

        endPoint.checkTimeouts(timeInMs);

        verifySlowConsumerDisconnect(never());
        verifyNoMoreErrors();
        verifyDoesNotBlock();
    }

    @Test
    public void shouldNotDisconnectRegularConsumerDueToTimeout()
    {
        long timeInMs = 100;
        channelWillWrite(BODY_LENGTH);
        onOutboundMessage(timeInMs, POSITION);

        timeInMs += (DEFAULT_SLOW_CONSUMER_TIMEOUT_IN_MS + 1);

        endPoint.checkTimeouts(timeInMs);

        verifySlowConsumerDisconnect(never());
        verifyNoMoreErrors();
    }

    @Test
    public void shouldDisconnectSlowConsumerAfterTimeoutAfterFragment()
    {
        becomeSlowConsumer();

        final int firstWrites = 41;

        channelWillWrite(firstWrites);
        onSlowOutboundMessage();
        verifyBlocksLibraryAt(BEGIN_POSITION);

        channelWillWrite(0);
        onSlowOutboundMessage();
        verifyBlocksLibraryAt(BEGIN_POSITION);

        endPoint.checkTimeouts(DEFAULT_SLOW_CONSUMER_TIMEOUT_IN_MS + 101);

        verifySlowConsumerDisconnect(times(1));
        errorLogged();
    }

    @Test
    public void shouldBecomeReplaySlowConsumer()
    {
        final long position = 0;
        channelWillWrite(0);
        onReplayMessage(0, position);

        assertBytesInBuffer(BODY_LENGTH);
    }

    @Test
    public void shouldNotSendFurtherMessagesBeforeReplayRetry()
    {
        long position = 0;
        channelWillWrite(0);
        onReplayMessage(0, position);
        byteBufferWritten();

        position += BODY_LENGTH;

        onReplayMessage(0, position);
        byteBufferNotWritten();

        assertBytesInBuffer(BODY_LENGTH + BODY_LENGTH);
    }

    @Test
    public void shouldRetryReplaySlowConsumer()
    {
        startValidReplay();

        final long position = 128;
        channelWillWrite(0);
        onReplayMessage(0, position);
        byteBufferWritten();

        onNormalStreamReplayComplete();
        assertReplayPaused();

        channelWillWrite(BODY_LENGTH);
        onSlowReplayMessage(0, position);
        byteBufferWritten();
        verifyDoesNotBlock();

        onSlowStreamReplayComplete();
        assertNotReplayPaused();

        assertBytesInBuffer(0);
    }

    private void startValidReplay()
    {
        endPoint.onSlowValidResendRequest(REPLAY_CORRELATION_ID);
        endPoint.onStartReplay(REPLAY_CORRELATION_ID, 64, false);
    }

    @Test
    public void shouldDisconnectReplaySlowConsumer()
    {
        long position = 0;
        channelWillWrite(0);
        onReplayMessage(0, position);

        position += FRAGMENT_LENGTH;
        onReplayMessage(0, position);

        position += FRAGMENT_LENGTH;
        onReplayMessage(0, position);

        position += FRAGMENT_LENGTH;
        onReplayMessage(0, position);

        verifySlowConsumerDisconnect(times(1));
    }

    @Test
    public void shouldBeAbleToFragmentReplaySlowConsumerRetries()
    {
        startValidReplay();

        final int firstWrites = 41;
        final int remaining = BODY_LENGTH - firstWrites;

        final long position = POSITION;
        channelWillWrite(0);
        onReplayMessage(0, position);
        byteBufferWritten();
        verifyDoesNotBlock();

        onNormalStreamReplayComplete();
        assertReplayPaused();

        channelWillWrite(firstWrites);
        onSlowReplayMessage(0, position);
        byteBufferWritten();
        assertBytesInBuffer(remaining);
        verifyBlocksReplayAt(BEGIN_POSITION);

        onSlowStreamReplayComplete();
        assertReplayPaused();

        channelWillWrite(remaining);
        onSlowReplayMessage(0, position);
        byteBufferWritten();
        assertBytesInBuffer(0);
        verifyNoMoreErrors();
        verifyDoesNotBlock();

        onSlowStreamReplayComplete();
        assertNotReplayPaused();
    }

    @Test
    public void shouldNotSendSlowConsumerMessageUntilReplayComplete()
    {
        startValidReplay();

        final int firstWrites = 41;
        final int remaining = BODY_LENGTH - firstWrites;

        final long position = POSITION;
        channelWillWrite(0);
        onReplayMessage(0, position);
        byteBufferWritten();

        onNormalStreamReplayComplete();
        assertReplayPaused();

        channelWillWrite(firstWrites);
        onSlowReplayMessage(0, position);
        byteBufferWritten();
        assertBytesInBuffer(remaining);

        onSlowStreamReplayComplete();
        assertReplayPaused();

        onOutboundMessage(0, position);
        byteBufferNotWritten();
        assertBytesInBuffer(remaining + BODY_LENGTH);

        onSlowOutboundMessage();
        byteBufferNotWritten();
        assertBytesInBuffer(remaining + BODY_LENGTH);
        verifyBlocksLibraryAt(BEGIN_POSITION);

        channelWillWrite(remaining);
        onSlowReplayMessage(0, position);
        byteBufferWritten();
        assertBytesInBuffer(BODY_LENGTH);

        onSlowStreamReplayComplete();
        assertNotReplayPaused();

        channelWillWrite(BODY_LENGTH);
        onSlowOutboundMessage();
        byteBufferWritten();
        assertBytesInBuffer(0);

        verifyNoMoreErrors();
    }

    @Test
    public void shouldNotSendReplayMessageUntilSlowConsumerComplete()
    {
        startValidReplay();

        final int firstWrites = 41;
        final int remaining = BODY_LENGTH - firstWrites;
        final long position = FRAGMENT_LENGTH;

        becomeSlowConsumer();

        channelWillWrite(firstWrites);
        onSlowOutboundMessage();
        byteBufferWritten();
        assertBytesInBuffer(remaining);
        verifyBlocksLibraryAt(BEGIN_POSITION);

        channelWillWrite(0);
        onReplayMessage(0, position);
        byteBufferNotWritten();
        assertBytesInBuffer(remaining + BODY_LENGTH);

        onNormalStreamReplayComplete();
        assertNotReplayPaused();

        onSlowReplayMessage(0, position);
        byteBufferNotWritten();
        assertBytesInBuffer(remaining + BODY_LENGTH);

        onSlowStreamReplayComplete();
        assertNotReplayPaused();

        channelWillWrite(remaining);
        onSlowOutboundMessage();
        byteBufferWritten();
        assertBytesInBuffer(BODY_LENGTH);

        channelWillWrite(BODY_LENGTH);
        onSlowReplayMessage(0, BODY_LENGTH);
        byteBufferWritten();
        assertBytesInBuffer(0);

        onSlowStreamReplayComplete();
        assertNotReplayPaused();

        verifyNoMoreErrors();
    }

    // TODO:
    // subscription already a slow consumer when this happens - ie we should only use onValidResendRequest from the
    // subscription that we're in
    // multiple resends in a buffer
    // add correlation ids to complete message

    @Test
    public void shouldNotSendReplayMessageUntilNormalMessagesDrained()
    {
        final int outboundMsgPosition = 768;
        final int msgPosition = 512;

        // Replayer wins the race and we try to start replaying whilst there are FIX messages on the outbound path
        endPoint.onStartReplay(REPLAY_CORRELATION_ID, msgPosition, false);
        verifyBlocksReplayAt(msgPosition - START_REPLAY_LENGTH);
        assertBytesInBuffer(START_REPLAY_LENGTH);

        channelWillWrite(BODY_LENGTH);
        onReplayMessage(1, BODY_LENGTH);
        assertBytesInBuffer(START_REPLAY_LENGTH + BODY_LENGTH);

        onSlowStartReplay(REPLAY_CORRELATION_ID, msgPosition);
        verifyBlocksReplayAt(msgPosition - START_REPLAY_LENGTH);

        channelWillWrite(BODY_LENGTH);
        onSlowReplayMessage(1, BODY_LENGTH);
        assertBytesInBuffer(START_REPLAY_LENGTH + BODY_LENGTH);

        byteBufferNotWritten();
        assertNotReplayPaused();

        // Send the FIX message in the outbound buffer
        // First time ignored as we're in the slow group
        channelWillWrite(BODY_LENGTH);
        onOutboundMessage(1, outboundMsgPosition);
        verifyDoesNotBlock();
        byteBufferNotWritten();
        assertBytesInBuffer(START_REPLAY_LENGTH + BODY_LENGTH + BODY_LENGTH);

        // now sent on the slow poll
        channelWillWrite(BODY_LENGTH);
        onSlowOutboundMessage();
        verifyDoesNotBlock();
        byteBufferWritten();
        assertBytesInBuffer(START_REPLAY_LENGTH + BODY_LENGTH);

        // Actually do the replay
        endPoint.onSlowValidResendRequest(REPLAY_CORRELATION_ID);

        onSlowStartReplay(REPLAY_CORRELATION_ID, msgPosition);
        verifyDoesNotBlock();

        channelWillWrite(BODY_LENGTH);
        onSlowReplayMessage(1, BODY_LENGTH);
        byteBufferWritten();
        assertBytesInBuffer(0);
        assertReplayPaused();

        onSlowStreamReplayComplete();

        assertNotReplayPaused();
    }

    private void onSlowStartReplay(final int replayCorrelationId, final int msgPosition)
    {
        endPoint.onStartReplay(replayCorrelationId, msgPosition, true);
    }

    private void byteBufferNotWritten()
    {
        byteBufferWritten(never());
    }

    private void errorLogged()
    {
        verify(errorHandler).onError(any(IllegalStateException.class));
    }

    private void verifyNoMoreErrors()
    {
        verifyNoMoreInteractions(errorHandler);
    }

    private void onOutboundMessage(final long timeInMs, final long position)
    {
        headerPosition(position);
        endPoint.onOutboundMessage(LIBRARY_ID, buffer, 0, BODY_LENGTH, 0, header, timeInMs, 0);
    }

    private void headerPosition(final long position)
    {
        when(header.position()).thenReturn(position);
    }

    private void onReplayMessage(final long timeInMs, final long position)
    {
        headerPosition(position);
        assertEquals(CONTINUE, endPoint.onReplayMessage(buffer, 0, BODY_LENGTH, timeInMs, header));
    }

    private void onSlowReplayMessage(final long timeInMs, final long position)
    {
        headerPosition(position);
        assertEquals(CONTINUE, endPoint.onSlowReplayMessage(buffer, 0, BODY_LENGTH, timeInMs, header, 0));
    }

    private void verifySlowConsumerDisconnect(final VerificationMode times)
    {
        verify(framer, times).onDisconnect(LIBRARY_ID, CONNECTION_ID, SLOW_CONSUMER);
    }

    private void byteBufferWritten()
    {
        byteBufferWritten(times(1));
    }

    private void byteBufferWritten(final VerificationMode times)
    {
        try
        {
            verify(tcpChannel, times).write(byteBuffer);
            reset(tcpChannel);
        }
        catch (final IOException e)
        {
            LangUtil.rethrowUnchecked(e);
        }
    }

    private void assertBytesInBuffer(final int bytes)
    {
        assertEquals(bytes, bytesInBuffer.get());
    }

    private void onSlowOutboundMessage()
    {
        onSlowOutboundMessage(100);
    }

    private void onSlowOutboundMessage(final long timeInMs)
    {
        headerPosition(POSITION);
        final Action action = endPoint.onSlowOutboundMessage(
            buffer,
            HEADER_LENGTH,
            LENGTH,
            header,
            BODY_LENGTH,
            LIBRARY_ID,
            timeInMs,
            0,
            1);
        assertEquals(CONTINUE, action);
    }

    private void becomeSlowConsumer()
    {
        channelWillWrite(0);
        onOutboundMessage(0, POSITION);
        byteBufferWritten();
    }

    private void channelWillWrite(final int bodyLength)
    {
        try
        {
            when(tcpChannel.write(byteBuffer)).thenReturn(bodyLength);
        }
        catch (final IOException e)
        {
            LangUtil.rethrowUnchecked(e);
        }
    }

    private AtomicCounter fakeCounter()
    {
        final AtomicLong value = new AtomicLong();
        final AtomicCounter atomicCounter = mock(AtomicCounter.class);
        final Answer<Long> get = inv -> value.get();
        final Answer<?> set = inv ->
        {
            value.set(inv.getArgument(0));
            return null;
        };

        final Answer<?> add = (inv) -> value.getAndAdd(inv.getArgument(0));

        when(atomicCounter.get()).then(get);
        when(atomicCounter.getWeak()).then(get);

        doAnswer(set).when(atomicCounter).set(anyLong());
        doAnswer(set).when(atomicCounter).setOrdered(anyLong());
        doAnswer(set).when(atomicCounter).setWeak(anyLong());

        when(atomicCounter.getAndAdd(anyLong())).then(add);
        when(atomicCounter.getAndAddOrdered(anyLong())).then(add);

        return atomicCounter;
    }

    private void verifyDoesNotBlock()
    {
        verify(libraryBlockablePosition, never()).blockPosition(anyLong());
        verifyDoesNotBlockReplay();
    }

    private void verifyDoesNotBlockReplay()
    {
        verify(replayBlockablePosition, never()).blockPosition(anyLong());
    }

    private void verifyBlocksLibraryAt(final long position)
    {
        verifyBlocksAt(position, libraryBlockablePosition);
    }

    private void verifyBlocksReplayAt(final long position)
    {
        verifyBlocksAt(position, replayBlockablePosition);
    }

    private void verifyBlocksAt(final long position, final BlockablePosition blockablePosition)
    {
        verify(blockablePosition).blockPosition(position);
        reset(blockablePosition);
    }

    private void assertNotReplayPaused()
    {
        assertFalse("should not be replay paused", endPoint.replayPaused());
    }

    private void assertReplayPaused()
    {
        assertTrue("should be replay paused", endPoint.replayPaused());
    }

    private void onSlowStreamReplayComplete()
    {
        endPoint.onReplayComplete();
    }

    private void onNormalStreamReplayComplete()
    {
        endPoint.onReplayComplete();
    }
}
