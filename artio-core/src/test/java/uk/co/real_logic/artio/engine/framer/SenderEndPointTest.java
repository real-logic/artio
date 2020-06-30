/*
 * Copyright 2015-2020 Real Logic Limited.
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

import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import org.agrona.ErrorHandler;
import org.agrona.LangUtil;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
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
import static uk.co.real_logic.artio.engine.logger.ArchiveDescriptor.alignTerm;
import static uk.co.real_logic.artio.messages.DisconnectReason.SLOW_CONSUMER;
import static uk.co.real_logic.artio.protocol.GatewayPublication.FRAME_SIZE;

public class SenderEndPointTest
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

    private final SenderEndPoint endPoint = new SenderEndPoint(
        CONNECTION_ID,
        LIBRARY_ID,
        libraryBlockablePosition,
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

    @Test
    public void shouldRetrySlowConsumerMessage()
    {
        becomeSlowConsumer();

        channelWillWrite(BODY_LENGTH);

        onSlowOutboundMessage();
        byteBufferWritten();
        assertBytesInBuffer(0);
        verifyDoesNotBlockLibrary();
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
        verifyDoesNotBlockLibrary();
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
        verifyDoesNotBlockLibrary();
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
        final long position = 0;
        channelWillWrite(0);
        onReplayMessage(0, position);
        byteBufferWritten();

        onNormalStreamReplayComplete();
        assertReplayPaused();

        channelWillWrite(BODY_LENGTH);
        onSlowReplayMessage(0, position);
        byteBufferWritten();
        verifyDoesNotBlockLibrary();

        onSlowStreamReplayComplete();
        assertNotReplayPaused();

        assertBytesInBuffer(0);
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
        final int firstWrites = 41;
        final int remaining = BODY_LENGTH - firstWrites;

        final long position = POSITION;
        channelWillWrite(0);
        onReplayMessage(0, position);
        byteBufferWritten();
        verifyDoesNotBlockLibrary();

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
        verifyDoesNotBlockLibrary();

        onSlowStreamReplayComplete();
        assertNotReplayPaused();
    }

    @Test
    public void shouldNotSendSlowConsumerMessageUntilReplayComplete()
    {
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
        endPoint.onOutboundMessage(LIBRARY_ID, buffer, 0, BODY_LENGTH, 0, position, timeInMs);
    }

    private void onReplayMessage(final long timeInMs, final long position)
    {
        endPoint.onReplayMessage(buffer, 0, BODY_LENGTH, timeInMs, position);
    }

    private void onSlowReplayMessage(final long timeInMs, final long position)
    {
        endPoint.onSlowReplayMessage(buffer, 0, BODY_LENGTH, timeInMs, position, 0);
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
        final Action action = endPoint.onSlowOutboundMessage(
            buffer,
            HEADER_LENGTH,
            LENGTH,
            POSITION,
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

    private void verifyDoesNotBlockLibrary()
    {
        verify(libraryBlockablePosition, never()).blockPosition(anyLong());
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
