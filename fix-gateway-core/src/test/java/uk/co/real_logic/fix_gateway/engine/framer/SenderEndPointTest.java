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

import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import org.agrona.ErrorHandler;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.junit.Test;
import org.mockito.stubbing.Answer;
import org.mockito.verification.VerificationMode;
import uk.co.real_logic.fix_gateway.messages.MessageHeaderDecoder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.ABORT;
import static io.aeron.logbuffer.ControlledFragmentHandler.Action.CONTINUE;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;
import static uk.co.real_logic.fix_gateway.engine.EngineConfiguration.DEFAULT_SLOW_CONSUMER_TIMEOUT_IN_MS;
import static uk.co.real_logic.fix_gateway.messages.DisconnectReason.SLOW_CONSUMER;
import static uk.co.real_logic.fix_gateway.protocol.GatewayPublication.FRAME_SIZE;

public class SenderEndPointTest
{
    private static final long CONNECTION_ID = 1;
    private static final int LIBRARY_ID = 2;
    private static final int MAX_BYTES_IN_BUFFER = 1024;
    private static final int HEADER_LENGTH = MessageHeaderDecoder.ENCODED_LENGTH;

    private static final long POSITION = 8 * 1024;
    private static final int BODY_LENGTH = 84;
    private static final int LENGTH = FRAME_SIZE + BODY_LENGTH;

    private TcpChannel tcpChannel = mock(TcpChannel.class);
    private AtomicCounter bytesInBuffer = fakeCounter();
    private AtomicCounter invalidLibraryAttempts = mock(AtomicCounter.class);
    private ErrorHandler errorHandler = mock(ErrorHandler.class);
    private Framer framer = mock(Framer.class);
    private ByteBuffer byteBuffer = ByteBuffer.allocate(1024);
    private UnsafeBuffer buffer = new UnsafeBuffer(byteBuffer);

    private SenderEndPoint endPoint = new SenderEndPoint(
        CONNECTION_ID,
        LIBRARY_ID,
        tcpChannel,
        bytesInBuffer,
        invalidLibraryAttempts,
        errorHandler,
        framer,
        MAX_BYTES_IN_BUFFER,
        DEFAULT_SLOW_CONSUMER_TIMEOUT_IN_MS,
        0);

    @Test
    public void shouldRetrySlowConsumerMessage() throws IOException
    {
        becomeSlowConsumer();

        channelWillWrite(BODY_LENGTH);

        onSlowConsumerMessageFragment(CONTINUE);
        byteBufferWritten();
        assertBytesInBuffer(0);
    }

    @Test
    public void shouldBeAbleToFragmentSlowConsumerRetries() throws IOException
    {
        becomeSlowConsumer();

        final int firstWrites = 41;
        final int remaining = BODY_LENGTH - firstWrites;

        channelWillWrite(firstWrites);
        onSlowConsumerMessageFragment(ABORT);
        byteBufferWritten();
        assertBytesInBuffer(remaining);

        channelWillWrite(remaining);
        onSlowConsumerMessageFragment(CONTINUE);
        byteBufferWritten();
        assertBytesInBuffer(0);
        verifyNoMoreErrors();
    }

    @Test
    public void shouldDisconnectSlowConsumerAfterTimeout() throws IOException
    {
        long timeInMs = 100;
        long position = POSITION;
        channelWillWrite(BODY_LENGTH);
        onNormalMessage(timeInMs, position);

        timeInMs += 100;
        position += BODY_LENGTH;

        channelWillWrite(0);
        onNormalMessage(timeInMs, position);

        timeInMs += DEFAULT_SLOW_CONSUMER_TIMEOUT_IN_MS  + 1;

        endPoint.checkTimeouts(timeInMs);

        verifySlowConsumerDisconnect(times(1));
        errorLogged();
    }

    @Test
    public void shouldNotDisconnectSlowConsumerBeforeTimeout() throws IOException
    {
        long timeInMs = 100;
        channelWillWrite(BODY_LENGTH);
        onNormalMessage(timeInMs, POSITION);

        timeInMs += (DEFAULT_SLOW_CONSUMER_TIMEOUT_IN_MS  - 1);

        endPoint.checkTimeouts(timeInMs);

        verifySlowConsumerDisconnect(never());
        verifyNoMoreErrors();
    }

    @Test
    public void shouldNotDisconnectAtStartDueToTimeout()
    {
        final long timeInMs = DEFAULT_SLOW_CONSUMER_TIMEOUT_IN_MS  - 1;

        endPoint.checkTimeouts(timeInMs);

        verifySlowConsumerDisconnect(never());
        verifyNoMoreErrors();
    }

    @Test
    public void shouldNotDisconnectRegularConsumerDueToTimeout() throws IOException
    {
        long timeInMs = 100;
        channelWillWrite(BODY_LENGTH);
        onNormalMessage(timeInMs, POSITION);

        timeInMs += (DEFAULT_SLOW_CONSUMER_TIMEOUT_IN_MS  + 1);

        endPoint.checkTimeouts(timeInMs);

        verifySlowConsumerDisconnect(never());
        verifyNoMoreErrors();
    }

    @Test
    public void shouldDisconnectSlowConsumerAfterTimeoutAfterFragment() throws IOException
    {
        becomeSlowConsumer();

        final int firstWrites = 41;

        channelWillWrite(firstWrites);
        onSlowConsumerMessageFragment(ABORT);

        channelWillWrite(0);
        onSlowConsumerMessageFragment(ABORT);

        endPoint.checkTimeouts(DEFAULT_SLOW_CONSUMER_TIMEOUT_IN_MS  + 101);

        verifySlowConsumerDisconnect(times(1));
        errorLogged();
    }

    private void errorLogged()
    {
        verify(errorHandler).onError(any(IllegalStateException.class));
    }

    public void verifyNoMoreErrors()
    {
        verifyNoMoreInteractions(errorHandler);
    }

    private void onNormalMessage(final long timeInMs, final long position)
    {
        endPoint.onNormalFramedMessage(LIBRARY_ID, buffer, 0, BODY_LENGTH, position, timeInMs);
    }

    private void verifySlowConsumerDisconnect(final VerificationMode times)
    {
        verify(framer, times).onDisconnect(LIBRARY_ID, CONNECTION_ID, SLOW_CONSUMER);
    }

    private void byteBufferWritten() throws IOException
    {
        verify(tcpChannel).write(byteBuffer);
        reset(tcpChannel);
    }

    private void assertBytesInBuffer(final int bytes)
    {
        assertEquals(bytes, bytesInBuffer.get());
    }

    private void onSlowConsumerMessageFragment(final Action expected)
    {
        final Action action = endPoint.onSlowConsumerMessageFragment(
            buffer,
            HEADER_LENGTH,
            LENGTH,
            POSITION,
            BODY_LENGTH,
            LIBRARY_ID,
            100);
        assertEquals(expected, action);
    }

    private void becomeSlowConsumer()
    {
        endPoint.becomeSlowConsumer(0, BODY_LENGTH, POSITION);
    }

    private void channelWillWrite(final int bodyLength) throws IOException
    {
        when(tcpChannel.write(byteBuffer)).thenReturn(bodyLength);
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

        final Answer<?> add = inv -> value.getAndAdd(inv.getArgument(0));

        when(atomicCounter.get()).then(get);
        when(atomicCounter.getWeak()).then(get);

        doAnswer(set).when(atomicCounter).set(anyLong());
        doAnswer(set).when(atomicCounter).setOrdered(anyLong());
        doAnswer(set).when(atomicCounter).setWeak(anyLong());

        when(atomicCounter.add(anyLong())).then(add);
        when(atomicCounter.addOrdered(anyLong())).then(add);

        return atomicCounter;
    }

}
