/*
 * Copyright 2023 Adaptive Financial Consulting Ltd.
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
import io.aeron.Publication;
import io.aeron.logbuffer.BufferClaim;
import io.aeron.logbuffer.ControlledFragmentHandler.Action;
import io.aeron.protocol.DataHeaderFlyweight;
import org.agrona.ErrorHandler;
import org.agrona.collections.IntHashSet;
import org.agrona.collections.MutableInteger;
import org.agrona.concurrent.UnsafeBuffer;
import org.agrona.concurrent.status.AtomicCounter;
import org.junit.Before;
import org.junit.Test;
import uk.co.real_logic.artio.engine.MessageTimingHandler;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import static io.aeron.logbuffer.ControlledFragmentHandler.Action.ABORT;
import static org.agrona.BitUtil.SIZE_OF_INT;
import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static uk.co.real_logic.artio.fixp.AbstractFixPOffsets.templateId;
import static uk.co.real_logic.artio.fixp.AbstractFixPParser.STANDARD_TEMPLATE_ID_OFFSET;
import static uk.co.real_logic.artio.fixp.SimpleOpenFramingHeader.*;

public class ImplicitFixPSenderEndPointPropertyTest
{
    private static final int CONNECTION_ID = 1;
    private static final int LIBRARY_ID = 2;
    private static final int CAPACITY = 256;
    private static final int MSG_SIZE = 64;
    private static final int HEADER_LENGTH = 8;

    private static final int TEMPLATE_INDEX = SOFH_LENGTH + STANDARD_TEMPLATE_ID_OFFSET;
    private static final int START_INDEX = SOFH_LENGTH + HEADER_LENGTH;

    private static final int SEQUENCE_TEMPLATE_ID = 9;
    private static final int RETRANSMISSION_TEMPLATE_ID = 13;
    private static final int ER_TEMPLATE_ID = 200;

    private static final int CYCLE_LIMIT = 500;

    private final TcpChannel channel = mock(TcpChannel.class);
    private final ErrorHandler errorHandler = mock(ErrorHandler.class);
    private final ExclusivePublication inboundPublication = mock(ExclusivePublication.class);
    private final ReproductionLogWriter reproductionPublication = mock(ReproductionLogWriter.class);
    private final MessageTimingHandler timingHandler = mock(MessageTimingHandler.class);
    private final FixPSenderEndPoints fixPSenderEndPoints = new FixPSenderEndPoints();
    private final UnsafeBuffer buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(CAPACITY));
    private final AtomicCounter bytesInBuffer = new AtomicCounter(new UnsafeBuffer(new byte[128]), 0);
    private final Framer framer = mock(Framer.class);

    private final FixPSenderEndPoint endPoint = FixPSenderEndPoint.of(
        CONNECTION_ID,
        channel,
        errorHandler,
        inboundPublication,
        reproductionPublication,
        LIBRARY_ID,
        timingHandler,
        false,
        STANDARD_TEMPLATE_ID_OFFSET,
        RETRANSMISSION_TEMPLATE_ID,
        fixPSenderEndPoints,
        bytesInBuffer,
        CAPACITY,
        framer,
        mock(FixPReceiverEndPoint.class));

    private final List<Object> liveMessages = new ArrayList<>();
    private final MutableInteger liveCursor = new MutableInteger();
    private final List<Object> replayMessages = new ArrayList<>();
    private final MutableInteger replayCursor = new MutableInteger();
    private final ByteBuffer sentMessages = ByteBuffer.allocate(1024);
    private final IntHashSet expectedErs = new IntHashSet();
    private final UnsafeBuffer wrapper = new UnsafeBuffer();
    private final Random random = new Random();
    private long seed;
    private int expectedMessageCount;
    private boolean libraryAwaitsReplayComplete;

    private void reset()
    {
        liveMessages.clear();
        liveCursor.set(0);
        replayMessages.clear();
        replayCursor.set(0);
        sentMessages.clear();
        expectedErs.clear();
        expectedMessageCount = 0;
    }

    @Before
    public void setUp() throws Exception
    {
        fixPSenderEndPoints.add(endPoint);

        when(channel.write(any(ByteBuffer.class), anyInt(), anyBoolean())).thenAnswer(invocation ->
        {
            final ByteBuffer bb = invocation.getArgument(0, ByteBuffer.class);
            int writeLength = bb.remaining();
            if (random.nextDouble() < 0.1)
            {
                writeLength = random.nextInt(writeLength);
            }
            for (int i = 0; i < writeLength; i++)
            {
                sentMessages.put(bb.get());
            }
            return writeLength;
        });

        when(inboundPublication.tryClaim(anyInt(), any(BufferClaim.class))).thenAnswer(invocation ->
        {
            if (random.nextDouble() < 0.1)
            {
                return Publication.BACK_PRESSURED;
            }
            libraryAwaitsReplayComplete = false;
            final Integer length = invocation.getArgument(0, Integer.class);
            final int bufferLength = length + DataHeaderFlyweight.HEADER_LENGTH;
            final BufferClaim claim = invocation.getArgument(1, BufferClaim.class);
            claim.wrap(new UnsafeBuffer(new byte[bufferLength]), 0, bufferLength);
            return (long)bufferLength;
        });
    }

    @Test
    public void shakeShouldSendAllLiveAndRetransmittedMessages()
    {
        try
        {
            final ThreadLocalRandom threadLocalRandom = ThreadLocalRandom.current();
            for (int i = 0; i < 1000; i++)
            {
                reset();
                random.setSeed(seed = threadLocalRandom.nextLong());
                shouldSendAllLiveAndRetransmittedMessages();
            }
        }
        catch (final Throwable t)
        {
            System.err.println("seed = " + seed + "L");
            throw t;
        }
    }

    private void shouldSendAllLiveAndRetransmittedMessages()
    {
        final int establishedSeqNo = 10;

        liveMessages.add(er(establishedSeqNo));
        liveMessages.add(er(establishedSeqNo + 1));
        liveMessages.add(er(establishedSeqNo + 2));

        final int idx = random.nextInt(liveMessages.size() + 2);
        if (idx <= liveMessages.size())
        {
            liveMessages.add(idx, retransmissionWithSequence(1, 3, establishedSeqNo + idx));

            replayMessages.add(er(1));
            replayMessages.add(er(2));
            replayMessages.add(er(3));
            replayMessages.add(new ReplayComplete());

            libraryAwaitsReplayComplete = true;
        }
        else
        {
            assertEquals(0, replayMessages.size());

            libraryAwaitsReplayComplete = false;
        }

        int cycles = 0;
        while (sentMessageCount() < expectedMessageCount || libraryAwaitsReplayComplete)
        {
            if (++cycles > CYCLE_LIMIT)
            {
                fail("cycle limit exceeded: messages=" + sentMessageCount() + "/" + expectedMessageCount +
                    " libraryAwaitsNotification=" + libraryAwaitsReplayComplete);
            }

            doWork();
        }

        int expectedSeq = establishedSeqNo;
        final IntHashSet actualErs = new IntHashSet();

        sentMessages.flip();
        wrapper.wrap(sentMessages);

        int offset = 0;
        while (offset < sentMessages.limit())
        {
            final int messageSize = readSofhMessageSize(wrapper, offset);
            assertNotEquals(0, messageSize);
            final int templateId = templateId(wrapper, offset, TEMPLATE_INDEX);
            switch (templateId)
            {
                case SEQUENCE_TEMPLATE_ID:
                case RETRANSMISSION_TEMPLATE_ID:
                    expectedSeq = wrapper.getInt(offset + START_INDEX, ByteOrder.LITTLE_ENDIAN);
                    break;

                case ER_TEMPLATE_ID:
                    final int seq = wrapper.getInt(offset + START_INDEX, ByteOrder.LITTLE_ENDIAN);
                    assertEquals(expectedSeq, seq);
                    expectedSeq++;
                    actualErs.add(seq);
                    break;
            }
            offset += messageSize;
        }

        assertEquals(expectedErs, actualErs);
    }

    private void doWork()
    {
        fixPSenderEndPoints.reattempt();
        pollMessages(liveMessages, liveCursor);
        pollMessages(replayMessages, replayCursor);
    }

    private void pollMessages(final List<Object> messages, final MutableInteger cursor)
    {
        final int size = messages.size();

        if (size == 0)
        {
            return;
        }

        final boolean retransmit = messages == replayMessages;
        final int remaining = size - cursor.get();
        final int count = random.nextInt(remaining + 1);
        for (int i = 0; i < count; i++)
        {
            final Object obj = messages.get(cursor.get());
            final Action action;
            if (obj instanceof Message)
            {
                final Message message = (Message)obj;
                wrapper.wrap(ByteBuffer.wrap(message.bytes));
                action = fixPSenderEndPoints.onMessage(CONNECTION_ID, wrapper, 0, retransmit);
            }
            else if (obj instanceof ReplayComplete)
            {
                action = fixPSenderEndPoints.onReplayComplete(CONNECTION_ID, 0);
            }
            else
            {
                throw new IllegalStateException("unknown type: " + obj);
            }
            if (action == ABORT)
            {
                break;
            }
            cursor.increment();
        }
    }

    private Message retransmissionWithSequence(final int nextSeqNo, final int count, final int seq)
    {
        int offset = 0;

        writeBinaryEntryPointSofh(buffer, offset, MSG_SIZE);
        buffer.putShort(offset + TEMPLATE_INDEX, (short)RETRANSMISSION_TEMPLATE_ID, ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(offset + START_INDEX, nextSeqNo, ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(offset + START_INDEX + SIZE_OF_INT, count, ByteOrder.LITTLE_ENDIAN);

        offset += MSG_SIZE;

        writeBinaryEntryPointSofh(buffer, offset, MSG_SIZE);
        buffer.putShort(offset + TEMPLATE_INDEX, (short)SEQUENCE_TEMPLATE_ID, ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(offset + START_INDEX, seq, ByteOrder.LITTLE_ENDIAN);

        final byte[] bytes = new byte[MSG_SIZE * 2];
        buffer.getBytes(0, bytes);

        expectedMessageCount += 2;

        return new Message(bytes, "Retransmission(" + nextSeqNo + "," + count + ") + Sequence(" + seq + ")");
    }

    private Message er(final int seqNo)
    {
        expectedErs.add(seqNo);

        final int offset = 0;
        writeBinaryEntryPointSofh(buffer, offset, MSG_SIZE);
        buffer.putShort(offset + TEMPLATE_INDEX, (short)ER_TEMPLATE_ID, ByteOrder.LITTLE_ENDIAN);
        buffer.putInt(offset + START_INDEX, seqNo, ByteOrder.LITTLE_ENDIAN);

        final byte[] bytes = new byte[MSG_SIZE];
        buffer.getBytes(offset, bytes);

        expectedMessageCount += 1;

        return new Message(bytes, "ER(" + seqNo + ")");
    }

    private int sentMessageCount()
    {
        return sentMessages.position() / MSG_SIZE;
    }

    private static final class Message
    {
        private final byte[] bytes;
        private final String label;

        private Message(final byte[] bytes, final String label)
        {
            this.bytes = bytes;
            this.label = label;
        }

        public String toString()
        {
            return label;
        }
    }

    private static final class ReplayComplete
    {
        public String toString()
        {
            return "ReplayComplete";
        }
    }
}
