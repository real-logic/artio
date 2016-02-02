/*
 * Copyright 2015 Real Logic Ltd.
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
package uk.co.real_logic.fix_gateway.engine.logger;

import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.ErrorHandler;
import uk.co.real_logic.agrona.IoUtil;
import uk.co.real_logic.agrona.collections.Long2LongHashMap;
import uk.co.real_logic.agrona.concurrent.AtomicBuffer;
import uk.co.real_logic.fix_gateway.decoder.HeaderDecoder;
import uk.co.real_logic.fix_gateway.messages.*;
import uk.co.real_logic.fix_gateway.util.AsciiBuffer;
import uk.co.real_logic.fix_gateway.util.MutableAsciiBuffer;

import java.util.concurrent.locks.LockSupport;

import static uk.co.real_logic.agrona.BitUtil.SIZE_OF_INT;

/**
 * Stores a cache of the last sent sequence number.
 * <p>
 * Each instance is not thread-safe, however, they can share a common
 * off-heap in a threadsafe manner.
 *
 * Layout:
 *
 * Message Header
 * Known Stream Position
 * Series of LastKnownSequenceNumber records
 */
public class SequenceNumberIndex implements Index
{
    /** We are up to date with the record, but we don't know about this session */
    public static final int UNKNOWN_SESSION = -1;
    /** The index behind the required position when indexing, please backoff and try again */
    public static final int STREAM_POSITION_BEHIND = -2;

    private static final int KNOWN_STREAM_POSITION_INDEX = MessageHeaderDecoder.ENCODED_LENGTH;
    private static final int HEADER_SIZE = KNOWN_STREAM_POSITION_INDEX + SIZE_OF_INT;
    private static final int RECORD_SIZE = LastKnownSequenceNumberDecoder.BLOCK_LENGTH;

    private static final int LOCK_OFFSET = 12;
    private static final int UNUSED = 0;
    private static final int UNACQUIRED = 1;
    private static final int AS_WRITER = 2;
    private static final int AS_READER = 3;

    private static final long MISSING_RECORD = -1L;

    private final MessageHeaderDecoder frameHeaderDecoder = new MessageHeaderDecoder();
    private final FixMessageDecoder messageFrame = new FixMessageDecoder();
    private final HeaderDecoder fixHeader = new HeaderDecoder();

    private final AsciiBuffer asciiBuffer = new MutableAsciiBuffer();
    private final MessageHeaderDecoder fileHeaderDecoder = new MessageHeaderDecoder();
    private final MessageHeaderEncoder fileHeaderEncoder = new MessageHeaderEncoder();
    private final LastKnownSequenceNumberEncoder lastKnownEncoder = new LastKnownSequenceNumberEncoder();
    private final LastKnownSequenceNumberDecoder lastKnownDecoder = new LastKnownSequenceNumberDecoder();
    private final int actingBlockLength = lastKnownEncoder.sbeBlockLength();
    private final int actingVersion = lastKnownEncoder.sbeSchemaVersion();
    private final Long2LongHashMap recordOffsets = new Long2LongHashMap(MISSING_RECORD);

    private final AtomicBuffer outputBuffer;
    private final ErrorHandler errorHandler;
    private final boolean isWriter;

    public static SequenceNumberIndex forWriting(final AtomicBuffer outputBuffer, final ErrorHandler errorHandler)
    {
        final SequenceNumberIndex index = new SequenceNumberIndex(outputBuffer, errorHandler, true);
        index.initialise();
        return index;
    }

    public static SequenceNumberIndex forReading(final AtomicBuffer outputBuffer, final ErrorHandler errorHandler)
    {
        final SequenceNumberIndex index = new SequenceNumberIndex(outputBuffer, errorHandler, false);
        index.validateBuffer();
        return index;
    }

    SequenceNumberIndex(final AtomicBuffer outputBuffer, final ErrorHandler errorHandler, final boolean isWriter)
    {
        this.outputBuffer = outputBuffer;
        this.errorHandler = errorHandler;
        this.isWriter = isWriter;
    }

    @Override
    public void indexRecord(
        final DirectBuffer buffer, final int srcOffset, final int length, final int streamId, final int aeronSessionId)
    {
        int offset = srcOffset;
        frameHeaderDecoder.wrap(buffer, offset);
        if (frameHeaderDecoder.templateId() == FixMessageEncoder.TEMPLATE_ID)
        {
            final int actingBlockLength = frameHeaderDecoder.blockLength();
            offset += frameHeaderDecoder.encodedLength();
            messageFrame.wrap(buffer, offset, actingBlockLength, frameHeaderDecoder.version());

            offset += actingBlockLength + 2;

            asciiBuffer.wrap(buffer);
            fixHeader.decode(asciiBuffer, offset, messageFrame.bodyLength());

            final int msgSeqNum = fixHeader.msgSeqNum();
            final long sessionId = messageFrame.session();

            if (!saveRecord(msgSeqNum, sessionId))
            {
                return;
            }

            knownStreamPosition(srcOffset + length);
        }
    }

    public int lastKnownSequenceNumber(final long sessionId)
    {
        final int lastRecordOffset = outputBuffer.capacity() - RECORD_SIZE;
        int position = HEADER_SIZE;
        while (position <= lastRecordOffset)
        {
            lastKnownDecoder.wrap(outputBuffer, position, actingBlockLength, actingVersion);

            if (lastKnownDecoder.sessionId() == sessionId)
            {
                acquireLock(position, AS_READER);
                final int sequenceNumber = lastKnownDecoder.sequenceNumber();
                releaseLock(position);
                return sequenceNumber;
            }

            position += RECORD_SIZE;
        }

        return UNKNOWN_SESSION;
    }

    @Override
    public void close()
    {
        if (isWriter)
        {
            IoUtil.unmap(outputBuffer.byteBuffer());
        }
    }

    private boolean saveRecord(final int msgSeqNum, final long sessionId)
    {
        int position = (int) recordOffsets.get(sessionId);
        if (position == MISSING_RECORD)
        {
            final int lastRecordOffset = outputBuffer.capacity() - RECORD_SIZE;
            position = HEADER_SIZE;
            while (position <= lastRecordOffset)
            {
                final long lock = lockVolatile(position);
                if (lock == UNUSED)
                {
                    createNewRecord(msgSeqNum, sessionId, position);
                    return true;
                }
                else
                {
                    lastKnownDecoder.wrap(outputBuffer, position, actingBlockLength, actingVersion);
                    if (lastKnownDecoder.sessionId() == sessionId)
                    {
                        updateRecord(msgSeqNum, position);
                        return true;
                    }
                }

                position += RECORD_SIZE;
            }
        }
        else
        {
            updateRecord(msgSeqNum, position);
            return true;
        }

        errorHandler.onError(new IllegalStateException("Unable to claim an position"));
        return false;
    }

    private void createNewRecord(final int msgSeqNum, final long sessionId, final int position)
    {
        lastKnownEncoder
            .wrap(outputBuffer, position)
            .sessionId(sessionId)
            .sequenceNumber(msgSeqNum)
            .lock(UNACQUIRED);

        recordOffsets.put(sessionId, position);
    }

    private void updateRecord(final int msgSeqNum, final int position)
    {
        acquireLock(position, AS_WRITER);

        lastKnownEncoder
            .wrap(outputBuffer, position)
            .sequenceNumber(msgSeqNum);

        releaseLock(position);
    }

    private void initialise()
    {
        if (knownStreamPosition() == 0)
        {
            fileHeaderEncoder
                .wrap(outputBuffer, 0)
                .blockLength(lastKnownEncoder.sbeBlockLength())
                .templateId(lastKnownEncoder.sbeTemplateId())
                .schemaId(lastKnownEncoder.sbeSchemaId())
                .version(lastKnownEncoder.sbeSchemaVersion());
        }
        else
        {
            validateBuffer();
        }
    }

    private void validateBuffer()
    {
        fileHeaderDecoder.wrap(outputBuffer, 0);
        validateField(lastKnownEncoder.sbeSchemaId(), fileHeaderDecoder.schemaId(), "Schema Id");
        validateField(actingVersion, fileHeaderDecoder.version(), "Schema Version");
        validateField(actingBlockLength, fileHeaderDecoder.blockLength(), "Block Length");
    }

    private void validateField(final int expected, final int read, final String name)
    {
        if (read != expected)
        {
            throw new IllegalStateException(
                String.format("Wrong %s: expected %d and got %d", name, expected, read));
        }
    }

    private void knownStreamPosition(final int position)
    {
        outputBuffer.putIntOrdered(KNOWN_STREAM_POSITION_INDEX, position);
    }

    private int knownStreamPosition()
    {
        return outputBuffer.getIntVolatile(KNOWN_STREAM_POSITION_INDEX);
    }

    private int lockVolatile(final int recordOffset)
    {
        return outputBuffer.getIntVolatile(recordOffset + LOCK_OFFSET);
    }

    private void acquireLock(final int recordOffset, final int as)
    {
        while (!outputBuffer.compareAndSetInt(recordOffset + LOCK_OFFSET, UNACQUIRED, as))
        {
            System.out.println(outputBuffer.getInt(recordOffset + LOCK_OFFSET));
            LockSupport.parkNanos(1000L);
        }
    }

    private void releaseLock(final int recordOffset)
    {
        outputBuffer.putIntVolatile(recordOffset + LOCK_OFFSET, UNACQUIRED);
    }
}
