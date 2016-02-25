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

/**
 * Stores a cache of the last sent sequence number.
 * <p>
 * Each instance is not thread-safe, however, they can share a common
 * off-heap in a single-writer threadsafe manner.
 *
 * Layout:
 *
 * Message Header
 * Known Stream Position
 * Series of LastKnownSequenceNumber records
 */
// TODO: 1. remove the lock and write the session id last and ordered when writing the record and put the
// sequence number ordered, padding to 8 byte alignment, use offset on the SBE schema
// TODO: 2. split out the reader/writer/descriptor
// TODO: 3. apply the alignment and checksumming rules
// TODO: 4. only update the file buffer when you rotate the term buffer,
// TODO: 5. rescan the last term buffer upon restart
public class SequenceNumberIndex implements Index
{
    /** We are up to date with the record, but we don't know about this session */
    public static final int UNKNOWN_SESSION = -1;

    private static final int HEADER_SIZE = MessageHeaderDecoder.ENCODED_LENGTH;
    private static final int RECORD_SIZE = LastKnownSequenceNumberDecoder.BLOCK_LENGTH;
    private static final int SEQUENCE_NUMBER_OFFSET = 8;

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
        final DirectBuffer buffer,
        final int srcOffset,
        final int length,
        final int streamId,
        final int aeronSessionId,
        final long position)
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

            saveRecord(msgSeqNum, sessionId);
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
                return lastKnownDecoder.sequenceNumber();
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

    private void saveRecord(final int newSequenceNumber, final long sessionId)
    {
        int position = (int) recordOffsets.get(sessionId);
        if (position == MISSING_RECORD)
        {
            final int lastRecordOffset = outputBuffer.capacity() - RECORD_SIZE;
            position = HEADER_SIZE;
            while (position <= lastRecordOffset)
            {
                lastKnownDecoder.wrap(outputBuffer, position, actingBlockLength, actingVersion);
                if (lastKnownDecoder.sequenceNumber() == 0)
                {
                    createNewRecord(newSequenceNumber, sessionId, position);
                    return;
                }
                else if (lastKnownDecoder.sessionId() == sessionId)
                {
                    updateRecord(position, newSequenceNumber);
                    return;
                }

                position += RECORD_SIZE;
            }
        }
        else
        {
            updateRecord(position, newSequenceNumber);
            return;
        }

        errorHandler.onError(new IllegalStateException("Unable to claim an position"));
    }

    private void createNewRecord(final int sequenceNumber, final long sessionId, final int position)
    {
        recordOffsets.put(sessionId, position);
        lastKnownEncoder
            .wrap(outputBuffer, position)
            .sessionId(sessionId);
        sequenceNumberOrdered(position, sequenceNumber);
    }

    private void updateRecord(final int position, final int sequenceNumber)
    {
        sequenceNumberOrdered(position, sequenceNumber);
    }

    private void initialise()
    {
        LoggerUtil.initialiseBuffer(
            outputBuffer,
            fileHeaderEncoder,
            fileHeaderDecoder,
            lastKnownEncoder.sbeSchemaId(),
            lastKnownEncoder.sbeTemplateId(),
            lastKnownEncoder.sbeSchemaVersion(),
            lastKnownEncoder.sbeBlockLength());
    }

    private void validateBuffer()
    {
        LoggerUtil.validateBuffer(
            outputBuffer,
            fileHeaderDecoder,
            lastKnownEncoder.sbeSchemaId(),
            actingVersion,
            actingBlockLength);
    }

    public void sequenceNumberOrdered(final int recordOffset, final int value)
    {
        outputBuffer.putIntOrdered(recordOffset + SEQUENCE_NUMBER_OFFSET, value);
    }
}
