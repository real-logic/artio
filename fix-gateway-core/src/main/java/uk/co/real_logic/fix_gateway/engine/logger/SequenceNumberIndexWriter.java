/*
 * Copyright 2015=2016 Real Logic Ltd.
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

import static uk.co.real_logic.fix_gateway.engine.logger.SequenceNumberIndexDescriptor.RECORD_SIZE;
import static uk.co.real_logic.fix_gateway.messages.LastKnownSequenceNumberEncoder.SCHEMA_VERSION;

// TODO: 3. apply the alignment and checksumming rules
// TODO: 4. only update the file buffer when you rotate the term buffer,
// TODO: 5. rescan the last term buffer upon restart
public class SequenceNumberIndexWriter implements Index
{
    private static final long MISSING_RECORD = -1L;
    private static final int SEQUENCE_NUMBER_OFFSET = 8;

    private final MessageHeaderDecoder frameHeaderDecoder = new MessageHeaderDecoder();
    private final FixMessageDecoder messageFrame = new FixMessageDecoder();
    private final HeaderDecoder fixHeader = new HeaderDecoder();

    private final AsciiBuffer asciiBuffer = new MutableAsciiBuffer();
    private final MessageHeaderDecoder fileHeaderDecoder = new MessageHeaderDecoder();
    private final MessageHeaderEncoder fileHeaderEncoder = new MessageHeaderEncoder();
    private final LastKnownSequenceNumberEncoder lastKnownEncoder = new LastKnownSequenceNumberEncoder();
    private final LastKnownSequenceNumberDecoder lastKnownDecoder = new LastKnownSequenceNumberDecoder();
    private final Long2LongHashMap recordOffsets = new Long2LongHashMap(MISSING_RECORD);

    private final AtomicBuffer outputBuffer;
    private final ErrorHandler errorHandler;

    public SequenceNumberIndexWriter(final AtomicBuffer outputBuffer, final ErrorHandler errorHandler)
    {
        this.outputBuffer = outputBuffer;
        this.errorHandler = errorHandler;
        initialise();
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

    @Override
    public void close()
    {
        IoUtil.unmap(outputBuffer.byteBuffer());
    }

    private void saveRecord(final int newSequenceNumber, final long sessionId)
    {
        int position = (int) recordOffsets.get(sessionId);
        if (position == MISSING_RECORD)
        {
            final int lastRecordOffset = outputBuffer.capacity() - RECORD_SIZE;
            position = SequenceNumberIndexDescriptor.HEADER_SIZE;
            while (position <= lastRecordOffset)
            {
                lastKnownDecoder.wrap(outputBuffer, position, RECORD_SIZE, SCHEMA_VERSION);
                if (lastKnownDecoder.sequenceNumber() == 0)
                {
                    createNewRecord(newSequenceNumber, sessionId, position);
                    return;
                }
                else if (lastKnownDecoder.sessionId() == sessionId)
                {
                    sequenceNumberOrdered(position, newSequenceNumber);
                    return;
                }

                position += RECORD_SIZE;
            }
        }
        else
        {
            sequenceNumberOrdered(position, newSequenceNumber);
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

    public void sequenceNumberOrdered(final int recordOffset, final int value)
    {
        outputBuffer.putIntOrdered(recordOffset + SEQUENCE_NUMBER_OFFSET, value);
    }
}
