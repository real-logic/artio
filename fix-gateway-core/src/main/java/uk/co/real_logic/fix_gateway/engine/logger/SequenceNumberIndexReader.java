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

import uk.co.real_logic.agrona.concurrent.AtomicBuffer;
import uk.co.real_logic.fix_gateway.messages.LastKnownSequenceNumberDecoder;
import uk.co.real_logic.fix_gateway.messages.LastKnownSequenceNumberEncoder;
import uk.co.real_logic.fix_gateway.messages.MessageHeaderDecoder;

import static uk.co.real_logic.fix_gateway.messages.LastKnownSequenceNumberEncoder.BLOCK_LENGTH;
import static uk.co.real_logic.fix_gateway.messages.LastKnownSequenceNumberEncoder.SCHEMA_VERSION;

public class SequenceNumberIndexReader
{
    /** We are up to date with the record, but we don't know about this session */
    public static final int UNKNOWN_SESSION = -1;

    private final MessageHeaderDecoder fileHeaderDecoder = new MessageHeaderDecoder();
    private final LastKnownSequenceNumberDecoder lastKnownDecoder = new LastKnownSequenceNumberDecoder();
    private final AtomicBuffer outputBuffer;

    public SequenceNumberIndexReader(final AtomicBuffer outputBuffer)
    {
        this.outputBuffer = outputBuffer;
        validateBuffer();
    }

    public int lastKnownSequenceNumber(final long sessionId)
    {
        final int lastRecordOffset = outputBuffer.capacity() - SequenceNumberIndexDescriptor.RECORD_SIZE;
        int position = SequenceNumberIndexDescriptor.HEADER_SIZE;
        while (position <= lastRecordOffset)
        {
            lastKnownDecoder.wrap(outputBuffer, position, BLOCK_LENGTH, SCHEMA_VERSION);

            if (lastKnownDecoder.sessionId() == sessionId)
            {
                return lastKnownDecoder.sequenceNumber();
            }

            position += SequenceNumberIndexDescriptor.RECORD_SIZE;
        }

        return UNKNOWN_SESSION;
    }

    private void validateBuffer()
    {
        LoggerUtil.validateBuffer(
            outputBuffer,
            fileHeaderDecoder,
            LastKnownSequenceNumberEncoder.SCHEMA_ID,
            SCHEMA_VERSION,
            BLOCK_LENGTH);
    }
}
