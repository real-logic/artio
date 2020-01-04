/*
 * Copyright 2015=2016 Real Logic Limited.
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
package uk.co.real_logic.artio.engine.logger;

import org.agrona.ErrorHandler;
import org.agrona.concurrent.AtomicBuffer;
import uk.co.real_logic.artio.engine.SectorFramer;
import uk.co.real_logic.artio.messages.MessageHeaderDecoder;
import uk.co.real_logic.artio.storage.messages.LastKnownSequenceNumberDecoder;
import uk.co.real_logic.artio.storage.messages.LastKnownSequenceNumberEncoder;

import static uk.co.real_logic.artio.engine.SectorFramer.OUT_OF_SPACE;
import static uk.co.real_logic.artio.engine.SessionInfo.UNK_SESSION;
import static uk.co.real_logic.artio.engine.logger.SequenceNumberIndexDescriptor.*;
import static uk.co.real_logic.artio.storage.messages.LastKnownSequenceNumberEncoder.BLOCK_LENGTH;
import static uk.co.real_logic.artio.storage.messages.LastKnownSequenceNumberEncoder.SCHEMA_VERSION;

public class SequenceNumberIndexReader
{
    private final MessageHeaderDecoder fileHeaderDecoder = new MessageHeaderDecoder();
    private final LastKnownSequenceNumberDecoder lastKnownDecoder = new LastKnownSequenceNumberDecoder();
    private final AtomicBuffer inMemoryBuffer;
    private final SectorFramer sectorFramer;
    private final IndexedPositionReader positions;
    private final ErrorHandler errorHandler;

    public SequenceNumberIndexReader(final AtomicBuffer inMemoryBuffer, final ErrorHandler errorHandler)
    {
        this.inMemoryBuffer = inMemoryBuffer;
        this.errorHandler = errorHandler;
        final int positionTableOffset = positionTableOffset(inMemoryBuffer.capacity());
        sectorFramer = new SectorFramer(positionTableOffset);
        validateBuffer();
        positions = new IndexedPositionReader(positionsBuffer(inMemoryBuffer, positionTableOffset));
    }

    public int lastKnownSequenceNumber(final long sessionId)
    {
        int position = SequenceNumberIndexDescriptor.HEADER_SIZE;
        while (true)
        {
            position = sectorFramer.claim(position, RECORD_SIZE);
            if (position == OUT_OF_SPACE)
            {
                return UNK_SESSION;
            }

            lastKnownDecoder.wrap(inMemoryBuffer, position, BLOCK_LENGTH, SCHEMA_VERSION);

            if (lastKnownDecoder.sessionId() == sessionId)
            {
                return lastKnownDecoder.sequenceNumber();
            }

            position += RECORD_SIZE;
        }
    }

    public long indexedPosition(final int aeronSessionId)
    {
        return positions.indexedPosition(aeronSessionId);
    }

    private void validateBuffer()
    {
        LoggerUtil.validateBuffer(
            inMemoryBuffer,
            fileHeaderDecoder,
            LastKnownSequenceNumberEncoder.SCHEMA_ID,
            errorHandler);
    }
}
