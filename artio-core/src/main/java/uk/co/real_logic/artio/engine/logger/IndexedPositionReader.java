/*
 * Copyright 2015-2024 Real Logic Limited, Adaptive Financial Consulting Ltd.
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

import org.agrona.concurrent.AtomicBuffer;
import uk.co.real_logic.artio.engine.SectorFramer;
import uk.co.real_logic.artio.messages.MessageHeaderDecoder;
import uk.co.real_logic.artio.storage.messages.IndexedPositionDecoder;

import static uk.co.real_logic.artio.engine.SectorFramer.OUT_OF_SPACE;
import static uk.co.real_logic.artio.engine.logger.IndexedPositionWriter.*;

/**
 * Reads out the position at which indexes have been written up to.
 */
class IndexedPositionReader
{
    static final long UNKNOWN_POSITION = -1;

    private final IndexedPositionDecoder decoder = new IndexedPositionDecoder();

    private final int actingBlockLength;
    private final int actingVersion;
    private final AtomicBuffer buffer;
    private final SectorFramer sectorFramer;

    IndexedPositionReader(final AtomicBuffer buffer)
    {
        this.buffer = buffer;
        final MessageHeaderDecoder messageHeader = new MessageHeaderDecoder();
        messageHeader.wrap(buffer, 0);
        actingBlockLength = messageHeader.blockLength();
        actingVersion = messageHeader.version();
        sectorFramer = new SectorFramer(buffer.capacity());
    }

    long indexedPosition(final long recordingId)
    {
        final IndexedPositionDecoder decoder = this.decoder;
        final int actingBlockLength = this.actingBlockLength;
        final int actingVersion = this.actingVersion;
        final AtomicBuffer buffer = this.buffer;

        int offset = HEADER_LENGTH;
        while (true)
        {
            offset = sectorFramer.claim(offset, RECORD_LENGTH);
            if (offset == OUT_OF_SPACE)
            {
                return UNKNOWN_POSITION;
            }

            decoder.wrap(buffer, offset, actingBlockLength, actingVersion);
            final long position = buffer.getLongVolatile(offset + POSITION_OFFSET);
            if (position == 0)
            {
                return UNKNOWN_POSITION;
            }
            if (decoder.recordingId() == recordingId)
            {
                return position;
            }

            offset += RECORD_LENGTH;
        }
    }

    /**
     * Reads the last position that has been indexed.
     *
     * @param consumer a callback that receives each session id and position
     */
    void readLastPosition(final IndexedPositionConsumer consumer)
    {
        final IndexedPositionDecoder decoder = this.decoder;
        final int actingBlockLength = this.actingBlockLength;
        final int actingVersion = this.actingVersion;
        final AtomicBuffer buffer = this.buffer;

        int offset = HEADER_LENGTH;
        while (true)
        {
            offset = sectorFramer.claim(offset, RECORD_LENGTH);
            if (offset == OUT_OF_SPACE)
            {
                return;
            }

            decoder.wrap(buffer, offset, actingBlockLength, actingVersion);
            final int sessionId = decoder.sessionId();
            final long recordingId = decoder.recordingId();
            if (sessionId != 0)
            {
                consumer.accept(sessionId, recordingId, decoder.position());
            }

            offset += RECORD_LENGTH;
        }
    }
}
