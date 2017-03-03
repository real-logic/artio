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
package uk.co.real_logic.fix_gateway.engine.logger;

import org.agrona.ErrorHandler;
import org.agrona.collections.Int2IntHashMap;
import org.agrona.concurrent.AtomicBuffer;
import uk.co.real_logic.fix_gateway.engine.ChecksumFramer;
import uk.co.real_logic.fix_gateway.messages.MessageHeaderDecoder;
import uk.co.real_logic.fix_gateway.messages.MessageHeaderEncoder;
import uk.co.real_logic.fix_gateway.storage.messages.IndexedPositionDecoder;
import uk.co.real_logic.fix_gateway.storage.messages.IndexedPositionEncoder;

import static uk.co.real_logic.fix_gateway.engine.SectorFramer.OUT_OF_SPACE;

/**
 * Writes out a log of the stream positions that we have indexed up to.
 * Not thread safe, but writes to a thread safe buffer.
 */
class IndexedPositionWriter
{
    static final int HEADER_LENGTH = MessageHeaderEncoder.ENCODED_LENGTH;
    static final int RECORD_LENGTH = IndexedPositionEncoder.BLOCK_LENGTH;
    static final int POSITION_OFFSET = 8;

    private static final int MISSING_RECORD = -1;

    private final IndexedPositionEncoder encoder = new IndexedPositionEncoder();
    private final int actingBlockLength = encoder.sbeBlockLength();
    private final int actingVersion = encoder.sbeSchemaVersion();
    private final IndexedPositionDecoder decoder = new IndexedPositionDecoder();
    private final Int2IntHashMap recordOffsets = new Int2IntHashMap(MISSING_RECORD);
    private final AtomicBuffer buffer;
    private final ErrorHandler errorHandler;
    private final ChecksumFramer checksumFramer;

    IndexedPositionWriter(final AtomicBuffer buffer, final ErrorHandler errorHandler, final int errorReportingOffset)
    {
        this.buffer = buffer;
        this.errorHandler = errorHandler;
        checksumFramer = new ChecksumFramer(
            buffer, buffer.capacity(), errorHandler, errorReportingOffset, "IndexedPosition");
        setupHeader();
    }

    private void setupHeader()
    {
        final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
        final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();

        messageHeaderDecoder.wrap(buffer, 0);
        if (messageHeaderDecoder.blockLength() == 0)
        {
            messageHeaderEncoder
                .wrap(buffer, 0)
                .templateId(encoder.sbeTemplateId())
                .schemaId(encoder.sbeSchemaId())
                .blockLength(actingBlockLength)
                .version(actingVersion);
        }
        else
        {
            checksumFramer.validateCheckSums();
        }
    }

    void indexedUpTo(final int aeronSessionId, final long position)
    {
        final Int2IntHashMap recordOffsets = this.recordOffsets;

        int offset = recordOffsets.get(aeronSessionId);
        if (offset == MISSING_RECORD)
        {
            final IndexedPositionDecoder decoder = this.decoder;
            final int actingBlockLength = this.actingBlockLength;
            final int actingVersion = this.actingVersion;
            final AtomicBuffer buffer = this.buffer;

            offset = HEADER_LENGTH;
            while (true)
            {
                offset = checksumFramer.claim(offset, RECORD_LENGTH);
                if (position == OUT_OF_SPACE)
                {
                    errorHandler.onError(new IllegalStateException(String.format(
                        "Unable to record new session (%d), indexed position buffer full",
                        aeronSessionId)));
                    return;
                }

                decoder.wrap(buffer, offset, actingBlockLength, actingVersion);
                if (decoder.position() == 0)
                {
                    encoder.wrap(buffer, offset)
                        .sessionId(aeronSessionId);

                    recordOffsets.put(aeronSessionId, offset);
                    putPosition(position, buffer, offset);
                    return;
                }

                offset += RECORD_LENGTH;
            }
        }
        else
        {
            putPosition(position, buffer, offset);
        }
    }

    void close()
    {
        updateChecksums();
    }

    void updateChecksums()
    {
        checksumFramer.updateChecksums();
    }

    AtomicBuffer buffer()
    {
        return buffer;
    }

    private void putPosition(final long position, final AtomicBuffer buffer, final int offset)
    {
        buffer.putLongVolatile(offset + POSITION_OFFSET, position);
    }
}
