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

import uk.co.real_logic.agrona.ErrorHandler;
import uk.co.real_logic.agrona.concurrent.AtomicBuffer;
import uk.co.real_logic.fix_gateway.messages.IndexedPositionDecoder;
import uk.co.real_logic.fix_gateway.messages.IndexedPositionEncoder;
import uk.co.real_logic.fix_gateway.messages.MessageHeaderDecoder;
import uk.co.real_logic.fix_gateway.messages.MessageHeaderEncoder;

/**
 * Writes out a log of the stream positions that we have indexed up to.
 * Not thread safe, but writes to a thread safe buffer.
 */
public class PositionsWriter
{
    static final int HEADER_LENGTH = MessageHeaderEncoder.ENCODED_LENGTH;
    static final int RECORD_LENGTH = IndexedPositionEncoder.BLOCK_LENGTH;
    static final int POSITION_OFFSET = 8;

    private final IndexedPositionEncoder encoder = new IndexedPositionEncoder();
    private final int actingBlockLength = encoder.sbeBlockLength();
    private final int actingVersion = encoder.sbeSchemaVersion();
    private final IndexedPositionDecoder decoder = new IndexedPositionDecoder();
    private final AtomicBuffer buffer;
    private final ErrorHandler errorHandler;

    public PositionsWriter(final AtomicBuffer buffer, final ErrorHandler errorHandler)
    {
        this.buffer = buffer;
        this.errorHandler = errorHandler;
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
    }

    public void indexedUpTo(final int streamId, final int aeronSessionId, final long position)
    {
        final IndexedPositionDecoder decoder = this.decoder;
        final int actingBlockLength = this.actingBlockLength;
        final int actingVersion = this.actingVersion;
        final AtomicBuffer buffer = this.buffer;
        final int lastIndex = buffer.capacity() - RECORD_LENGTH;

        int offset = HEADER_LENGTH;
        while (offset <= lastIndex)
        {
            decoder.wrap(buffer, offset, actingBlockLength, actingVersion);
            if (decoder.position() == 0)
            {
                encoder.wrap(buffer, offset)
                       .streamId(streamId)
                       .sessionId(aeronSessionId);

                putPosition(position, buffer, offset);
                return;
            }
            else if (decoder.streamId() == streamId && decoder.sessionId() == aeronSessionId)
            {
                putPosition(position, buffer, offset);
                return;
            }

            offset += RECORD_LENGTH;
        }

        errorHandler.onError(new IllegalStateException(String.format(
            "Unable to record new session [%d, %d], indexed position buffer full",
            streamId,
            aeronSessionId)));
    }

    private void putPosition(final long position, final AtomicBuffer buffer, final int offset)
    {
        buffer.putLongVolatile(offset + POSITION_OFFSET, position);
    }
}
