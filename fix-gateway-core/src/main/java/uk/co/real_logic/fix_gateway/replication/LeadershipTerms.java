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
package uk.co.real_logic.fix_gateway.replication;

import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.fix_gateway.messages.LeadershipTermDecoder;
import uk.co.real_logic.fix_gateway.messages.LeadershipTermEncoder;
import uk.co.real_logic.fix_gateway.messages.MessageHeaderDecoder;
import uk.co.real_logic.fix_gateway.messages.MessageHeaderEncoder;

import static java.util.Objects.requireNonNull;
import static uk.co.real_logic.agrona.BitUtil.SIZE_OF_INT;

/**
 * A sequence of session id and position intervals that correspond to leadership terms
 */
// TODO: move this to persistent off-heap storage
// TODO: decide on storage structure, perhaps a tree of intervals?
public class LeadershipTerms
{
    private static final int CURRENT_ROW_OFFSET = MessageHeaderEncoder.ENCODED_LENGTH;
    private static final int HEADER_SIZE = CURRENT_ROW_OFFSET + SIZE_OF_INT;
    private static final int ROW_SIZE = LeadershipTermEncoder.BLOCK_LENGTH;

    private final MutableDirectBuffer buffer;
    private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final LeadershipTermEncoder encoder = new LeadershipTermEncoder();
    private final LeadershipTermDecoder decoder = new LeadershipTermDecoder();

    private final int actingBlockLength;
    private final int actingVersion;

    public LeadershipTerms(final MutableDirectBuffer buffer)
    {
        this.buffer = buffer;
        setupHeader();
        actingBlockLength = messageHeaderDecoder.blockLength();
        actingVersion = messageHeaderDecoder.version();
    }

    private void setupHeader()
    {
        messageHeaderEncoder
            .wrap(buffer, 0)
            .blockLength(encoder.sbeBlockLength())
            .templateId(encoder.sbeTemplateId())
            .schemaId(encoder.sbeSchemaId())
            .version(encoder.sbeSchemaVersion());

        messageHeaderDecoder.wrap(buffer, 0);

        currentRow(HEADER_SIZE);
    }

    private void currentRow(final int value)
    {
        buffer.putInt(CURRENT_ROW_OFFSET, value);
    }

    private int currentRow()
    {
        return buffer.getInt(CURRENT_ROW_OFFSET);
    }

    public void onNewLeader(
        final long finalStreamPositionOfPreviousLeader,
        final long initialPosition,
        final long initialStreamPosition,
        final int sessionId)
    {
        int currentRow = currentRow();

        if (buffer.getInt(currentRow) != 0)
        {
            encoder
                .wrap(buffer, currentRow)
                .finalStreamPosition(finalStreamPositionOfPreviousLeader);
        }

        currentRow += ROW_SIZE;

        encoder
            .wrap(buffer, currentRow)
            .initialPosition(initialPosition)
            .initialStreamPosition(initialStreamPosition)
            .sessionId(sessionId);

        currentRow(currentRow);
    }

    public boolean find(final long position, final Cursor cursor)
    {
        requireNonNull(cursor, "Cursor cannot be null");

        decoder.wrap(buffer, currentRow(), actingBlockLength, actingVersion);

        if (decoder.sessionId() == 0)
        {
            return false;
        }

        final long termOffset = position - decoder.initialPosition();

        cursor.sessionId = decoder.sessionId();
        cursor.streamPosition = decoder.initialStreamPosition() + termOffset;

        return true;
    }

    public static class Cursor
    {
        private long streamPosition;
        private int sessionId;

        public long streamPosition()
        {
            return streamPosition;
        }

        public int sessionId()
        {
            return sessionId;
        }
    }

}
