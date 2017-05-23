/*
 * Copyright 2015-2017 Real Logic Ltd.
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

import org.agrona.MutableDirectBuffer;
import uk.co.real_logic.fix_gateway.replication.messages.LeadershipTermDecoder;
import uk.co.real_logic.fix_gateway.replication.messages.LeadershipTermEncoder;
import uk.co.real_logic.fix_gateway.replication.messages.MessageHeaderDecoder;
import uk.co.real_logic.fix_gateway.replication.messages.MessageHeaderEncoder;

import java.nio.ByteOrder;

import static java.util.Objects.requireNonNull;
import static org.agrona.BitUtil.SIZE_OF_INT;

/**
 * A sequence of session id and position intervals that correspond to leadership terms.
 *
 * This acts as an off-heap archive that does a binary search over the underlying
 * intervals.
 */
class LeadershipTermIndex
{
    private static final int NO_HEADER_WRITTEN = 0;

    private static final int CURRENT_ROW_OFFSET = MessageHeaderEncoder.ENCODED_LENGTH;
    private static final int HEADER_SIZE = CURRENT_ROW_OFFSET + SIZE_OF_INT;
    private static final int ROW_SIZE = LeadershipTermEncoder.BLOCK_LENGTH;
    private static final int INITIAL_POSITION_OFFSET = SIZE_OF_INT;

    private final MutableDirectBuffer buffer;
    private final MessageHeaderEncoder messageHeaderEncoder = new MessageHeaderEncoder();
    private final MessageHeaderDecoder messageHeaderDecoder = new MessageHeaderDecoder();
    private final LeadershipTermEncoder encoder = new LeadershipTermEncoder();
    private final LeadershipTermDecoder decoder = new LeadershipTermDecoder();

    private final int actingBlockLength;
    private final int actingVersion;

    LeadershipTermIndex(final MutableDirectBuffer buffer)
    {
        this.buffer = buffer;
        setupHeader();
        actingBlockLength = messageHeaderDecoder.blockLength();
        actingVersion = messageHeaderDecoder.version();
    }

    private void setupHeader()
    {
        messageHeaderDecoder.wrap(buffer, 0);

        if (messageHeaderDecoder.schemaId() == NO_HEADER_WRITTEN)
        {
            writeNewHeader();
        }
    }

    private void writeNewHeader()
    {
        messageHeaderEncoder
            .wrap(buffer, 0)
            .blockLength(encoder.sbeBlockLength())
            .templateId(encoder.sbeTemplateId())
            .schemaId(encoder.sbeSchemaId())
            .version(encoder.sbeSchemaVersion());

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

    void onNewLeader(
        final long finalStreamPositionOfPreviousLeader,
        final long initialPosition,
        final long initialStreamPosition,
        final int sessionId)
    {
        final int currentRow = currentRow();

        if (buffer.getInt(currentRow) != 0)
        {
            encoder
                .wrap(buffer, currentRow)
                .finalTransportPosition(finalStreamPositionOfPreviousLeader);
        }

        encoder
            .wrap(buffer, currentRow)
            .initialPosition(initialPosition)
            .initialTransportPosition(initialStreamPosition)
            .sessionId(sessionId);

        currentRow(currentRow + ROW_SIZE);
    }

    boolean find(final long position, final Cursor cursor)
    {
        requireNonNull(cursor, "Cursor cannot be null");

        final MutableDirectBuffer buffer = this.buffer;

        final int currentRow = currentRow();
        if (currentRow == HEADER_SIZE)
        {
            return false;
        }

        int minIndex = 0;
        int maxIndex = (currentRow - HEADER_SIZE) / ROW_SIZE - 1;
        while (minIndex <= maxIndex)
        {
            final int midIndex = (minIndex + maxIndex) >>> 1;
            final int midOffset = offset(midIndex);
            final long startOfMid = initialPosition(buffer, midOffset);
            final long endOfMid = readEndOfMid(buffer, midOffset);

            if (position < startOfMid)
            {
                maxIndex = midIndex - 1;
            }
            else if (position < endOfMid)
            {
                final long termOffset = position - startOfMid;

                final LeadershipTermDecoder decoder =
                    this.decoder.wrap(buffer, midOffset, actingBlockLength, actingVersion);

                cursor.sessionId = decoder.sessionId();
                cursor.transportPosition = decoder.initialTransportPosition() + termOffset;

                return true;
            }
            else
            {
                minIndex = midIndex + 1;
            }
        }

        return false;
    }

    private long readEndOfMid(final MutableDirectBuffer buffer, final int midOffset)
    {
        long endOfMid = initialPosition(buffer, midOffset + ROW_SIZE);
        if (endOfMid == 0)
        {
            endOfMid = Long.MAX_VALUE;
        }
        return endOfMid;
    }

    private long initialPosition(final MutableDirectBuffer buffer, final int offset)
    {
        return buffer.getLong(offset + INITIAL_POSITION_OFFSET, ByteOrder.LITTLE_ENDIAN);
    }

    private int offset(final int index)
    {
        return HEADER_SIZE + index * ROW_SIZE;
    }

    static class Cursor
    {
        private long transportPosition;
        private int sessionId;

        long transportPosition()
        {
            return transportPosition;
        }

        int sessionId()
        {
            return sessionId;
        }
    }
}
