/*
 * Copyright 2020 Monotonic Ltd.
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
package uk.co.real_logic.artio.ilink;

import iLinkBinary.NewOrderSingle514Decoder;
import org.agrona.DirectBuffer;
import org.agrona.collections.Int2IntHashMap;
import uk.co.real_logic.artio.fixp.AbstractFixPOffsets;
import uk.co.real_logic.sbe.ir.Ir;
import uk.co.real_logic.sbe.ir.Signal;
import uk.co.real_logic.sbe.ir.Token;

import java.util.List;

import static java.nio.ByteOrder.LITTLE_ENDIAN;

public class ILink3Offsets extends AbstractFixPOffsets
{
    private final Int2IntHashMap templateIdToSeqNumOffset = new Int2IntHashMap(MISSING_OFFSET);
    private final Int2IntHashMap templateIdToUuidOffset = new Int2IntHashMap(MISSING_OFFSET);
    private final Int2IntHashMap templateIdToPossRetransOffset = new Int2IntHashMap(MISSING_OFFSET);
    private final Int2IntHashMap templateIdToSendingTimeEpochOffset = new Int2IntHashMap(MISSING_OFFSET);

    public static final int SEQ_NUM_ID = 9726;
    public static final int UUID_ID = 39001;
    public static final int POSS_RETRANS_ID = 9765;
    public static final int SENDING_TIME_EPOCH_ID = 5297;

    public ILink3Offsets()
    {
        final Ir ir = Ilink3Protocol.loadSbeIr();
        ir.messages().forEach(messageTokens ->
        {
            final int templateId = templateId(messageTokens);
            findOffset(messageTokens, templateId, SEQ_NUM_ID, templateIdToSeqNumOffset);
            findOffset(messageTokens, templateId, UUID_ID, templateIdToUuidOffset);
            findOffset(messageTokens, templateId, POSS_RETRANS_ID, templateIdToPossRetransOffset);
            findOffset(messageTokens, templateId, SENDING_TIME_EPOCH_ID, templateIdToSendingTimeEpochOffset);

            // sanity check static offset assumptions on startup.
            // Lowest template id for an application message (ie a message with a seq num
            if (templateId >= NewOrderSingle514Decoder.TEMPLATE_ID)
            {
                final int seqNumOffset = templateIdToSeqNumOffset.get(templateId);
                if (!(ispartyDetailsOffset(templateId, seqNumOffset) ||
                    seqNumOffset == NORMAL_CLIENT_MSG_SEQ_NUM_OFFSET ||
                    seqNumOffset == EXCHANGE_MSG_SEQ_NUM_OFFSET))
                {
                    throw new IllegalStateException(String.format(
                        "Invalid assumption: template %d has a non-standard sequence number offset of %d",
                        templateId,
                        seqNumOffset));
                }
            }
        });
    }

    private boolean ispartyDetailsOffset(final int templateId, final int seqNumOffset)
    {
        return templateId == PARTY_DETAILS_LIST_REQUEST_ID && seqNumOffset == PARTY_DETAILS_LIST_REQUEST_SEQ_NUM_OFFSET;
    }

    private static void findOffset(
        final List<Token> messageTokens,
        final int templateId,
        final int fieldId,
        final Int2IntHashMap templateIdToFieldOffset)
    {
        messageTokens
            .stream()
            .filter(token -> token.id() == fieldId && token.signal() == Signal.BEGIN_FIELD)
            .findFirst()
            .ifPresent(seqNum ->
            templateIdToFieldOffset.put(templateId, seqNum.offset()));
    }

    public int seqNumOffset(final int templateId)
    {
        return templateIdToSeqNumOffset.get(templateId);
    }

    public int possRetransOffset(final int templateId)
    {
        return templateIdToPossRetransOffset.get(templateId);
    }

    public int sendingTimeEpochOffset(final int templateId)
    {
        return templateIdToSendingTimeEpochOffset.get(templateId);
    }

    public int seqNum(final int templateId, final DirectBuffer buffer, final int messageOffset)
    {
        final int seqNumOffset = seqNumOffset(templateId);
        if (seqNumOffset == MISSING_OFFSET)
        {
            return MISSING_OFFSET;
        }

        return buffer.getInt(messageOffset + seqNumOffset, LITTLE_ENDIAN);
    }

    public long uuid(final int templateId, final DirectBuffer buffer, final int messageOffset)
    {
        final int uuidOffset = templateIdToUuidOffset.get(templateId);
        if (uuidOffset == MISSING_OFFSET)
        {
            return MISSING_OFFSET;
        }

        return buffer.getLong(messageOffset + uuidOffset, LITTLE_ENDIAN);
    }

    public int possRetrans(final int templateId, final DirectBuffer buffer, final int messageOffset)
    {
        final int possRetransOffset = possRetransOffset(templateId);
        if (possRetransOffset == MISSING_OFFSET)
        {
            return MISSING_OFFSET;
        }

        return (short)buffer.getByte(messageOffset + possRetransOffset) & 0xFF;
    }
}
