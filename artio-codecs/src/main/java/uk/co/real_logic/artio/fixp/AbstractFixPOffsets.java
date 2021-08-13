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
package uk.co.real_logic.artio.fixp;

import org.agrona.DirectBuffer;
import org.agrona.LangUtil;
import org.agrona.MutableDirectBuffer;
import uk.co.real_logic.sbe.ir.Ir;
import uk.co.real_logic.sbe.ir.IrDecoder;
import uk.co.real_logic.sbe.ir.Token;

import java.io.InputStream;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.List;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static uk.co.real_logic.artio.fixp.AbstractFixPParser.FIXP_MESSAGE_HEADER_LENGTH;

public abstract class AbstractFixPOffsets
{
    public static final int MISSING_OFFSET = -1;

    public static final int NORMAL_CLIENT_MSG_SEQ_NUM_OFFSET = 17;
    public static final int PARTY_DETAILS_LIST_REQUEST_SEQ_NUM_OFFSET = 16;
    public static final int EXCHANGE_MSG_SEQ_NUM_OFFSET = 0;
    public static final int MINIMUM_BUSINESS_MSG_TEMPLATE_ID = 514;
    public static final int PARTY_DETAILS_LIST_REQUEST_ID = 537;
    public static final int TEMPLATE_ID_OFFSET = 2;

    // Optimised path for sequence numbers based upon common patterns.
    public static long clientSeqNum(final DirectBuffer buffer, final int sbeHeaderOffset)
    {
        final int templateId = templateId(buffer, sbeHeaderOffset, TEMPLATE_ID_OFFSET);
        if (templateId < MINIMUM_BUSINESS_MSG_TEMPLATE_ID)
        {
            return MISSING_OFFSET;
        }

        final int messageOffset = sbeHeaderOffset + FIXP_MESSAGE_HEADER_LENGTH;
        final int fieldOffset = clientSeqNumOffset(templateId);
        return seqNum(buffer, messageOffset + fieldOffset);
    }

    public static int templateId(final DirectBuffer buffer, final int sbeHeaderOffset, final int templateIdOffset)
    {
        return buffer.getShort(sbeHeaderOffset + templateIdOffset, LITTLE_ENDIAN) & 0xFFFF;
    }

    public static void clientSeqNum(
        final int templateId, final MutableDirectBuffer buffer, final int messageOffset, final long seqNum)
    {
        final int fieldOffset = clientSeqNumOffset(templateId);
        seqNum(buffer, messageOffset + fieldOffset, seqNum);
    }

    private static int clientSeqNumOffset(final int templateId)
    {
        return templateId == PARTY_DETAILS_LIST_REQUEST_ID ?
            PARTY_DETAILS_LIST_REQUEST_SEQ_NUM_OFFSET : NORMAL_CLIENT_MSG_SEQ_NUM_OFFSET;
    }

    public static long exchangeSeqNum(final DirectBuffer buffer, final int messageOffset)
    {
        return seqNum(buffer, messageOffset + EXCHANGE_MSG_SEQ_NUM_OFFSET);
    }

    private static long seqNum(final DirectBuffer buffer, final int index)
    {
        return buffer.getInt(index, LITTLE_ENDIAN) & 0xFFFF_FFFFL;
    }

    private static void seqNum(final MutableDirectBuffer buffer, final int index, final long seqNum)
    {
        buffer.putInt(index, (int)seqNum, LITTLE_ENDIAN);
    }

    public abstract int seqNumOffset(int templateId);

    public abstract int seqNum(int templateId, DirectBuffer buffer, int messageOffset);

    public abstract int possRetransOffset(int templateId);

    public abstract int possRetrans(int templateId, DirectBuffer buffer, int messageOffset);

    public abstract int sendingTimeEpochOffset(int templateId);

    public static Ir loadSbeIr(final Class<?> encoder, final String fileName)
    {
        try
        {
            final InputStream stream = encoder.getResourceAsStream(fileName);
            if (stream == null)
            {
                final URL resource = encoder.getResource(".");
                final String encoderDir = resource == null ? "unknown" : resource.toURI().getPath();
                throw new IllegalStateException("Unable to find SBE IR: " + fileName +
                    " associated with resource: " + encoder + " @ " + encoderDir);
            }
            final int length = stream.available();
            final byte[] bytes = new byte[length];
            int remaining = length;
            while (remaining > 0)
            {
                remaining -= stream.read(bytes, length - remaining, remaining);
            }
            try (IrDecoder irDecoder = new IrDecoder(ByteBuffer.wrap(bytes)))
            {
                return irDecoder.decode();
            }
        }
        catch (final Exception e)
        {
            LangUtil.rethrowUnchecked(e);
            return null;
        }
    }

    public static int templateId(final List<Token> messageTokens)
    {
        final Token beginMessage = messageTokens.get(0);
        return beginMessage.id();
    }
}
