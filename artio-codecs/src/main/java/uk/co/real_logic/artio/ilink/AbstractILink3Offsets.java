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

import org.agrona.DirectBuffer;
import org.agrona.ErrorHandler;
import org.agrona.MutableDirectBuffer;

import java.lang.reflect.InvocationTargetException;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static uk.co.real_logic.artio.ilink.AbstractBinaryParser.ILINK_MESSAGE_HEADER_LENGTH;

public abstract class AbstractILink3Offsets
{
    public static final int MISSING_OFFSET = -1;

    static final int NORMAL_CLIENT_MSG_SEQ_NUM_OFFSET = 17;
    static final int PARTY_DETAILS_LIST_REQUEST_SEQ_NUM_OFFSET = 16;
    static final int EXCHANGE_MSG_SEQ_NUM_OFFSET = 0;
    static final int MINIMUM_BUSINESS_MSG_TEMPLATE_ID = 514;
    static final int PARTY_DETAILS_LIST_REQUEST_ID = 537;
    static final int TEMPLATE_ID_OFFSET = 2;

    public static AbstractILink3Offsets make(final ErrorHandler errorHandler)
    {
        try
        {
            final Class<?> cls = Class.forName("uk.co.real_logic.artio.ilink.ILink3Offsets");
            return (AbstractILink3Offsets)cls.getConstructor().newInstance();
        }
        catch (final ClassNotFoundException | NoSuchMethodException | InstantiationException |
            IllegalAccessException | InvocationTargetException e)
        {
            errorHandler.onError(e);
            return null;
        }
    }

    // Optimised path for sequence numbers based upon common patterns.
    public static long clientSeqNum(final DirectBuffer buffer, final int sbeHeaderOffset)
    {
        final int templateId = buffer.getShort(sbeHeaderOffset + TEMPLATE_ID_OFFSET, LITTLE_ENDIAN) & 0xFFFF;
        if (templateId < MINIMUM_BUSINESS_MSG_TEMPLATE_ID)
        {
            return MISSING_OFFSET;
        }

        final int messageOffset = sbeHeaderOffset + ILINK_MESSAGE_HEADER_LENGTH;
        final int fieldOffset = clientSeqNumOffset(templateId);
        return seqNum(buffer, messageOffset + fieldOffset);
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
}
