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
import org.agrona.MutableDirectBuffer;

import java.nio.ByteOrder;

public final class SimpleOpenFramingHeader
{
    public static final int SOFH_MSG_SIZE_LENGTH = 2;
    public static final int SOFH_ENCODING_LENGTH = 2;
    public static final int SOFH_LENGTH = SOFH_MSG_SIZE_LENGTH + SOFH_ENCODING_LENGTH;

    public static final int SOFH_MSG_SIZE_OFFSET = 0;
    public static final int SOFH_ENCODING_OFFSET = SOFH_MSG_SIZE_LENGTH;

    public static final short CME_ENCODING_TYPE = (short)0xCAFE;

    public static void writeSofh(final MutableDirectBuffer buffer, final int offset, final int messageSize)
    {
        buffer.putShort(offset + SOFH_MSG_SIZE_OFFSET, (short)messageSize, ByteOrder.LITTLE_ENDIAN);
        buffer.putShort(offset + SOFH_ENCODING_OFFSET, CME_ENCODING_TYPE, ByteOrder.LITTLE_ENDIAN);
    }

    public static int readSofh(final DirectBuffer buffer, final int offset)
    {
        final int messageSize = readSofhMessageSize(buffer, offset);
        final short encodingType = buffer.getShort(offset + SOFH_ENCODING_OFFSET, ByteOrder.LITTLE_ENDIAN);
        if (encodingType != CME_ENCODING_TYPE)
        {
            throw new IllegalArgumentException(
                "Unsupported Encoding Type: " + encodingType + " should be " + CME_ENCODING_TYPE);
        }
        return messageSize;
    }

    public static int readSofhMessageSize(final DirectBuffer buffer, final int offset)
    {
        return buffer.getShort(offset + SOFH_MSG_SIZE_OFFSET, ByteOrder.LITTLE_ENDIAN) & 0xFFFF;
    }
}
