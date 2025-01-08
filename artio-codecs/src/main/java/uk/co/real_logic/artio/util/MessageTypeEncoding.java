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
package uk.co.real_logic.artio.util;

import org.agrona.collections.LongHashSet;

import java.util.Set;

import static org.agrona.BufferUtil.ARRAY_BASE_OFFSET;
import static org.agrona.UnsafeAccess.UNSAFE;

/**
 * Class for handling the encoding and decoding of Artio's packed message types.
 *
 * FIX uses 1 or 2 character ascii sequences as a way of representing the message type of messages. Some venues
 * have a longer representation with more message types in. Artio has a packed representation where the bytes of
 * the message type are encoded into a long.
 */
public final class MessageTypeEncoding
{
    public static final int MAX_MESSAGE_TYPE_LENGTH = 8;

    private static final int MESSAGE_TYPE_BITSHIFT = 8;

    /**
     * Creates a packed message type from a string.
     *
     * @param messageType message type as ascii string.
     * @return the packed message type.
     * @throws IllegalArgumentException if messageType parameter is too long.
     */
    public static long packMessageType(final String messageType)
    {
        final int length = messageType.length();
        checkLength(length);

        long packed = 0;
        for (int index = 0; index < length; index++)
        {
            final int asciiValue = (byte)messageType.charAt(index);
            packed |= asciiValue << (MESSAGE_TYPE_BITSHIFT * index);
        }

        return packed;
    }

    public static LongHashSet packAllMessageTypes(final Set<String> messageTypes)
    {
        final LongHashSet packedMessageTypes = new LongHashSet();
        messageTypes.forEach(messageTypeAsString ->
            packedMessageTypes.add(packMessageType(messageTypeAsString)));
        return packedMessageTypes;
    }

    /**
     * Creates a packed message type from a char[] and length.
     *
     * @param messageType message type as ascii char[].
     * @param length the number of characters within messageType to use.
     * @return the packed message type.
     * @throws IllegalArgumentException if messageType parameter is too long.
     */
    public static long packMessageType(final char[] messageType, final int length)
    {
        checkLength(length);

        long packed = 0;
        for (int index = 0; index < length; index++)
        {
            final int asciiValue = (byte)messageType[index];
            packed |= asciiValue << (MESSAGE_TYPE_BITSHIFT * index);
        }

        return packed;
    }

    private static void checkLength(final int length)
    {
        if (length > MAX_MESSAGE_TYPE_LENGTH)
        {
            throw new IllegalArgumentException("Message types longer than 8 are not supported yet");
        }
    }

    /**
     * Creates a packed message type from a byte[] and length.
     *
     * @param messageType message type as ascii byte[].
     * @param offset the offset within the messagetype to start looking
     * @param length the number of characters within messageType to use.
     * @return the packed message type.
     * @throws IllegalArgumentException if messageType parameter is too long.
     */
    public static long packMessageType(final byte[] messageType, final int offset, final int length)
    {
        return packMessageType(messageType, ARRAY_BASE_OFFSET, offset, length);
    }

    static long packMessageType(final byte[] messageType, final long baseOffset, final int offset, final int length)
    {
        checkLength(length);

        if (length == 1)
        {
            return UNSAFE.getByte(messageType, baseOffset + offset);
        }
        else if (length == 2)
        {
            return UNSAFE.getShort(messageType, baseOffset + offset);
        }
        else
        {
            long packed = 0;
            for (int index = 0; index < length; index++)
            {
                final int asciiValue = UNSAFE.getByte(messageType, baseOffset + offset + index);
                packed |= asciiValue << (MESSAGE_TYPE_BITSHIFT * index);
            }
            return packed;
        }
    }

    /**
     * Unpacks a packed message type into a byte array so that it can be encoded into different formats.
     * Note: this only supports up to 2 character message types, which is enough for most normal FIX dictionaries
     * but not the "ultra-packed" message type that the <code>packMessageType()</code> methods can support for
     * unusual dictionaries. The unpacked value is a byte[] that contains the ascii encoded String of the message
     * type.
     *
     * @param packedMessageType the FIX message type in packed format.
     * @param dest a destination byte array where the unpacked value is put, should be at least two bytes long.
     * @throws ArrayIndexOutOfBoundsException if dest is too short.
     *
     * @return the length of the unpacked value
     */
    public static int unpackMessageType(final long packedMessageType, final byte[] dest)
    {
        dest[0] = (byte)packedMessageType;
        final byte secondByte = (byte)(packedMessageType >>> 8);
        if (secondByte == 0)
        {
            return 1;
        }
        else
        {
            dest[1] = secondByte;
            return 2;
        }
    }
}
