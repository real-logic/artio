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
package uk.co.real_logic.fix_gateway.framer;

import uk.co.real_logic.agrona.concurrent.AtomicBuffer;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.util.StringFlyweight;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import static uk.co.real_logic.fix_gateway.util.StringFlyweight.UNKNOWN_INDEX;

/**
 * Handles incoming data from sockets
 */
public class ReceiverEndPoint
{
    private static final byte START_OF_HEADER = 0x01;

    private static final byte BODY_LENGTH_FIELD = 9;
    private static final byte CHECKSUM_FIELD = 10;

    private static final int COMMON_PREFIX_LENGTH = "8=FIX.4.2 ".length();
    private static final int START_OF_BODY_LENGTH = COMMON_PREFIX_LENGTH + 2;

    private static final int MIN_CHECKSUM_SIZE = " 10=".length() + 1;

    private final AtomicBuffer buffer;
    private final SocketChannel channel;
    private final MessageHandler handler;
    private final StringFlyweight string;

    private int usedBufferData = 0;

    public ReceiverEndPoint(final SocketChannel channel, final int bufferSize, final MessageHandler handler)
    {
        this.channel = channel;
        this.handler = handler;
        buffer = new UnsafeBuffer(ByteBuffer.allocateDirect(bufferSize));
        string = new StringFlyweight(buffer);
    }

    public void receiveData()
    {
        try
        {
            readData();
            frameMessages();
        }
        catch (IOException e)
        {
            // TODO
            e.printStackTrace();
        }
    }

    private void readData() throws IOException
    {
        usedBufferData += channel.read(buffer.byteBuffer());
    }

    private void frameMessages()
    {
        int offset = 0;
        while (true)
        {
            final int startOfBodyLength = offset + START_OF_BODY_LENGTH;
            if (usedBufferData < startOfBodyLength)
            {
                // Need more data
                break;
            }

            try
            {
                if (validateBodyLengthTag(offset))
                {
                    invalidateMessage();
                    return;
                }

                final int indexOfLastByteOfMessage = findIndexOfLastByteOfMessage(offset, startOfBodyLength);
                if (indexOfLastByteOfMessage == UNKNOWN_INDEX)
                {
                    // Need more data
                    break;
                }

                final int length = (indexOfLastByteOfMessage + 1) - offset;
                handler.onMessage(buffer, offset, length);

                offset += length;
            }
            catch (Exception e)
            {
                // TODO: remove exceptions from the common path
                e.printStackTrace();
                break;
            }
        }

        moveRemainingDataToBufferStart(offset);
    }

    private int findIndexOfLastByteOfMessage(int offset, int startOfBodyLength)
    {
        final int endOfBodyLength = string.scan(startOfBodyLength + 1, usedBufferData, START_OF_HEADER);
        final int earliestChecksumEnd = endOfBodyLength + getBodyLength(offset, endOfBodyLength) + MIN_CHECKSUM_SIZE;
        return string.scan(earliestChecksumEnd, usedBufferData, START_OF_HEADER);
    }

    private int getBodyLength(final int offset, final int endOfBodyLength)
    {
        return string.getInt(offset + START_OF_BODY_LENGTH, endOfBodyLength);
    }

    private boolean validateBodyLengthTag(final int offset)
    {
        return string.getDigit(offset + COMMON_PREFIX_LENGTH) != BODY_LENGTH_FIELD
            || string.getChar(offset + COMMON_PREFIX_LENGTH + 1) != '=';
    }

    private void moveRemainingDataToBufferStart(final int offset)
    {
        usedBufferData -= offset;
        buffer.putBytes(0, buffer, offset, usedBufferData);
        buffer.byteBuffer().position(usedBufferData);
    }

    private void invalidateMessage()
    {
        // TODO
        System.err.println("Invalid message");
    }

}
