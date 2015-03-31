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

import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.common.concurrent.logbuffer.BufferClaim;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.concurrent.AtomicCounter;
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer;
import uk.co.real_logic.fix_gateway.DebugLogger;
import uk.co.real_logic.fix_gateway.MessageHandler;
import uk.co.real_logic.fix_gateway.messages.FixMessage;
import uk.co.real_logic.sbe.codec.java.CodecUtil;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static uk.co.real_logic.fix_gateway.messages.FixMessage.BLOCK_LENGTH;

/**
 * A proxy for publishing messages fix related messages
 *
 */
public class GatewayPublication implements MessageHandler
{
    public static final int FRAME_SIZE = BLOCK_LENGTH + FixMessage.bodyHeaderSize();
    // SBE Message offset: offset + fixMessage.sbeBlockLength() + fixMessage.bodyHeaderSize();

    // TODO: get SBE to generate this code:
    public static void putBodyLength(final UnsafeBuffer unsafeBuffer, final int offset, final int bodyLength)
    {
        CodecUtil.uint16Put(unsafeBuffer, offset + BLOCK_LENGTH, bodyLength, LITTLE_ENDIAN);
    }

    private final FixMessage messageFrame = new FixMessage();

    private final BufferClaim bufferClaim;
    private final Publication dataPublication;
    private final AtomicCounter fails;

    public GatewayPublication(final Publication dataPublication, final AtomicCounter fails)
    {
        this.dataPublication = dataPublication;
        bufferClaim = new BufferClaim();
        this.fails = fails;
    }

    public void onMessage(
        final DirectBuffer buffer, final int offset, final int length, final long sessionId, final int messageType)
    {
        saveFixMessage(buffer, offset, length, sessionId, messageType);
    }

    private void saveFixMessage(
        final DirectBuffer srcBuffer,
        final int srcOffset,
        final int srcLength,
        final long sessionId,
        final int messageType)
    {
        final int framedLength = srcLength + FRAME_SIZE;

        while (!dataPublication.tryClaim(framedLength, bufferClaim))
        {
            // TODO: backoff
            fails.increment();
        }

        final int destOffset = bufferClaim.offset();
        final int desMessageOffset = destOffset + FRAME_SIZE;

        final UnsafeBuffer unsafeBuffer = (UnsafeBuffer) bufferClaim.buffer();
        messageFrame
            .wrapForEncode(unsafeBuffer, destOffset)
            .messageType(messageType)
            .session(sessionId)
            .connection(0L); // TODO
        putBodyLength(unsafeBuffer, destOffset, srcLength);

        unsafeBuffer.putBytes(desMessageOffset, srcBuffer, srcOffset, srcLength);

        bufferClaim.commit();

        DebugLogger.log("Enqueued %s\n", unsafeBuffer, desMessageOffset, srcLength);
    }

    public void saveConnect()
    {
    }

    public void saveDisconnect()
    {
    }
}
