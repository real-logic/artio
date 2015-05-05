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
import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.concurrent.AtomicCounter;
import uk.co.real_logic.fix_gateway.DebugLogger;
import uk.co.real_logic.fix_gateway.messages.*;

/**
 * A proxy for publishing messages fix related messages
 */
public class GatewayPublication
{
    public static final int FRAME_SIZE = FixMessageEncoder.BLOCK_LENGTH + FixMessageDecoder.bodyHeaderSize();

    private final MessageHeaderEncoder header = new MessageHeaderEncoder();
    private final ConnectEncoder connect = new ConnectEncoder();
    private final DisconnectEncoder disconnect = new DisconnectEncoder();
    private final FixMessageEncoder messageFrame = new FixMessageEncoder();

    private final BufferClaim bufferClaim;
    private final Publication dataPublication;
    private final AtomicCounter fails;

    public GatewayPublication(final Publication dataPublication, final AtomicCounter fails)
    {
        this.dataPublication = dataPublication;
        bufferClaim = new BufferClaim();
        this.fails = fails;
    }

    public void saveMessage(
        final DirectBuffer srcBuffer,
        final int srcOffset,
        final int srcLength,
        final long sessionId,
        final int messageType)
    {
        final int framedLength = header.size() + FRAME_SIZE + srcLength;
        claim(framedLength);

        int offset = bufferClaim.offset();

        final MutableDirectBuffer destBuffer = bufferClaim.buffer();

        header
            .wrap(destBuffer, offset, 0)
            .blockLength(messageFrame.sbeBlockLength())
            .templateId(messageFrame.sbeTemplateId())
            .schemaId(messageFrame.sbeSchemaId())
            .version(messageFrame.sbeSchemaVersion());

        offset += header.size();

        messageFrame
            .wrap(destBuffer, offset)
            .messageType(messageType)
            .session(sessionId)
            .connection(0L)
            .putBody(srcBuffer, srcOffset, srcLength);

        bufferClaim.commit();

        DebugLogger.log("Enqueued %s\n", destBuffer, offset, framedLength);
    }

    public void saveConnect(final long connectionId, final long sessionId)
    {
        claim(ConnectEncoder.BLOCK_LENGTH);

        final MutableDirectBuffer buffer = bufferClaim.buffer();
        int offset = bufferClaim.offset();

        header
            .wrap(buffer, offset, 0)
            .blockLength(connect.sbeBlockLength())
            .templateId(connect.sbeTemplateId())
            .schemaId(connect.sbeSchemaId())
            .version(connect.sbeSchemaVersion());

        offset += header.size();

        connect
            .wrap(buffer, offset)
            .connection(connectionId)
            .session(sessionId);

        bufferClaim.commit();
    }

    public void saveDisconnect(final long connectionId)
    {
        claim(header.size() + disconnect.size());

        final MutableDirectBuffer buffer = bufferClaim.buffer();
        int offset = bufferClaim.offset();

        header
            .wrap(buffer, offset, 0)
            .blockLength(disconnect.sbeBlockLength())
            .templateId(disconnect.sbeTemplateId())
            .schemaId(disconnect.sbeSchemaId())
            .version(disconnect.sbeSchemaVersion());

        offset += header.size();

        disconnect
            .wrap(buffer, offset)
            .connection(connectionId);

        bufferClaim.commit();
    }

    private void claim(final int framedLength)
    {
        while (dataPublication.tryClaim(framedLength, bufferClaim) < 0L)
        {
            // TODO: backoff
            fails.increment();
        }
    }
}
