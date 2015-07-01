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
import uk.co.real_logic.aeron.logbuffer.BufferClaim;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.concurrent.AtomicCounter;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.fix_gateway.DebugLogger;
import uk.co.real_logic.fix_gateway.messages.*;

import java.nio.charset.StandardCharsets;

import static java.nio.charset.StandardCharsets.US_ASCII;

/**
 * A proxy for publishing messages fix related messages
 */
public class GatewayPublication
{
    public static final int FRAME_SIZE = FixMessageEncoder.BLOCK_LENGTH + FixMessageDecoder.bodyHeaderLength();
    public static final int CONNECT_SIZE = ConnectEncoder.BLOCK_LENGTH + ConnectDecoder.addressHeaderLength();

    private final MessageHeaderEncoder header = new MessageHeaderEncoder();
    private final LogonEncoder logon = new LogonEncoder();
    private final ConnectEncoder connect = new ConnectEncoder();
    private final InitiateConnectionEncoder initiateConnection = new InitiateConnectionEncoder();
    private final RequestDisconnectEncoder requestDisconnect = new RequestDisconnectEncoder();
    private final DisconnectEncoder disconnect = new DisconnectEncoder();
    private final FixMessageEncoder messageFrame = new FixMessageEncoder();

    private final BufferClaim bufferClaim;
    private final Publication dataPublication;
    private final IdleStrategy idleStrategy;
    private final AtomicCounter fails;

    public GatewayPublication(
        final Publication dataPublication, final AtomicCounter fails, final IdleStrategy idleStrategy)
    {
        this.dataPublication = dataPublication;
        this.idleStrategy = idleStrategy;
        bufferClaim = new BufferClaim();
        this.fails = fails;
    }

    public long saveMessage(
        final DirectBuffer srcBuffer,
        final int srcOffset,
        final int srcLength,
        final int messageType, final long sessionId, final long connectionId)
    {
        final int framedLength = header.encodedLength() + FRAME_SIZE + srcLength;
        final long position = claim(framedLength);

        int offset = bufferClaim.offset();

        final MutableDirectBuffer destBuffer = bufferClaim.buffer();

        header
            .wrap(destBuffer, offset)
            .blockLength(messageFrame.sbeBlockLength())
            .templateId(messageFrame.sbeTemplateId())
            .schemaId(messageFrame.sbeSchemaId())
            .version(messageFrame.sbeSchemaVersion());

        offset += header.encodedLength();

        messageFrame
            .wrap(destBuffer, offset)
            .messageType(messageType)
            .session(sessionId)
            .connection(connectionId)
            .putBody(srcBuffer, srcOffset, srcLength);

        bufferClaim.commit();

        DebugLogger.log("Enqueued %s\n", srcBuffer, srcOffset, srcLength);

        return position;
    }

    public long saveLogon(final long connectionId, final long sessionId)
    {
        final long position = claim(header.encodedLength() + LogonEncoder.BLOCK_LENGTH);

        final MutableDirectBuffer buffer = bufferClaim.buffer();
        int offset = bufferClaim.offset();

        header
            .wrap(buffer, offset)
            .blockLength(logon.sbeBlockLength())
            .templateId(logon.sbeTemplateId())
            .schemaId(logon.sbeSchemaId())
            .version(logon.sbeSchemaVersion());

        offset += header.encodedLength();

        logon
            .wrap(buffer, offset)
            .connection(connectionId)
            .session(sessionId);

        bufferClaim.commit();

        return position;
    }

    public long saveConnect(final long connectionId,
                            final String address,
                            final int libraryId,
                            final ConnectionType type)
    {
        final byte[] addressString = address.getBytes(StandardCharsets.UTF_8);

        final int length = header.encodedLength() + CONNECT_SIZE + addressString.length;
        final long position = claim(length);

        final MutableDirectBuffer buffer = bufferClaim.buffer();
        int offset = bufferClaim.offset();

        header
            .wrap(buffer, offset)
            .blockLength(connect.sbeBlockLength())
            .templateId(connect.sbeTemplateId())
            .schemaId(connect.sbeSchemaId())
            .version(connect.sbeSchemaVersion());

        offset += header.encodedLength();

        connect
            .wrap(buffer, offset)
            .connection(connectionId)
            .libraryId(libraryId)
            .type(type)
            .putAddress(addressString, 0, addressString.length);

        bufferClaim.commit();

        return position;
    }

    public long saveDisconnect(final long connectionId)
    {
        final long position = claim(header.encodedLength() + DisconnectEncoder.BLOCK_LENGTH);

        final MutableDirectBuffer buffer = bufferClaim.buffer();
        int offset = bufferClaim.offset();

        header
            .wrap(buffer, offset)
            .blockLength(disconnect.sbeBlockLength())
            .templateId(disconnect.sbeTemplateId())
            .schemaId(disconnect.sbeSchemaId())
            .version(disconnect.sbeSchemaVersion());

        offset += header.encodedLength();

        disconnect
            .wrap(buffer, offset)
            .connection(connectionId);

        bufferClaim.commit();

        return position;
    }

    public long saveRequestDisconnect(final long connectionId)
    {
        final long position = claim(header.encodedLength() + RequestDisconnectDecoder.BLOCK_LENGTH);

        final MutableDirectBuffer buffer = bufferClaim.buffer();
        int offset = bufferClaim.offset();

        header
            .wrap(buffer, offset)
            .blockLength(requestDisconnect.sbeBlockLength())
            .templateId(requestDisconnect.sbeTemplateId())
            .schemaId(requestDisconnect.sbeSchemaId())
            .version(requestDisconnect.sbeSchemaVersion());

        offset += header.encodedLength();

        requestDisconnect
            .wrap(buffer, offset)
            .connection(connectionId);

        bufferClaim.commit();

        return position;
    }

    public long saveInitiateConnection(
        final String host,
        final int port,
        final String senderCompId,
        final String targetCompId)
    {
        final byte[] hostBytes = host.getBytes(US_ASCII);
        final byte[] senderCompIdBytes = senderCompId.getBytes(US_ASCII);
        final byte[] targetCompIdBytes = targetCompId.getBytes(US_ASCII);

        final long position = claim(
            header.encodedLength() +
            InitiateConnectionEncoder.BLOCK_LENGTH +
            InitiateConnectionDecoder.hostHeaderLength() * 3 +
            hostBytes.length +
            senderCompIdBytes.length +
            targetCompIdBytes.length);

        final MutableDirectBuffer buffer = bufferClaim.buffer();
        int offset = bufferClaim.offset();

        header
            .wrap(buffer, offset)
            .blockLength(initiateConnection.sbeBlockLength())
            .templateId(initiateConnection.sbeTemplateId())
            .schemaId(initiateConnection.sbeSchemaId())
            .version(initiateConnection.sbeSchemaVersion());

        offset += header.encodedLength();

        initiateConnection
            .wrap(buffer, offset)
            .port(port)
            .putHost(hostBytes, 0, hostBytes.length);

        initiateConnection.putSenderCompId(senderCompIdBytes, 0, senderCompIdBytes.length);
        initiateConnection.putTargetCompId(targetCompIdBytes, 0, targetCompIdBytes.length);

        bufferClaim.commit();

        return position;
    }

    public int streamId()
    {
        return dataPublication.streamId();
    }

    public int sessionId()
    {
        return dataPublication.sessionId();
    }

    private long claim(final int framedLength)
    {
        long position;
        while ((position = dataPublication.tryClaim(framedLength, bufferClaim)) < 0L)
        {
            idleStrategy.idle(0);
            fails.increment();
        }

        return position;
    }
}
