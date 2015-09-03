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
package uk.co.real_logic.fix_gateway.streams;

import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.aeron.logbuffer.BufferClaim;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.concurrent.AtomicCounter;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.agrona.concurrent.NanoClock;
import uk.co.real_logic.fix_gateway.DebugLogger;
import uk.co.real_logic.fix_gateway.messages.*;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;
import static uk.co.real_logic.aeron.Publication.NOT_CONNECTED;
import static uk.co.real_logic.fix_gateway.CommonConfiguration.*;

/**
 * A proxy for publishing messages fix related messages
 */
public class GatewayPublication
{

    public static final int FRAME_SIZE = FixMessageEncoder.BLOCK_LENGTH + FixMessageDecoder.bodyHeaderLength();

    private static final int CONNECT_SIZE = ConnectEncoder.BLOCK_LENGTH + ConnectDecoder.addressHeaderLength();

    private static final int MAX_CLAIM_ATTEMPTS = BACKOFF_SPINS + BACKOFF_YIELDS + 1000;

    private final MessageHeaderEncoder header = new MessageHeaderEncoder();
    private final LogonEncoder logon = new LogonEncoder();
    private final ConnectEncoder connect = new ConnectEncoder();
    private final InitiateConnectionEncoder initiateConnection = new InitiateConnectionEncoder();
    private final RequestDisconnectEncoder requestDisconnect = new RequestDisconnectEncoder();
    private final DisconnectEncoder disconnect = new DisconnectEncoder();
    private final FixMessageEncoder messageFrame = new FixMessageEncoder();
    private final ErrorEncoder error = new ErrorEncoder();
    private final ApplicationHeartbeatEncoder applicationHeartbeat = new ApplicationHeartbeatEncoder();

    private final BufferClaim bufferClaim;
    private final Publication dataPublication;
    private final IdleStrategy idleStrategy;
    private final NanoClock nanoClock;
    private final AtomicCounter fails;

    public GatewayPublication(
        final Publication dataPublication,
        final AtomicCounter fails,
        final IdleStrategy idleStrategy,
        final NanoClock nanoClock)
    {
        this.dataPublication = dataPublication;
        this.idleStrategy = idleStrategy;
        this.nanoClock = nanoClock;
        bufferClaim = new BufferClaim();
        this.fails = fails;
    }

    public long saveMessage(
        final DirectBuffer srcBuffer,
        final int srcOffset,
        final int srcLength,
        final int libraryId,
        final int messageType,
        final long sessionId,
        final long connectionId,
        final MessageStatus status)
    {
        final long timestamp = TIME_MESSAGES ? nanoClock.nanoTime() : 0L;
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
            .libraryId(libraryId)
            .messageType(messageType)
            .session(sessionId)
            .connection(connectionId)
            .timestamp(timestamp)
            .status(status)
            .putBody(srcBuffer, srcOffset, srcLength);

        bufferClaim.commit();

        DebugLogger.log("Enqueued %s\n", srcBuffer, srcOffset, srcLength);

        return position;
    }

    public long saveLogon(final int libraryId, final long connectionId, final long sessionId)
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
            .libraryId(libraryId)
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
        final byte[] addressString = address.getBytes(UTF_8);

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

    public long saveDisconnect(final int libraryId, final long connectionId)
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
            .libraryId(libraryId)
            .connection(connectionId);

        bufferClaim.commit();

        return position;
    }

    public long saveRequestDisconnect(final int libraryId, final long connectionId)
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
            .libraryId(libraryId)
            .connection(connectionId);

        bufferClaim.commit();

        return position;
    }

    public long saveInitiateConnection(
        final int libraryId,
        final String host,
        final int port,
        final String senderCompId,
        final String senderSubId,
        final String senderLocationId,
        final String targetCompId)
    {
        final byte[] hostBytes = host.getBytes(US_ASCII);
        final byte[] senderCompIdBytes = senderCompId.getBytes(US_ASCII);
        final byte[] senderSubIdBytes = senderSubId.getBytes(US_ASCII);
        final byte[] senderLocationIdBytes = senderLocationId.getBytes(US_ASCII);
        final byte[] targetCompIdBytes = targetCompId.getBytes(US_ASCII);

        final long position = claim(
            header.encodedLength() +
            InitiateConnectionEncoder.BLOCK_LENGTH +
            InitiateConnectionDecoder.hostHeaderLength() * 5 +
            hostBytes.length +
            senderCompIdBytes.length +
            senderSubIdBytes.length +
            senderLocationIdBytes.length +
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
            .libraryId(libraryId)
            .port(port)
            .putHost(hostBytes, 0, hostBytes.length);

        initiateConnection.putSenderCompId(senderCompIdBytes, 0, senderCompIdBytes.length);
        initiateConnection.putSenderSubId(senderSubIdBytes, 0, senderSubIdBytes.length);
        initiateConnection.putSenderLocationId(senderLocationIdBytes, 0, senderLocationIdBytes.length);
        initiateConnection.putTargetCompId(targetCompIdBytes, 0, targetCompIdBytes.length);

        bufferClaim.commit();

        return position;
    }

    public long saveError(final GatewayError errorType, final int libraryId, final String message)
    {
        final byte[] messageBytes = message.getBytes(UTF_8);
        final int length = header.encodedLength() + ErrorEncoder.BLOCK_LENGTH + ErrorDecoder.messageHeaderLength() +
            messageBytes.length;
        final long position = claim(length);

        final MutableDirectBuffer buffer = bufferClaim.buffer();
        int offset = bufferClaim.offset();

        header
            .wrap(buffer, offset)
            .blockLength(error.sbeBlockLength())
            .templateId(error.sbeTemplateId())
            .schemaId(error.sbeSchemaId())
            .version(error.sbeSchemaVersion());

        offset += header.encodedLength();

        error
            .wrap(buffer, offset)
            .type(errorType)
            .libraryId(libraryId)
            .putMessage(messageBytes, 0, messageBytes.length);

        bufferClaim.commit();

        return position;
    }

    public long saveApplicationHeartbeat(final int libraryId)
    {
        final long position = claim(header.encodedLength() + ApplicationHeartbeatEncoder.BLOCK_LENGTH);

        final MutableDirectBuffer buffer = bufferClaim.buffer();
        int offset = bufferClaim.offset();

        header
            .wrap(buffer, offset)
            .blockLength(applicationHeartbeat.sbeBlockLength())
            .templateId(applicationHeartbeat.sbeTemplateId())
            .schemaId(applicationHeartbeat.sbeSchemaId())
            .version(applicationHeartbeat.sbeSchemaVersion());

        offset += header.encodedLength();

        applicationHeartbeat
            .wrap(buffer, offset)
            .libraryId(libraryId);

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
        long position = 0;
        for (int i = 0; i < MAX_CLAIM_ATTEMPTS; i++)
        {
            position = dataPublication.tryClaim(framedLength, bufferClaim);

            if (position > 0L)
            {
                return position;
            }

            idleStrategy.idle(0);
            fails.increment();
        }

        if (position == NOT_CONNECTED)
        {
            // TODO: identify when its sensible to determine disconnect
        }

        throw new IllegalStateException(
            "Unable to send publish message, probably a missing an engine or library instance");

        // return position;
    }
}
