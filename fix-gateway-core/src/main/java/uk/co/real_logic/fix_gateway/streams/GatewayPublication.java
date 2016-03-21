/*
 * Copyright 2015-2016 Real Logic Ltd.
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
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.concurrent.AtomicCounter;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.agrona.concurrent.NanoClock;
import uk.co.real_logic.fix_gateway.DebugLogger;
import uk.co.real_logic.fix_gateway.ReliefValve;
import uk.co.real_logic.fix_gateway.messages.*;

import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.nio.charset.StandardCharsets.UTF_8;
import static uk.co.real_logic.fix_gateway.CommonConfiguration.TIME_MESSAGES;
import static uk.co.real_logic.fix_gateway.engine.logger.SequenceNumberIndexReader.UNKNOWN_SESSION;
import static uk.co.real_logic.fix_gateway.messages.ConnectionType.ACCEPTOR;
import static uk.co.real_logic.fix_gateway.messages.ConnectionType.INITIATOR;

/**
 * A proxy for publishing messages fix related messages
 */
public class GatewayPublication extends AbstractPublication
{

    public static final int FRAME_SIZE = FixMessageEncoder.BLOCK_LENGTH + FixMessageDecoder.bodyHeaderLength();
    public static final int CONNECT_SIZE = ConnectEncoder.BLOCK_LENGTH + ConnectDecoder.addressHeaderLength();
    public static final int HEARTBEAT_LENGTH = HEADER_LENGTH + ApplicationHeartbeatEncoder.BLOCK_LENGTH;
    public static final int LIBRARY_CONNECT_LENGTH = HEADER_LENGTH + LibraryConnectEncoder.BLOCK_LENGTH;
    public static final int DISCONNECT_LENGTH = HEADER_LENGTH + DisconnectEncoder.BLOCK_LENGTH;
    public static final int RELEASE_SESSION_LENGTH = HEADER_LENGTH + ReleaseSessionEncoder.BLOCK_LENGTH;
    public static final int RELEASE_SESSION_REPLY_LENGTH = HEADER_LENGTH + ReleaseSessionReplyDecoder.BLOCK_LENGTH;
    public static final int REQUEST_SESSION_LENGTH = HEADER_LENGTH + RequestSessionEncoder.BLOCK_LENGTH;
    public static final int REQUEST_SESSION_REPLY_LENGTH = HEADER_LENGTH + RequestSessionReplyEncoder.BLOCK_LENGTH;

    private final LogonEncoder logon = new LogonEncoder();
    private final ConnectEncoder connect = new ConnectEncoder();
    private final InitiateConnectionEncoder initiateConnection = new InitiateConnectionEncoder();
    private final RequestDisconnectEncoder requestDisconnect = new RequestDisconnectEncoder();
    private final DisconnectEncoder disconnect = new DisconnectEncoder();
    private final FixMessageEncoder messageFrame = new FixMessageEncoder();
    private final ErrorEncoder error = new ErrorEncoder();
    private final ApplicationHeartbeatEncoder applicationHeartbeat = new ApplicationHeartbeatEncoder();
    private final LibraryConnectEncoder libraryConnect = new LibraryConnectEncoder();
    private final RequestSessionEncoder requestSession = new RequestSessionEncoder();
    private final RequestSessionReplyEncoder requestSessionReply = new RequestSessionReplyEncoder();
    private final ReleaseSessionEncoder releaseSession = new ReleaseSessionEncoder();
    private final ReleaseSessionReplyEncoder releaseSessionReply = new ReleaseSessionReplyEncoder();

    private final NanoClock nanoClock;

    public GatewayPublication(
        final Publication dataPublication,
        final AtomicCounter fails,
        final IdleStrategy idleStrategy,
        final NanoClock nanoClock,
        final int maxClaimAttempts,
        final ReliefValve reliefValve)
    {
        super(maxClaimAttempts, idleStrategy, fails, reliefValve, dataPublication);
        this.nanoClock = nanoClock;
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

    public long saveLogon(final int libraryId,
                          final long connectionId,
                          final long sessionId)
    {
        return saveLogon(libraryId, connectionId, sessionId, UNKNOWN_SESSION, UNKNOWN_SESSION);
    }

    public long saveLogon(final int libraryId,
                          final long connectionId,
                          final long sessionId,
                          final int lastSentSequenceNumber,
                          final int lastReceivedSequenceNumber)
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
            .session(sessionId)
            .lastSentSequenceNumber(lastSentSequenceNumber)
            .lastReceivedSequenceNumber(lastReceivedSequenceNumber);

        bufferClaim.commit();

        return position;
    }

    public long saveConnect(final long connectionId,
                            final String address,
                            final int libraryId,
                            final ConnectionType type,
                            final int lastSentSequenceNumber,
                            final int lastReceivedSequenceNumber)
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
            .lastSentSequenceNumber(lastSentSequenceNumber)
            .lastReceivedSequenceNumber(lastReceivedSequenceNumber)
            .putAddress(addressString, 0, addressString.length);

        bufferClaim.commit();

        return position;
    }

    public long saveDisconnect(final int libraryId, final long connectionId, final DisconnectReason reason)
    {
        final long position = claim(DISCONNECT_LENGTH);

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
            .connection(connectionId)
            .reason(reason);

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
        final long position = claim(HEARTBEAT_LENGTH);

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

    public long saveLibraryConnect(final int libraryId, final boolean isAcceptor)
    {
        final long position = claim(LIBRARY_CONNECT_LENGTH);

        final MutableDirectBuffer buffer = bufferClaim.buffer();
        int offset = bufferClaim.offset();

        header
            .wrap(buffer, offset)
            .blockLength(libraryConnect.sbeBlockLength())
            .templateId(libraryConnect.sbeTemplateId())
            .schemaId(libraryConnect.sbeSchemaId())
            .version(libraryConnect.sbeSchemaVersion());

        offset += header.encodedLength();

        libraryConnect
            .wrap(buffer, offset)
            .libraryId(libraryId)
            .typeHandled(isAcceptor ? ACCEPTOR : INITIATOR);

        bufferClaim.commit();

        return position;
    }

    public long saveReleaseSession(final int libraryId, final long connectionId, final long correlationId)
    {
        final long position = claim(RELEASE_SESSION_REPLY_LENGTH);

        final MutableDirectBuffer buffer = bufferClaim.buffer();
        int offset = bufferClaim.offset();

        header
            .wrap(buffer, offset)
            .blockLength(releaseSession.sbeBlockLength())
            .templateId(releaseSession.sbeTemplateId())
            .schemaId(releaseSession.sbeSchemaId())
            .version(releaseSession.sbeSchemaVersion());

        offset += header.encodedLength();

        releaseSession
            .wrap(buffer, offset)
            .libraryId(libraryId)
            .connection(connectionId)
            .correlationId(correlationId);

        bufferClaim.commit();

        return position;
    }

    public long saveReleaseSessionReply(final SessionReplyStatus status, final long correlationId)
    {
        final long position = claim(RELEASE_SESSION_LENGTH);

        final MutableDirectBuffer buffer = bufferClaim.buffer();
        int offset = bufferClaim.offset();

        header
            .wrap(buffer, offset)
            .blockLength(releaseSessionReply.sbeBlockLength())
            .templateId(releaseSessionReply.sbeTemplateId())
            .schemaId(releaseSessionReply.sbeSchemaId())
            .version(releaseSessionReply.sbeSchemaVersion());

        offset += header.encodedLength();

        releaseSessionReply
            .wrap(buffer, offset)
            .correlationId(correlationId)
            .status(status);

        bufferClaim.commit();

        return position;
    }

    public long saveRequestSession(final int libraryId, final long connectionId, final long correlationId)
    {
        final long position = claim(REQUEST_SESSION_LENGTH);

        final MutableDirectBuffer buffer = bufferClaim.buffer();
        int offset = bufferClaim.offset();

        header
            .wrap(buffer, offset)
            .blockLength(requestSession.sbeBlockLength())
            .templateId(requestSession.sbeTemplateId())
            .schemaId(requestSession.sbeSchemaId())
            .version(requestSession.sbeSchemaVersion());

        offset += header.encodedLength();

        requestSession
            .wrap(buffer, offset)
            .libraryId(libraryId)
            .connection(connectionId)
            .correlationId(correlationId);

        bufferClaim.commit();

        return position;
    }

    public long saveRequestSessionReply(final SessionReplyStatus status, final int correlationId)
    {
        final long position = claim(REQUEST_SESSION_REPLY_LENGTH);

        final MutableDirectBuffer buffer = bufferClaim.buffer();
        int offset = bufferClaim.offset();

        header
            .wrap(buffer, offset)
            .blockLength(requestSessionReply.sbeBlockLength())
            .templateId(requestSessionReply.sbeTemplateId())
            .schemaId(requestSessionReply.sbeSchemaId())
            .version(requestSessionReply.sbeSchemaVersion());

        offset += header.encodedLength();

        requestSessionReply
            .wrap(buffer, offset)
            .correlationId(correlationId)
            .status(status);

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

}
