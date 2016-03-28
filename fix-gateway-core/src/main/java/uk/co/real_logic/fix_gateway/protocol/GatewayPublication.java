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
package uk.co.real_logic.fix_gateway.protocol;

import uk.co.real_logic.aeron.Publication;
import uk.co.real_logic.agrona.DirectBuffer;
import uk.co.real_logic.agrona.MutableDirectBuffer;
import uk.co.real_logic.agrona.concurrent.AtomicCounter;
import uk.co.real_logic.agrona.concurrent.IdleStrategy;
import uk.co.real_logic.agrona.concurrent.NanoClock;
import uk.co.real_logic.fix_gateway.DebugLogger;
import uk.co.real_logic.fix_gateway.ReliefValve;
import uk.co.real_logic.fix_gateway.messages.*;

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
    private static final byte[] NO_BYTES = {};

    public static final int FRAME_SIZE = FixMessageEncoder.BLOCK_LENGTH + FixMessageDecoder.bodyHeaderLength();
    public static final int CONNECT_SIZE = ManageConnectionEncoder.BLOCK_LENGTH + ManageConnectionDecoder.addressHeaderLength();
    public static final int HEARTBEAT_LENGTH = HEADER_LENGTH + ApplicationHeartbeatEncoder.BLOCK_LENGTH;
    public static final int LIBRARY_CONNECT_LENGTH = HEADER_LENGTH + LibraryConnectEncoder.BLOCK_LENGTH;
    public static final int DISCONNECT_LENGTH = HEADER_LENGTH + DisconnectEncoder.BLOCK_LENGTH;
    public static final int RELEASE_SESSION_LENGTH = HEADER_LENGTH + ReleaseSessionEncoder.BLOCK_LENGTH;
    public static final int RELEASE_SESSION_REPLY_LENGTH = HEADER_LENGTH + ReleaseSessionReplyDecoder.BLOCK_LENGTH;
    public static final int REQUEST_SESSION_LENGTH = HEADER_LENGTH + RequestSessionEncoder.BLOCK_LENGTH;
    public static final int REQUEST_SESSION_REPLY_LENGTH = HEADER_LENGTH + RequestSessionReplyEncoder.BLOCK_LENGTH;
    public static final int CONNECT_FIXED_LENGTH = HEADER_LENGTH + ConnectEncoder.BLOCK_LENGTH +
        ConnectEncoder.addressHeaderLength();

    private final LogonEncoder logon = new LogonEncoder();
    private final ManageConnectionEncoder manageConnection = new ManageConnectionEncoder();
    private final InitiateConnectionEncoder initiateConnection = new InitiateConnectionEncoder();
    private final RequestDisconnectEncoder requestDisconnect = new RequestDisconnectEncoder();
    private final DisconnectEncoder disconnect = new DisconnectEncoder();
    private final FixMessageEncoder fixMessage = new FixMessageEncoder();
    private final ErrorEncoder error = new ErrorEncoder();
    private final ApplicationHeartbeatEncoder applicationHeartbeat = new ApplicationHeartbeatEncoder();
    private final LibraryConnectEncoder libraryConnect = new LibraryConnectEncoder();
    private final RequestSessionEncoder requestSession = new RequestSessionEncoder();
    private final RequestSessionReplyEncoder requestSessionReply = new RequestSessionReplyEncoder();
    private final ReleaseSessionEncoder releaseSession = new ReleaseSessionEncoder();
    private final ReleaseSessionReplyEncoder releaseSessionReply = new ReleaseSessionReplyEncoder();
    private final ConnectEncoder connect = new ConnectEncoder();

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
            .blockLength(fixMessage.sbeBlockLength())
            .templateId(fixMessage.sbeTemplateId())
            .schemaId(fixMessage.sbeSchemaId())
            .version(fixMessage.sbeSchemaVersion());

        offset += header.encodedLength();

        fixMessage
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
        return saveLogon(
            libraryId, connectionId, sessionId,
            UNKNOWN_SESSION, UNKNOWN_SESSION,
            null, null, null, null, null, null);
    }

    public long saveLogon(final int libraryId,
                          final long connectionId,
                          final long sessionId,
                          final int lastSentSequenceNumber,
                          final int lastReceivedSequenceNumber,
                          final String senderCompId,
                          final String senderSubId,
                          final String senderLocationId,
                          final String targetCompId,
                          final String username,
                          final String password)
    {
        final byte[] senderCompIdBytes = bytes(senderCompId);
        final byte[] senderSubIdBytes = bytes(senderSubId);
        final byte[] senderLocationIdBytes = bytes(senderLocationId);
        final byte[] targetCompIdBytes = bytes(targetCompId);
        final byte[] usernameBytes = bytes(username);
        final byte[] passwordBytes = bytes(password);

        final long position = claim(
            header.encodedLength() +
            LogonEncoder.BLOCK_LENGTH +
            LogonEncoder.senderSubIdHeaderLength() * 6 +
            senderCompIdBytes.length +
            senderSubIdBytes.length +
            senderLocationIdBytes.length +
            targetCompIdBytes.length +
            usernameBytes.length +
            passwordBytes.length);

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
            .lastReceivedSequenceNumber(lastReceivedSequenceNumber)
            .putSenderCompId(senderCompIdBytes, 0, senderCompIdBytes.length)
            .putSenderSubId(senderSubIdBytes, 0, senderSubIdBytes.length)
            .putSenderLocationId(senderLocationIdBytes, 0, senderLocationIdBytes.length)
            .putTargetCompId(targetCompIdBytes, 0, targetCompIdBytes.length)
            .putUsername(usernameBytes, 0, usernameBytes.length)
            .putPassword(passwordBytes, 0, passwordBytes.length);

        bufferClaim.commit();

        return position;
    }

    public long saveManageConnection(final long connectionId,
                                     final String address,
                                     final int libraryId,
                                     final ConnectionType type,
                                     final int lastSentSequenceNumber,
                                     final int lastReceivedSequenceNumber,
                                     final SessionState sessionState)
    {
        final byte[] addressBytes = bytes(address);

        final int length = header.encodedLength() + CONNECT_SIZE + addressBytes.length;
        final long position = claim(length);

        final MutableDirectBuffer buffer = bufferClaim.buffer();
        int offset = bufferClaim.offset();

        header
            .wrap(buffer, offset)
            .blockLength(manageConnection.sbeBlockLength())
            .templateId(manageConnection.sbeTemplateId())
            .schemaId(manageConnection.sbeSchemaId())
            .version(manageConnection.sbeSchemaVersion());

        offset += header.encodedLength();

        manageConnection
            .wrap(buffer, offset)
            .connection(connectionId)
            .libraryId(libraryId)
            .type(type)
            .lastSentSequenceNumber(lastSentSequenceNumber)
            .lastReceivedSequenceNumber(lastReceivedSequenceNumber)
            .sessionState(sessionState)
            .putAddress(addressBytes, 0, addressBytes.length);

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

    public long saveConnect(final long connectionId, final String address)
    {
        final byte[] addressBytes = bytes(address);

        final long position = claim(CONNECT_FIXED_LENGTH + addressBytes.length);

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
            .putAddress(addressBytes, 0, addressBytes.length);

        bufferClaim.commit();

        return position;
    }

    public long saveRequestDisconnect(final int libraryId, final long connectionId)
    {
        final long position = claim(header.encodedLength() + RequestDisconnectEncoder.BLOCK_LENGTH);

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
        final String targetCompId,
        final SequenceNumberType sequenceNumberType,
        final int requestedInitialSequenceNumber,
        final String username,
        final String password)
    {
        final byte[] hostBytes = bytes(host);
        final byte[] senderCompIdBytes = bytes(senderCompId);
        final byte[] senderSubIdBytes = bytes(senderSubId);
        final byte[] senderLocationIdBytes = bytes(senderLocationId);
        final byte[] targetCompIdBytes = bytes(targetCompId);
        final byte[] usernameBytes = bytes(username);
        final byte[] passwordBytes = bytes(password);

        final long position = claim(
            header.encodedLength() +
            InitiateConnectionEncoder.BLOCK_LENGTH +
            InitiateConnectionDecoder.hostHeaderLength() * 7 +
            hostBytes.length +
            senderCompIdBytes.length +
            senderSubIdBytes.length +
            senderLocationIdBytes.length +
            targetCompIdBytes.length +
            usernameBytes.length +
            passwordBytes.length);

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
            .requestedInitialSequenceNumber(requestedInitialSequenceNumber)
            .sequenceNumberType(sequenceNumberType)
            .putHost(hostBytes, 0, hostBytes.length)
            .putSenderCompId(senderCompIdBytes, 0, senderCompIdBytes.length)
            .putSenderSubId(senderSubIdBytes, 0, senderSubIdBytes.length)
            .putSenderLocationId(senderLocationIdBytes, 0, senderLocationIdBytes.length)
            .putTargetCompId(targetCompIdBytes, 0, targetCompIdBytes.length)
            .putUsername(usernameBytes, 0, usernameBytes.length)
            .putPassword(passwordBytes, 0, passwordBytes.length);

        bufferClaim.commit();

        return position;
    }

    public long saveError(final GatewayError errorType, final int libraryId, final String message)
    {
        final byte[] messageBytes = bytes(message);
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

    public long saveReleaseSession(
        final int libraryId,
        final long connectionId,
        final long correlationId,
        final SessionState state,
        final long heartbeatIntervalInMs,
        final int lastSentSequenceNumber,
        final int lastReceivedSequenceNumber)
    {
        final long position = claim(RELEASE_SESSION_LENGTH);

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
            .correlationId(correlationId)
            .heartbeatIntervalInMs(heartbeatIntervalInMs)
            .state(state)
            .lastSentSequenceNumber(lastSentSequenceNumber)
            .lastReceivedSequenceNumber(lastReceivedSequenceNumber);

        bufferClaim.commit();

        return position;
    }

    public long saveReleaseSessionReply(final SessionReplyStatus status, final long correlationId)
    {
        final long position = claim(RELEASE_SESSION_REPLY_LENGTH);

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

    public long saveRequestSessionReply(final SessionReplyStatus status, final long correlationId)
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

    private byte[] bytes(final String host)
    {
        if (host == null)
        {
            return NO_BYTES;
        }
        return host.getBytes(UTF_8);
    }
}
