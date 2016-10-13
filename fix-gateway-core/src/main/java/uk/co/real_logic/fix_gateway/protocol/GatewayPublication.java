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

import org.agrona.DirectBuffer;
import org.agrona.MutableDirectBuffer;
import org.agrona.concurrent.IdleStrategy;
import org.agrona.concurrent.NanoClock;
import org.agrona.concurrent.status.AtomicCounter;
import uk.co.real_logic.fix_gateway.DebugLogger;
import uk.co.real_logic.fix_gateway.engine.SessionInfo;
import uk.co.real_logic.fix_gateway.messages.*;
import uk.co.real_logic.fix_gateway.messages.ControlNotificationEncoder.SessionsEncoder;
import uk.co.real_logic.fix_gateway.replication.ClusterablePublication;

import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static uk.co.real_logic.fix_gateway.DebugLogger.logSbeMessage;
import static uk.co.real_logic.fix_gateway.LogTag.FIX_MESSAGE;
import static uk.co.real_logic.fix_gateway.LogTag.GATEWAY_MESSAGE;
import static uk.co.real_logic.fix_gateway.messages.NotLeaderEncoder.libraryChannelHeaderLength;

/**
 * A proxy for publishing messages fix related messages
 */
public class GatewayPublication extends ClaimablePublication
{
    private static final byte[] NO_BYTES = {};

    public static final int FRAME_SIZE = FixMessageEncoder.BLOCK_LENGTH + FixMessageDecoder.bodyHeaderLength();

    private static final int CONNECT_SIZE = ManageConnectionEncoder.BLOCK_LENGTH + ManageConnectionDecoder.addressHeaderLength();
    private static final int HEARTBEAT_LENGTH = HEADER_LENGTH + ApplicationHeartbeatEncoder.BLOCK_LENGTH;
    private static final int LIBRARY_CONNECT_LENGTH = HEADER_LENGTH + LibraryConnectEncoder.BLOCK_LENGTH;
    private static final int DISCONNECT_LENGTH = HEADER_LENGTH + DisconnectEncoder.BLOCK_LENGTH;
    private static final int RELEASE_SESSION_LENGTH = HEADER_LENGTH + ReleaseSessionEncoder.BLOCK_LENGTH +
        ReleaseSessionEncoder.usernameHeaderLength() + ReleaseSessionEncoder.passwordHeaderLength();
    private static final int RELEASE_SESSION_REPLY_LENGTH = HEADER_LENGTH + ReleaseSessionReplyDecoder.BLOCK_LENGTH;
    private static final int REQUEST_SESSION_LENGTH = HEADER_LENGTH + RequestSessionEncoder.BLOCK_LENGTH;
    private static final int REQUEST_SESSION_REPLY_LENGTH = HEADER_LENGTH + RequestSessionReplyEncoder.BLOCK_LENGTH;
    private static final int CONNECT_FIXED_LENGTH = HEADER_LENGTH + ConnectEncoder.BLOCK_LENGTH +
        ConnectEncoder.addressHeaderLength();
    private static final int NOT_LEADER_BLOCK_LENGTH =
        NotLeaderEncoder.BLOCK_LENGTH + HEADER_LENGTH + libraryChannelHeaderLength();

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
    private final CatchupEncoder catchup = new CatchupEncoder();
    private final NewSentPositionEncoder newSentPosition = new NewSentPositionEncoder();
    private final ResetSessionIdsEncoder resetSessionIds = new ResetSessionIdsEncoder();
    private final NotLeaderEncoder notLeader = new NotLeaderEncoder();
    private final ControlNotificationEncoder controlNotification = new ControlNotificationEncoder();
    private final LibraryTimeoutEncoder libraryTimeout = new LibraryTimeoutEncoder();
    private final ResetSequenceNumberEncoder resetSequenceNumber = new ResetSequenceNumberEncoder();

    private final NanoClock nanoClock;

    public GatewayPublication(
        final ClusterablePublication dataPublication,
        final AtomicCounter fails,
        final IdleStrategy idleStrategy,
        final NanoClock nanoClock,
        final int maxClaimAttempts)
    {
        super(maxClaimAttempts, idleStrategy, fails, dataPublication);
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
        final long timestamp = nanoClock.nanoTime();
        final int framedLength = header.encodedLength() + FRAME_SIZE + srcLength;
        final long position = claim(framedLength);
        if (position < 0)
        {
            return position;
        }

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

        DebugLogger.log(FIX_MESSAGE, "Enqueued %s\n", srcBuffer, srcOffset, srcLength);

        return position;
    }

    public long saveLogon(final int libraryId,
                          final long connectionId,
                          final long sessionId)
    {
        return saveLogon(
            libraryId, connectionId, sessionId,
            SessionInfo.UNK_SESSION, SessionInfo.UNK_SESSION,
            null, null, null, null, null, null, LogonStatus.NEW);
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
                          final String password,
                          final LogonStatus status)
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

        if (position < 0)
        {
            return position;
        }

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
            .status(status)
            .putSenderCompId(senderCompIdBytes, 0, senderCompIdBytes.length)
            .putSenderSubId(senderSubIdBytes, 0, senderSubIdBytes.length)
            .putSenderLocationId(senderLocationIdBytes, 0, senderLocationIdBytes.length)
            .putTargetCompId(targetCompIdBytes, 0, targetCompIdBytes.length)
            .putUsername(usernameBytes, 0, usernameBytes.length)
            .putPassword(passwordBytes, 0, passwordBytes.length);

        bufferClaim.commit();

        logSbeMessage(GATEWAY_MESSAGE, buffer, bufferClaim.offset());

        return position;
    }

    public long saveManageConnection(
        final long connectionId,
        final long sessionId,
        final String address,
        final int libraryId,
        final ConnectionType type,
        final int lastSentSequenceNumber,
        final int lastReceivedSequenceNumber,
        final SessionState sessionState,
        final int heartbeatIntervalInS,
        final long replyToId)
    {
        final byte[] addressBytes = bytes(address);

        final int length = header.encodedLength() + CONNECT_SIZE + addressBytes.length;
        final long position = claim(length);
        if (position < 0)
        {
            return position;
        }

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
            .session(sessionId)
            .libraryId(libraryId)
            .type(type)
            .lastSentSequenceNumber(lastSentSequenceNumber)
            .lastReceivedSequenceNumber(lastReceivedSequenceNumber)
            .sessionState(sessionState)
            .heartbeatIntervalInS(heartbeatIntervalInS)
            .replyToId(replyToId)
            .putAddress(addressBytes, 0, addressBytes.length);

        bufferClaim.commit();

        logSbeMessage(GATEWAY_MESSAGE, buffer, bufferClaim.offset());

        return position;
    }

    public long saveDisconnect(final int libraryId, final long connectionId, final DisconnectReason reason)
    {
        final long position = claim(DISCONNECT_LENGTH);
        if (position < 0)
        {
            return position;
        }

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

        logSbeMessage(GATEWAY_MESSAGE, buffer, bufferClaim.offset());

        return position;
    }

    public long saveConnect(final long connectionId, final String address)
    {
        final byte[] addressBytes = bytes(address);

        final long position = claim(CONNECT_FIXED_LENGTH + addressBytes.length);
        if (position < 0)
        {
            return position;
        }

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

        logSbeMessage(GATEWAY_MESSAGE, buffer, bufferClaim.offset());

        return position;
    }

    public long saveResetSessionIds()
    {
        final long position = claim(HEADER_LENGTH);
        if (position < 0)
        {
            return position;
        }

        final MutableDirectBuffer buffer = bufferClaim.buffer();
        final int offset = bufferClaim.offset();

        header
            .wrap(buffer, offset)
            .blockLength(resetSessionIds.sbeBlockLength())
            .templateId(resetSessionIds.sbeTemplateId())
            .schemaId(resetSessionIds.sbeSchemaId())
            .version(resetSessionIds.sbeSchemaVersion());

        bufferClaim.commit();

        logSbeMessage(GATEWAY_MESSAGE, buffer, bufferClaim.offset());

        return position;
    }

    public long saveResetSequenceNumber(final long sessionId)
    {
        final long position = claim(HEADER_LENGTH + ResetSequenceNumberEncoder.BLOCK_LENGTH);
        if (position < 0)
        {
            return position;
        }

        final MutableDirectBuffer buffer = bufferClaim.buffer();
        int offset = bufferClaim.offset();

        header
            .wrap(buffer, offset)
            .blockLength(resetSequenceNumber.sbeBlockLength())
            .templateId(resetSequenceNumber.sbeTemplateId())
            .schemaId(resetSequenceNumber.sbeSchemaId())
            .version(resetSequenceNumber.sbeSchemaVersion());

        offset += header.encodedLength();

        resetSequenceNumber
            .wrap(buffer, offset)
            .session(sessionId);

        bufferClaim.commit();

        logSbeMessage(GATEWAY_MESSAGE, buffer, bufferClaim.offset());

        return position;
    }

    public long saveRequestDisconnect(
        final int libraryId, final long connectionId, final DisconnectReason reason)
    {
        final long position = claim(header.encodedLength() + RequestDisconnectEncoder.BLOCK_LENGTH);
        if (position < 0)
        {
            return position;
        }

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
            .connection(connectionId)
            .reason(reason);

        bufferClaim.commit();

        logSbeMessage(GATEWAY_MESSAGE, buffer, bufferClaim.offset());

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
        final String password,
        final int heartbeatIntervalInS,
        final long correlationId)
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
        if (position < 0)
        {
            return position;
        }

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
            .heartbeatIntervalInS(heartbeatIntervalInS)
            .correlationId(correlationId)
            .putHost(hostBytes, 0, hostBytes.length)
            .putSenderCompId(senderCompIdBytes, 0, senderCompIdBytes.length)
            .putSenderSubId(senderSubIdBytes, 0, senderSubIdBytes.length)
            .putSenderLocationId(senderLocationIdBytes, 0, senderLocationIdBytes.length)
            .putTargetCompId(targetCompIdBytes, 0, targetCompIdBytes.length)
            .putUsername(usernameBytes, 0, usernameBytes.length)
            .putPassword(passwordBytes, 0, passwordBytes.length);

        bufferClaim.commit();

        logSbeMessage(GATEWAY_MESSAGE, buffer, bufferClaim.offset());

        return position;
    }

    public long saveError(
        final GatewayError errorType,
        final int libraryId,
        final long replyToId,
        final String message)
    {
        final byte[] messageBytes = bytes(message);
        final int length = header.encodedLength() + ErrorEncoder.BLOCK_LENGTH + ErrorDecoder.messageHeaderLength() +
            messageBytes.length;
        final long position = claim(length);
        if (position < 0)
        {
            return position;
        }

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
            .replyToId(replyToId)
            .putMessage(messageBytes, 0, messageBytes.length);

        bufferClaim.commit();

        logSbeMessage(GATEWAY_MESSAGE, buffer, bufferClaim.offset());

        return position;
    }

    public long saveApplicationHeartbeat(final int libraryId)
    {
        final long position = claim(HEARTBEAT_LENGTH);
        if (position < 0)
        {
            return position;
        }

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

        logSbeMessage(GATEWAY_MESSAGE, buffer, bufferClaim.offset());

        return position;
    }

    public long saveLibraryConnect(
        final int libraryId, final long correlationId)
    {
        final long position = claim(LIBRARY_CONNECT_LENGTH);
        if (position < 0)
        {
            return position;
        }

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
            .correlationId(correlationId);

        bufferClaim.commit();

        logSbeMessage(GATEWAY_MESSAGE, buffer, bufferClaim.offset());

        return position;
    }

    public long saveReleaseSession(
        final int libraryId,
        final long connectionId,
        final long correlationId,
        final SessionState state,
        final long heartbeatIntervalInMs,
        final int lastSentSequenceNumber,
        final int lastReceivedSequenceNumber,
        final String username,
        final String password)
    {
        final byte[] usernameBytes = bytes(username);
        final byte[] passwordBytes = bytes(password);

        final long position = claim(RELEASE_SESSION_LENGTH + usernameBytes.length + passwordBytes.length);
        if (position < 0)
        {
            return position;
        }

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
            .lastReceivedSequenceNumber(lastReceivedSequenceNumber)
            .putUsername(usernameBytes, 0, usernameBytes.length)
            .putPassword(passwordBytes, 0, passwordBytes.length);

        bufferClaim.commit();

        logSbeMessage(GATEWAY_MESSAGE, buffer, bufferClaim.offset());

        return position;
    }

    public long saveReleaseSessionReply(final int libraryId, final SessionReplyStatus status, final long replyToId)
    {
        final long position = claim(RELEASE_SESSION_REPLY_LENGTH);
        if (position < 0)
        {
            return position;
        }

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
            .replyToId(replyToId)
            .status(status);

        bufferClaim.commit();

        logSbeMessage(GATEWAY_MESSAGE, buffer, bufferClaim.offset());

        return position;
    }

    public long saveRequestSession(
        final int libraryId,
        final long sessionId,
        final long correlationId,
        final int lastReceivedSequenceNumber)
    {
        final long position = claim(REQUEST_SESSION_LENGTH);
        if (position < 0)
        {
            return position;
        }

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
            .sessionId(sessionId)
            .correlationId(correlationId)
            .lastReceivedSequenceNumber(lastReceivedSequenceNumber);

        bufferClaim.commit();

        logSbeMessage(GATEWAY_MESSAGE, buffer, bufferClaim.offset());

        return position;
    }

    public long saveRequestSessionReply(final int libraryId, final SessionReplyStatus status, final long replyToId)
    {
        final long position = claim(REQUEST_SESSION_REPLY_LENGTH);
        if (position < 0)
        {
            return position;
        }

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
            .replyToId(replyToId)
            .status(status);

        bufferClaim.commit();

        logSbeMessage(GATEWAY_MESSAGE, buffer, bufferClaim.offset());

        return position;
    }

    public long saveCatchup(
        final int libraryId, final long connectionId, final int messageCount)
    {
        final long position = claim(CatchupEncoder.BLOCK_LENGTH + HEADER_LENGTH);
        if (position < 0)
        {
            return position;
        }

        final MutableDirectBuffer buffer = bufferClaim.buffer();
        int offset = bufferClaim.offset();

        header
            .wrap(buffer, offset)
            .blockLength(catchup.sbeBlockLength())
            .templateId(catchup.sbeTemplateId())
            .schemaId(catchup.sbeSchemaId())
            .version(catchup.sbeSchemaVersion());

        offset += header.encodedLength();

        catchup
            .wrap(buffer, offset)
            .libraryId(libraryId)
            .connection(connectionId)
            .messageCount(messageCount);

        bufferClaim.commit();

        logSbeMessage(GATEWAY_MESSAGE, buffer, bufferClaim.offset());

        return position;
    }

    public long saveNotLeader(
        final int libraryId, final long replyToId, final DirectBuffer channel)
    {
        final int channelLength = (channel == null ? 0 : channel.capacity());
        final long position = claim(NOT_LEADER_BLOCK_LENGTH + channelLength);
        if (position < 0)
        {
            return position;
        }

        final MutableDirectBuffer buffer = bufferClaim.buffer();
        int offset = bufferClaim.offset();

        header
            .wrap(buffer, offset)
            .blockLength(notLeader.sbeBlockLength())
            .templateId(notLeader.sbeTemplateId())
            .schemaId(notLeader.sbeSchemaId())
            .version(notLeader.sbeSchemaVersion());

        offset += header.encodedLength();

        notLeader
            .wrap(buffer, offset)
            .libraryId(libraryId)
            .replyToId(replyToId);

        if (channel != null)
        {
            notLeader.putLibraryChannel(channel, 0, channelLength);
        }

        bufferClaim.commit();

        logSbeMessage(GATEWAY_MESSAGE, buffer, bufferClaim.offset());

        return position;
    }

    public long saveNewSentPosition(
        final int libraryId, final long sentPosition)
    {
        final long position = claim(NewSentPositionEncoder.BLOCK_LENGTH + HEADER_LENGTH);
        if (position < 0)
        {
            return position;
        }

        final MutableDirectBuffer buffer = bufferClaim.buffer();
        int offset = bufferClaim.offset();

        header
            .wrap(buffer, offset)
            .blockLength(newSentPosition.sbeBlockLength())
            .templateId(newSentPosition.sbeTemplateId())
            .schemaId(newSentPosition.sbeSchemaId())
            .version(newSentPosition.sbeSchemaVersion());

        offset += header.encodedLength();

        newSentPosition
            .wrap(buffer, offset)
            .libraryId(libraryId)
            .position(sentPosition);

        bufferClaim.commit();

        logSbeMessage(GATEWAY_MESSAGE, buffer, bufferClaim.offset());

        return position;
    }

    public long saveLibraryTimeout(final int libraryId, final long connectCorrelationId)
    {
        final long position = claim(LibraryTimeoutEncoder.BLOCK_LENGTH + HEADER_LENGTH);
        if (position < 0)
        {
            return position;
        }

        final MutableDirectBuffer buffer = bufferClaim.buffer();
        int offset = bufferClaim.offset();

        header
            .wrap(buffer, offset)
            .blockLength(libraryTimeout.sbeBlockLength())
            .templateId(libraryTimeout.sbeTemplateId())
            .schemaId(libraryTimeout.sbeSchemaId())
            .version(libraryTimeout.sbeSchemaVersion());

        offset += header.encodedLength();

        libraryTimeout
            .wrap(buffer, offset)
            .libraryId(libraryId)
            .connectCorrelationId(connectCorrelationId);

        bufferClaim.commit();

        logSbeMessage(GATEWAY_MESSAGE, buffer, bufferClaim.offset());

        return position;
    }

    public long saveControlNotification(
        final int libraryId, final List<SessionInfo> sessions)
    {
        final int sessionsCount = sessions.size();
        final long position = claim(
            HEADER_LENGTH +
            ControlNotificationEncoder.BLOCK_LENGTH +
            GroupSizeEncodingEncoder.ENCODED_LENGTH +
            sessionsCount * SessionsEncoder.sbeBlockLength());

        if (position < 0)
        {
            return position;
        }

        final MutableDirectBuffer buffer = bufferClaim.buffer();
        int offset = bufferClaim.offset();

        header
            .wrap(buffer, offset)
            .blockLength(controlNotification.sbeBlockLength())
            .templateId(controlNotification.sbeTemplateId())
            .schemaId(controlNotification.sbeSchemaId())
            .version(controlNotification.sbeSchemaVersion());

        offset += header.encodedLength();

        controlNotification
            .wrap(buffer, offset)
            .libraryId(libraryId);

        final SessionsEncoder sessionsEncoder = controlNotification.sessionsCount(sessionsCount);
        for (int i = 0; i < sessionsCount; i++)
        {
            final long sessionId = sessions.get(i).sessionId();
            sessionsEncoder.next().sessionId(sessionId);
        }

        bufferClaim.commit();

        logSbeMessage(GATEWAY_MESSAGE, buffer, bufferClaim.offset());

        return position;
    }

    public int id()
    {
        return dataPublication.id();
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
